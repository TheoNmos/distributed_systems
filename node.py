"""Processo de um nó do chat distribuído."""

from __future__ import annotations

import argparse
import threading
import time
import uuid
from pathlib import Path

from cli import format_status, run_cli
from config import ClusterConfig, load_config
from election import BullyElection
from network import Network
from ordering import DeliveryBuffer, VectorClock
from snapshot import ChandyLamport

HEARTBEAT_EVERY = 0.7
NACK_AFTER = 0.6
UNSEQ_AFTER = 0.5
CONTROL_TICK = 0.15


class Node:
    def __init__(self, node_id: int, cfg: ClusterConfig) -> None:
        if node_id not in cfg.ids():
            raise SystemExit(f"id {node_id} não está em {cfg.ids()}")
        self.node_id = node_id
        self.cfg = cfg
        self.ids = cfg.ids()
        self.lock = threading.RLock()
        self.print_lock = threading.Lock()
        self.running = False
        self.clock = VectorClock(node_id, self.ids)
        self.buffer = DeliveryBuffer()
        self.local_order: list[str] = []
        self.privadas: list[str] = []
        self.seen_udp: set[str] = set()
        self.next_global_seq = 1
        self.seq_log: dict[int, dict] = {}
        self.msg_id_to_seq: dict[str, int] = {}
        self.nack_at: dict[str, float] = {}
        self.unseq_at: dict[str, float] = {}
        self.expected_fragments = len(self.ids)
        self.net = Network(node_id, cfg, self._on_udp, self._on_tcp, self.log)
        peers = [i for i in self.ids if i != node_id]
        self.election = BullyElection(
            node_id=node_id,
            all_ids=self.ids,
            send_tcp=self._send_tcp,
            on_became_leader=self._on_became_leader,
            get_last_seq=self.max_known_seq,
            log=self.log,
        )
        self.snapshot = ChandyLamport(
            node_id=node_id,
            peer_ids=peers,
            send_tcp=self._send_tcp,
            capture_state=self._capture_state,
            notify_fragment=self._on_snapshot_fragment,
            log=self.log,
        )
        self._last_heartbeat_sent = 0.0
        self._snapshot_print_lock = threading.Lock()

    def log(self, line: str) -> None:
        with self.print_lock:
            print(line, flush=True)

    def max_known_seq(self) -> int:
        with self.lock:
            known = [self.buffer.max_known_seq(), *self.seq_log.keys()]
            return max(known) if known else 0

    def start(self) -> None:
        self.running = True
        self.net.start()
        self.log(
            f"[nó {self.node_id}] escutando TCP {self.cfg.get(self.node_id).host}:"
            f"{self.cfg.get(self.node_id).porta} | multicast "
            f"{self.cfg.grupo_multicast}:{self.cfg.porta_grupo}"
        )
        threading.Thread(target=self._control_loop, name=f"ctrl-{self.node_id}", daemon=True).start()
        time.sleep(0.4)

    def shutdown(self) -> None:
        self.running = False
        self.net.stop()

    def send_private(self, dest: int, texto: str) -> None:
        if dest not in self.ids:
            self.log(f"destino {dest} desconhecido")
            return
        if dest == self.node_id:
            self.log("não envie privado para si mesmo")
            return
        if self.election.crashed:
            self.log("nó crashed — não envia")
            return
        msg = {
            "tipo": "PRIVATE",
            "origem": self.node_id,
            "destino": dest,
            "msg_id": uuid.uuid4().hex[:8],
            "payload": texto,
        }
        with self.lock:
            self.local_order.append(f"PRIVADO -> {dest}: {texto}")
        self._send_tcp(dest, msg)
        self.log(f"[LOCAL] privado para {dest}: {texto}")

    def send_group(self, texto: str) -> None:
        if self.election.crashed:
            self.log("nó crashed — não envia")
            return
        msg_id = uuid.uuid4().hex[:8]
        with self.lock:
            self.clock.increment_self()
            msg = {
                "tipo": "GROUP",
                "origem": self.node_id,
                "msg_id": msg_id,
                "vetor": self.clock.as_list(),
                "payload": texto,
            }
            self.local_order.append(f"GROUP {msg_id}: {texto}")
            self.buffer.add_group(msg)
            self.unseq_at[msg_id] = time.time()
            seq_msg = None
            if self.election.is_leader():
                seq_msg = self._assign_seq_locked(msg)
        self.net.multicast(msg)
        self.log(f"[LOCAL] grupo {msg_id}: {texto}")
        if seq_msg is not None:
            self._publish_seq(seq_msg)
            self._try_deliver()

    def start_snapshot(self) -> None:
        if self.election.crashed:
            self.log("nó crashed — sem snapshot")
            return
        with self.lock:
            self.snapshot.start()

    def crash(self) -> None:
        with self.lock:
            self.election.crash()

    def status_text(self) -> str:
        with self.lock:
            next_seq = self.next_global_seq if self.election.is_leader() else "-"
            global_lines = [
                f"#{m['global_seq']} <nó {m['origem']}> {m.get('payload', '')}  V={m.get('vetor')}"
                for m in self.buffer.global_order
            ]
            return format_status(
                node_id=self.node_id,
                leader_id=self.election.leader_id,
                crashed=self.election.crashed,
                last_seq=self.buffer.last_delivered,
                next_seq=next_seq,
                vector=self.clock.as_list(),
                vector_ids=self.clock.ids,
                buffer_groups=len(self.buffer.groups) - len(self.buffer.delivered_ids),
                buffer_seqs=len(self.buffer.msg_of_seq),
                unsequenced=len(self.buffer.unsequenced()),
                local_order=list(self.local_order),
                global_order=global_lines,
                privadas=list(self.privadas),
            )

    def _send_tcp(self, dest: int, msg: dict) -> None:
        self.net.send_tcp(dest, msg)

    def _on_became_leader(self) -> None:
        with self.lock:
            self.next_global_seq = max(self.next_global_seq, self.max_known_seq() + 1)
        self.log(f"[seq] próximo global_seq = {self.next_global_seq}")

    def _assign_seq_locked(self, group_msg: dict) -> dict | None:
        msg_id = group_msg["msg_id"]
        if msg_id in self.msg_id_to_seq:
            return None
        seq = self.next_global_seq
        self.next_global_seq += 1
        self.msg_id_to_seq[msg_id] = seq
        self.seq_log[seq] = group_msg
        self.buffer.add_seq(msg_id, seq, origem_grupo=int(group_msg["origem"]))
        self.unseq_at.pop(msg_id, None)
        self.log(f"[seq] atribui global_seq={seq} a {msg_id} (origem {group_msg['origem']})")
        return {
            "tipo": "SEQ",
            "origem": self.node_id,
            "msg_id": msg_id,
            "global_seq": seq,
            "origem_grupo": group_msg["origem"],
        }

    def _on_udp(self, msg: dict) -> None:
        if not self.running or self.election.crashed:
            return
        tipo = msg.get("tipo")
        key = f"{tipo}:{msg.get('msg_id')}:{msg.get('global_seq')}:{msg.get('origem')}"
        with self.lock:
            if key in self.seen_udp:
                return
            self.seen_udp.add(key)
            if len(self.seen_udp) > 5000:
                self.seen_udp = set(list(self.seen_udp)[-2000:])
        if tipo == "GROUP":
            self._handle_group(msg, via="udp")
        elif tipo == "SEQ":
            self._handle_seq(msg)

    def _on_tcp(self, msg: dict) -> None:
        if not self.running:
            return
        tipo = msg.get("tipo")
        origem = int(msg.get("origem", 0))
        if tipo == "MARKER":
            if self.election.crashed:
                return
            with self.lock:
                self.snapshot.on_marker(origem, msg["snapshot_id"], int(msg["initiator"]))
            return
        if tipo == "SNAPSHOT_STATE":
            with self.lock:
                self.snapshot.on_fragment(msg)
            return
        if self.election.crashed:
            return
        with self.lock:
            if tipo not in ("ELECTION", "OK", "COORDINATOR", "HEARTBEAT", "LAST_SEQ"):
                self.snapshot.on_tcp_app_message(origem, msg)
        if tipo in ("ELECTION", "OK", "COORDINATOR", "HEARTBEAT"):
            with self.lock:
                prev_leader = self.election.leader_id
                self.election.on_message(msg)
                new_leader = self.election.leader_id
            if tipo == "COORDINATOR" and new_leader != prev_leader:
                self._after_coordinator(msg)
            return
        if tipo == "PRIVATE":
            self._handle_private(msg)
            return
        if tipo == "GROUP":
            self._handle_group(msg, via="tcp")
            return
        if tipo == "SEQ":
            self._handle_seq(msg)
            return
        if tipo == "NACK":
            self._handle_nack(msg)
            return
        if tipo == "RETRANS":
            inner = msg.get("mensagem")
            if isinstance(inner, dict):
                if inner.get("tipo") == "GROUP":
                    self._handle_group(inner, via="tcp")
                elif inner.get("tipo") == "SEQ":
                    self._handle_seq(inner)
            return
        if tipo == "LAST_SEQ":
            self._handle_last_seq(msg)

    def _after_coordinator(self, msg: dict) -> None:
        leader = int(msg["origem"])
        with self.lock:
            last = self.max_known_seq()
            pending = list(self.buffer.unsequenced())
        self._send_tcp(
            leader,
            {
                "tipo": "LAST_SEQ",
                "origem": self.node_id,
                "destino": leader,
                "last_seq": last,
            },
        )
        for group in pending:
            self._send_tcp(leader, group)

    def _handle_last_seq(self, msg: dict) -> None:
        with self.lock:
            if not self.election.is_leader():
                return
            reported = int(msg.get("last_seq", 0))
            self.next_global_seq = max(self.next_global_seq, reported + 1)

    def _publish_seq(self, seq_msg: dict) -> None:
        self.net.multicast(seq_msg)
        self.net.send_tcp_all(seq_msg)

    def _handle_private(self, msg: dict) -> None:
        texto = msg.get("payload", "")
        origem = msg["origem"]
        with self.lock:
            self.privadas.append(f"de {origem}: {texto}")
        self.log(f"[PRIVADO] de {origem}: {texto}")

    def _handle_group(self, msg: dict, via: str) -> None:
        seq_msg = None
        with self.lock:
            self.buffer.add_group(msg)
            msg_id = msg["msg_id"]
            if msg_id not in self.buffer.seq_of and msg_id not in self.unseq_at:
                self.unseq_at[msg_id] = time.time()
            if self.election.is_leader():
                seq_msg = self._assign_seq_locked(msg)
        if seq_msg is not None:
            self._publish_seq(seq_msg)
        self._try_deliver()

    def _handle_seq(self, msg: dict) -> None:
        with self.lock:
            self.buffer.add_seq(
                msg["msg_id"],
                int(msg["global_seq"]),
                origem_grupo=msg.get("origem_grupo"),
            )
            self.unseq_at.pop(msg["msg_id"], None)
            if self.election.is_leader():
                self.msg_id_to_seq.setdefault(msg["msg_id"], int(msg["global_seq"]))
        self._try_deliver()

    def _try_deliver(self) -> None:
        with self.lock:
            delivered = self.buffer.drain()
            for item in delivered:
                incoming = item.get("vetor") or []
                if incoming:
                    self.clock.merge(incoming)
                self.clock.increment_self()
        for item in delivered:
            self.log(
                f"[GLOBAL #{item['global_seq']}] nó {item['origem']}: {item.get('payload')}  "
                f"V={item.get('vetor')}"
            )

    def _handle_nack(self, msg: dict) -> None:
        dest = int(msg["origem"])
        with self.lock:
            if not self.election.is_leader() and "msg_id" not in msg:
                return
            if "global_seq" in msg and int(msg["global_seq"]) in self.seq_log:
                seq = int(msg["global_seq"])
                group = self.seq_log[seq]
                seq_msg = {
                    "tipo": "SEQ",
                    "origem": self.node_id,
                    "msg_id": group["msg_id"],
                    "global_seq": seq,
                    "origem_grupo": group["origem"],
                }
                retrans_group = {"tipo": "RETRANS", "origem": self.node_id, "destino": dest, "mensagem": group}
                retrans_seq = {"tipo": "RETRANS", "origem": self.node_id, "destino": dest, "mensagem": seq_msg}
            elif "msg_id" in msg:
                msg_id = msg["msg_id"]
                group = self.buffer.groups.get(msg_id) or self.seq_log.get(self.msg_id_to_seq.get(msg_id, -1))
                if not group:
                    return
                seq = self.buffer.seq_of.get(msg_id) or self.msg_id_to_seq.get(msg_id)
                retrans_group = {"tipo": "RETRANS", "origem": self.node_id, "destino": dest, "mensagem": group}
                retrans_seq = None
                if seq is not None:
                    retrans_seq = {
                        "tipo": "RETRANS",
                        "origem": self.node_id,
                        "destino": dest,
                        "mensagem": {
                            "tipo": "SEQ",
                            "origem": self.node_id,
                            "msg_id": msg_id,
                            "global_seq": seq,
                            "origem_grupo": group["origem"],
                        },
                    }
            else:
                return
        self._send_tcp(dest, retrans_group)
        if retrans_seq:
            self._send_tcp(dest, retrans_seq)
        self.log(f"[nack] retransmitiu para {dest}")

    def _capture_state(self) -> dict:
        return {
            "node_id": self.node_id,
            "lider": self.election.leader_id,
            "crashed": self.election.crashed,
            "vetor": self.clock.as_list(),
            "vetor_ids": self.clock.ids,
            "last_seq": self.buffer.last_delivered,
            "next_seq": self.next_global_seq if self.election.is_leader() else None,
            "ordem_global": [
                {"global_seq": m["global_seq"], "origem": m["origem"], "payload": m.get("payload")}
                for m in self.buffer.global_order
            ],
            "ordem_local": list(self.local_order),
            "buffer_group_ids": [m for m in self.buffer.groups if m not in self.buffer.delivered_ids],
            "buffer_seqs": dict(self.buffer.msg_of_seq),
        }

    def _on_snapshot_fragment(self, _msg: dict) -> None:
        with self.lock:
            fragments = dict(self.snapshot.fragments)
            sid = _msg.get("snapshot_id")
        if len(fragments) < self.expected_fragments:
            self.log(f"[snapshot] fragmento {len(fragments)}/{self.expected_fragments}")
            return
        lines = [f"===== ESTADO GLOBAL {sid} ====="]
        for nid in sorted(fragments):
            frag = fragments[nid]
            estado = frag.get("estado", {})
            canais = frag.get("canais", {})
            lines.append(f"-- nó {nid} --")
            lines.append(
                f"  líder={estado.get('lider')} vetor={estado.get('vetor')} last_seq={estado.get('last_seq')}"
            )
            lines.append(f"  ordem global: {estado.get('ordem_global')}")
            for src, queued in canais.items():
                lines.append(f"  canal {src}->{nid}: {queued if queued else '(vazio)'}")
        lines.append("===== fim snapshot =====")
        self.log("\n".join(lines))

    def _control_loop(self) -> None:
        while self.running:
            now = time.time()
            seq_to_send: list[dict] = []
            nacks: list[dict] = []
            groups_to_leader: list[dict] = []
            heartbeat = None
            with self.lock:
                self.election.tick(now)
                if self.election.is_leader() and now - self._last_heartbeat_sent >= HEARTBEAT_EVERY:
                    heartbeat = {
                        "tipo": "HEARTBEAT",
                        "origem": self.node_id,
                        "last_seq": self.buffer.last_delivered,
                    }
                    self._last_heartbeat_sent = now
                hole = self.buffer.hole()
                leader = self.election.leader_id
                if hole and leader and not self.election.crashed:
                    kind, seq, msg_id = hole
                    key = f"{kind}:{seq}:{msg_id}"
                    if key not in self.nack_at or now - self.nack_at[key] >= NACK_AFTER:
                        self.nack_at[key] = now
                        if kind == "seq":
                            nacks.append(
                                {
                                    "tipo": "NACK",
                                    "origem": self.node_id,
                                    "destino": leader,
                                    "global_seq": seq,
                                }
                            )
                        elif kind == "payload" and msg_id:
                            origem = self.buffer.origem_of_msg(msg_id) or leader
                            nacks.append(
                                {
                                    "tipo": "NACK",
                                    "origem": self.node_id,
                                    "destino": origem,
                                    "msg_id": msg_id,
                                    "global_seq": seq,
                                }
                            )
                if leader and not self.election.crashed:
                    for group in self.buffer.unsequenced():
                        mid = group["msg_id"]
                        first = self.unseq_at.get(mid, now)
                        if now - first >= UNSEQ_AFTER:
                            groups_to_leader.append(group)
                            self.unseq_at[mid] = now
            if heartbeat is not None:
                self.net.send_tcp_all(heartbeat)
            for nack in nacks:
                self._send_tcp(int(nack["destino"]), nack)
                self.log(f"[nack] pede {nack.get('global_seq') or nack.get('msg_id')} a {nack['destino']}")
            if leader:
                for group in groups_to_leader:
                    self._send_tcp(leader, group)
            for seq_msg in seq_to_send:
                self.net.multicast(seq_msg)
            self._try_deliver()
            time.sleep(CONTROL_TICK)


def parse_args(argv: list[str] | None = None) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Nó do chat distribuído")
    parser.add_argument("--id", type=int, required=True, help="identificador deste nó")
    parser.add_argument(
        "--config",
        default=str(Path(__file__).resolve().parent / "config" / "nos.json"),
        help="catálogo estático JSON",
    )
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> None:
    args = parse_args(argv)
    cfg = load_config(args.config)
    node = Node(args.id, cfg)
    node.start()
    try:
        run_cli(
            node_id=node.node_id,
            send_private=node.send_private,
            send_group=node.send_group,
            snapshot=node.start_snapshot,
            status=node.status_text,
            crash=node.crash,
            shutdown=node.shutdown,
            log=node.log,
        )
        if node.running:
            try:
                while node.running:
                    time.sleep(0.5)
            except KeyboardInterrupt:
                pass
    finally:
        node.shutdown()


if __name__ == "__main__":
    main()
