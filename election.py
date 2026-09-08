"""Algoritmo do Valentão (Bully) com detecção de falha por heartbeat."""

from __future__ import annotations

import time
from collections.abc import Callable

OK_TIMEOUT = 1.0
COORD_TIMEOUT = 2.0
LEADER_TIMEOUT = 2.5


class BullyElection:
    def __init__(
        self,
        node_id: int,
        all_ids: list[int],
        send_tcp: Callable[[int, dict], None],
        on_became_leader: Callable[[], None],
        get_last_seq: Callable[[], int],
        log: Callable[[str], None],
    ) -> None:
        self.node_id = node_id
        self.all_ids = sorted(all_ids)
        self.send_tcp = send_tcp
        self.on_became_leader = on_became_leader
        self.get_last_seq = get_last_seq
        self.log = log
        self.leader_id: int | None = None
        self.crashed = False
        self.in_election = False
        self.ok_received = False
        self.waiting_ok_until = 0.0
        self.waiting_coord_until = 0.0
        self.last_heartbeat = 0.0
        self._election_started_at = 0.0

    def is_leader(self) -> bool:
        return (not self.crashed) and self.leader_id == self.node_id

    def higher_ids(self) -> list[int]:
        return [nid for nid in self.all_ids if nid > self.node_id]

    def crash(self) -> None:
        self.crashed = True
        if self.leader_id == self.node_id:
            self.leader_id = None
        self.in_election = False
        self.log("[bully] nó em modo /crash — para heartbeats e deixa de disputar eleição")

    def start_election(self, reason: str = "") -> None:
        if self.crashed:
            return
        now = time.time()
        if self.in_election and now - self._election_started_at < 0.4:
            return
        extra = f" ({reason})" if reason else ""
        self.log(f"[bully] inicia eleição{extra}")
        self.in_election = True
        self.ok_received = False
        self._election_started_at = now
        self.leader_id = None
        higher = self.higher_ids()
        if not higher:
            self.declare_leader()
            return
        for dest in higher:
            self.send_tcp(dest, {"tipo": "ELECTION", "origem": self.node_id, "destino": dest})
        self.waiting_ok_until = now + OK_TIMEOUT
        self.waiting_coord_until = 0.0

    def declare_leader(self) -> None:
        if self.crashed:
            return
        self.leader_id = self.node_id
        self.in_election = False
        self.ok_received = False
        self.waiting_ok_until = 0.0
        self.waiting_coord_until = 0.0
        self.last_heartbeat = time.time()
        last_seq = self.get_last_seq()
        self.log(f"[bully] este nó é o COORDENADOR (id={self.node_id}, last_seq={last_seq})")
        for dest in self.all_ids:
            if dest != self.node_id:
                self.send_tcp(
                    dest,
                    {
                        "tipo": "COORDINATOR",
                        "origem": self.node_id,
                        "destino": dest,
                        "last_seq": last_seq,
                    },
                )
        self.on_became_leader()

    def on_message(self, msg: dict) -> None:
        if self.crashed:
            return
        tipo = msg["tipo"]
        origem = int(msg["origem"])
        if tipo == "ELECTION":
            if origem < self.node_id:
                self.send_tcp(origem, {"tipo": "OK", "origem": self.node_id, "destino": origem})
                if self.is_leader():
                    self.send_tcp(
                        origem,
                        {
                            "tipo": "COORDINATOR",
                            "origem": self.node_id,
                            "destino": origem,
                            "last_seq": self.get_last_seq(),
                        },
                    )
                    return
                self.start_election(reason=f"ELECTION de {origem}")
        elif tipo == "OK":
            if self.in_election:
                self.ok_received = True
                self.waiting_ok_until = 0.0
                self.waiting_coord_until = time.time() + COORD_TIMEOUT
                self.log(f"[bully] OK de {origem}; aguarda COORDENADOR")
        elif tipo == "COORDINATOR":
            if origem < self.node_id:
                self.log(f"[bully] COORDENADOR {origem} tem ID menor; reeleição")
                self.start_election(reason="coordenador inferior")
                return
            changed = self.leader_id != origem
            self.leader_id = origem
            self.in_election = False
            self.ok_received = False
            self.waiting_ok_until = 0.0
            self.waiting_coord_until = 0.0
            self.last_heartbeat = time.time()
            if changed:
                self.log(f"[bully] líder agora é {origem}")
        elif tipo == "HEARTBEAT":
            if origem == self.node_id:
                return
            current = self.leader_id or 0
            if self.leader_id is None or origem >= current:
                if self.leader_id != origem:
                    self.log(f"[bully] líder agora é {origem} (heartbeat)")
                self.leader_id = origem
                self.last_heartbeat = time.time()
                self.in_election = False
                self.ok_received = False

    def tick(self, now: float) -> None:
        if self.crashed:
            return
        if self.in_election and not self.ok_received and self.waiting_ok_until and now >= self.waiting_ok_until:
            self.log("[bully] timeout sem OK — declara-se líder")
            self.declare_leader()
            return
        if self.in_election and self.ok_received and self.waiting_coord_until and now >= self.waiting_coord_until:
            self.log("[bully] timeout aguardando COORDENADOR — nova eleição")
            self.start_election(reason="timeout coordenador")
            return
        if self.leader_id is not None and self.leader_id != self.node_id:
            if self.last_heartbeat and now - self.last_heartbeat >= LEADER_TIMEOUT:
                self.log(f"[bully] silêncio do líder {self.leader_id}")
                self.leader_id = None
                self.start_election(reason="falha do líder")
        elif self.leader_id is None and not self.in_election:
            self.start_election(reason="sem líder")
