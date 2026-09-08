"""Relógio vetorial e buffer de entrega por número global."""

from __future__ import annotations


class VectorClock:
    def __init__(self, node_id: int, node_ids: list[int]) -> None:
        self.node_id = node_id
        self.ids = sorted(node_ids)
        self.index = {nid: i for i, nid in enumerate(self.ids)}
        self.values = [0] * len(self.ids)

    def increment_self(self) -> None:
        self.values[self.index[self.node_id]] += 1

    def merge(self, incoming: list[int]) -> None:
        for i, value in enumerate(incoming):
            if i < len(self.values):
                self.values[i] = max(self.values[i], value)

    def as_list(self) -> list[int]:
        return list(self.values)


class DeliveryBuffer:
    """Agrupa GROUP + SEQ e entrega quando global_seq == last_delivered + 1."""

    def __init__(self) -> None:
        self.last_delivered = 0
        self.groups: dict[str, dict] = {}
        self.seq_of: dict[str, int] = {}
        self.msg_of_seq: dict[int, str] = {}
        self.delivered_ids: set[str] = set()
        self.global_order: list[dict] = []
        self.origem_of: dict[str, int] = {}

    def add_group(self, msg: dict) -> bool:
        msg_id = msg["msg_id"]
        if msg_id in self.delivered_ids:
            return False
        if msg_id not in self.groups:
            self.groups[msg_id] = msg
            self.origem_of[msg_id] = int(msg["origem"])
            return True
        return False

    def add_seq(self, msg_id: str, seq: int, origem_grupo: int | None = None) -> bool:
        if msg_id in self.delivered_ids:
            return False
        if seq in self.msg_of_seq and self.msg_of_seq[seq] != msg_id:
            return False
        self.seq_of[msg_id] = seq
        self.msg_of_seq[seq] = msg_id
        if origem_grupo is not None:
            self.origem_of[msg_id] = origem_grupo
        return True

    def drain(self) -> list[dict]:
        delivered: list[dict] = []
        while True:
            nxt = self.last_delivered + 1
            msg_id = self.msg_of_seq.get(nxt)
            if not msg_id or msg_id not in self.groups:
                break
            msg = dict(self.groups[msg_id])
            msg["global_seq"] = nxt
            self.global_order.append(msg)
            self.delivered_ids.add(msg_id)
            self.last_delivered = nxt
            delivered.append(msg)
        return delivered

    def max_known_seq(self) -> int:
        known = [self.last_delivered, *self.msg_of_seq.keys()]
        return max(known)

    def hole(self) -> tuple[str, int, str | None] | None:
        nxt = self.last_delivered + 1
        if nxt in self.msg_of_seq:
            msg_id = self.msg_of_seq[nxt]
            if msg_id not in self.groups:
                return ("payload", nxt, msg_id)
            return None
        if any(seq > self.last_delivered for seq in self.msg_of_seq):
            return ("seq", nxt, None)
        return None

    def unsequenced(self) -> list[dict]:
        out = []
        for msg_id, msg in self.groups.items():
            if msg_id not in self.seq_of and msg_id not in self.delivered_ids:
                out.append(msg)
        return out

    def origem_of_msg(self, msg_id: str) -> int | None:
        return self.origem_of.get(msg_id)
