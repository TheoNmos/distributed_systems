"""Snapshot consistente de Chandy-Lamport sobre canais TCP FIFO."""

from __future__ import annotations

import uuid
from collections.abc import Callable


class ChandyLamport:
    def __init__(
        self,
        node_id: int,
        peer_ids: list[int],
        send_tcp: Callable[[int, dict], None],
        capture_state: Callable[[], dict],
        notify_fragment: Callable[[dict], None],
        log: Callable[[str], None],
    ) -> None:
        self.node_id = node_id
        self.peer_ids = list(peer_ids)
        self.send_tcp = send_tcp
        self.capture_state = capture_state
        self.notify_fragment = notify_fragment
        self.log = log
        self.snapshot_id: str | None = None
        self.initiator: int | None = None
        self.recorded = False
        self.channel_closed: dict[int, bool] = {}
        self.channel_state: dict[int, list[dict]] = {}
        self.local_state: dict | None = None
        self.fragments: dict[int, dict] = {}
        self.done_ids: set[str] = set()
        self.collecting_id: str | None = None

    def start(self) -> str:
        sid = uuid.uuid4().hex[:8]
        self.log(f"[snapshot] inicia captura {sid}")
        self.fragments = {}
        self.collecting_id = sid
        self._record_and_propagate(sid, self.node_id, from_peer=None)
        return sid

    def on_marker(self, origem: int, snapshot_id: str, initiator: int) -> None:
        if snapshot_id in self.done_ids:
            return
        if not self.recorded or self.snapshot_id != snapshot_id:
            self._record_and_propagate(snapshot_id, initiator, from_peer=origem)
            return
        self.channel_closed[origem] = True
        self._maybe_finish()

    def on_tcp_app_message(self, origem: int, msg: dict) -> None:
        if not self.recorded:
            return
        if origem in self.channel_closed and not self.channel_closed[origem]:
            slim = {
                "tipo": msg.get("tipo"),
                "origem": msg.get("origem"),
                "destino": msg.get("destino"),
                "msg_id": msg.get("msg_id"),
                "global_seq": msg.get("global_seq"),
                "payload": msg.get("payload"),
            }
            self.channel_state[origem].append(slim)

    def on_fragment(self, msg: dict) -> None:
        origem = int(msg["origem"])
        sid = msg.get("snapshot_id")
        if sid != self.collecting_id:
            return
        self.fragments[origem] = msg
        self.notify_fragment(msg)

    def _record_and_propagate(self, snapshot_id: str, initiator: int, from_peer: int | None) -> None:
        self.snapshot_id = snapshot_id
        self.initiator = initiator
        self.recorded = True
        self.local_state = self.capture_state()
        self.channel_state = {p: [] for p in self.peer_ids}
        self.channel_closed = {p: False for p in self.peer_ids}
        if from_peer is not None:
            self.channel_closed[from_peer] = True
        marker = {
            "tipo": "MARKER",
            "origem": self.node_id,
            "snapshot_id": snapshot_id,
            "initiator": initiator,
        }
        for peer in self.peer_ids:
            m = dict(marker)
            m["destino"] = peer
            self.send_tcp(peer, m)
        self._maybe_finish()

    def _maybe_finish(self) -> None:
        if not self.recorded:
            return
        if self.peer_ids and not all(self.channel_closed.get(p, False) for p in self.peer_ids):
            return
        fragment = {
            "tipo": "SNAPSHOT_STATE",
            "origem": self.node_id,
            "destino": self.initiator,
            "snapshot_id": self.snapshot_id,
            "estado": self.local_state,
            "canais": {str(k): v for k, v in self.channel_state.items()},
        }
        self.log(f"[snapshot] fragmento local pronto ({self.snapshot_id})")
        if self.initiator == self.node_id:
            self.fragments[self.node_id] = fragment
            self.notify_fragment(fragment)
        else:
            assert self.initiator is not None
            self.send_tcp(self.initiator, fragment)
        self.done_ids.add(self.snapshot_id or "")
        self.recorded = False
        self.snapshot_id = None
        self.channel_closed = {}
        self.channel_state = {}
