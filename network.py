"""Multicast UDP (grupo) e malha TCP unicast (controle + privado)."""

from __future__ import annotations

import json
import socket
import struct
import threading
from collections.abc import Callable

from config import ClusterConfig, NodeInfo

Handler = Callable[[dict], None]


def _encode(msg: dict) -> bytes:
    return (json.dumps(msg, ensure_ascii=False) + "\n").encode("utf-8")


class Network:
    def __init__(
        self,
        node_id: int,
        cfg: ClusterConfig,
        on_udp: Handler,
        on_tcp: Handler,
        log: Callable[[str], None],
    ) -> None:
        self.node_id = node_id
        self.cfg = cfg
        self.info = cfg.get(node_id)
        self.on_udp = on_udp
        self.on_tcp = on_tcp
        self.log = log
        self._running = False
        self._udp_recv: socket.socket | None = None
        self._udp_send: socket.socket | None = None
        self._tcp_server: socket.socket | None = None
        self._out: dict[int, socket.socket] = {}
        self._out_lock = threading.Lock()
        self._send_locks: dict[int, threading.Lock] = {
            n.id: threading.Lock() for n in cfg.nos if n.id != node_id
        }

    def start(self) -> None:
        self._running = True
        self._udp_send = self._make_udp_sender()
        self._udp_recv = self._make_udp_receiver()
        self._tcp_server = self._make_tcp_server()
        threading.Thread(target=self._udp_loop, name=f"udp-{self.node_id}", daemon=True).start()
        threading.Thread(target=self._tcp_accept_loop, name=f"tcp-{self.node_id}", daemon=True).start()

    def stop(self) -> None:
        self._running = False
        for sock in (self._udp_recv, self._udp_send, self._tcp_server):
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass
        with self._out_lock:
            for sock in self._out.values():
                try:
                    sock.close()
                except OSError:
                    pass
            self._out.clear()

    def multicast(self, msg: dict) -> None:
        data = json.dumps(msg, ensure_ascii=False).encode("utf-8")
        assert self._udp_send is not None
        self._udp_send.sendto(data, (self.cfg.grupo_multicast, self.cfg.porta_grupo))

    def send_tcp(self, dest_id: int, msg: dict) -> None:
        if dest_id == self.node_id:
            self.on_tcp(msg)
            return
        dest = self.cfg.get(dest_id)
        lock = self._send_locks[dest_id]
        payload = _encode(msg)
        with lock:
            try:
                sock = self._get_out(dest)
                sock.sendall(payload)
            except OSError:
                self._drop_out(dest_id)
                try:
                    sock = self._get_out(dest)
                    sock.sendall(payload)
                except OSError:
                    pass

    def send_tcp_all(self, msg: dict) -> None:
        for node in self.cfg.nos:
            if node.id != self.node_id:
                self.send_tcp(node.id, msg)

    def _make_udp_sender(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_TTL, 1)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_LOOP, 1)
        iface = socket.inet_aton(self.info.host)
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_MULTICAST_IF, iface)
        return sock

    def _make_udp_receiver(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM, socket.IPPROTO_UDP)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        if hasattr(socket, "SO_REUSEPORT"):
            sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEPORT, 1)
        sock.bind(("", self.cfg.porta_grupo))
        mreq = struct.pack("4s4s", socket.inet_aton(self.cfg.grupo_multicast), socket.inet_aton(self.info.host))
        sock.setsockopt(socket.IPPROTO_IP, socket.IP_ADD_MEMBERSHIP, mreq)
        sock.settimeout(0.5)
        return sock

    def _make_tcp_server(self) -> socket.socket:
        sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
        sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        sock.bind((self.info.host, self.info.porta))
        sock.listen(64)
        sock.settimeout(0.5)
        return sock

    def _get_out(self, dest: NodeInfo) -> socket.socket:
        with self._out_lock:
            existing = self._out.get(dest.id)
            if existing is not None:
                return existing
            sock = socket.socket(socket.AF_INET, socket.SOCK_STREAM)
            sock.settimeout(0.35)
            sock.connect((dest.host, dest.porta))
            sock.settimeout(None)
            self._out[dest.id] = sock
            return sock

    def _drop_out(self, dest_id: int) -> None:
        with self._out_lock:
            sock = self._out.pop(dest_id, None)
            if sock is not None:
                try:
                    sock.close()
                except OSError:
                    pass

    def _udp_loop(self) -> None:
        assert self._udp_recv is not None
        while self._running:
            try:
                data, _addr = self._udp_recv.recvfrom(65536)
            except TimeoutError:
                continue
            except OSError:
                if self._running:
                    self.log("[rede] socket UDP encerrado")
                break
            try:
                msg = json.loads(data.decode("utf-8"))
            except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                self.log(f"[rede] UDP inválido: {exc}")
                continue
            try:
                self.on_udp(msg)
            except Exception as exc:  # noqa: BLE001 — isola a thread de rede
                self.log(f"[rede] erro no handler UDP: {exc}")

    def _tcp_accept_loop(self) -> None:
        assert self._tcp_server is not None
        while self._running:
            try:
                conn, _addr = self._tcp_server.accept()
            except TimeoutError:
                continue
            except OSError:
                break
            threading.Thread(
                target=self._tcp_client_loop,
                args=(conn,),
                name=f"tcp-in-{self.node_id}",
                daemon=True,
            ).start()

    def _tcp_client_loop(self, conn: socket.socket) -> None:
        buf = b""
        try:
            while self._running:
                chunk = conn.recv(4096)
                if not chunk:
                    break
                buf += chunk
                while b"\n" in buf:
                    line, buf = buf.split(b"\n", 1)
                    if not line.strip():
                        continue
                    try:
                        msg = json.loads(line.decode("utf-8"))
                    except (UnicodeDecodeError, json.JSONDecodeError) as exc:
                        self.log(f"[rede] TCP inválido: {exc}")
                        continue
                    try:
                        self.on_tcp(msg)
                    except Exception as exc:  # noqa: BLE001
                        self.log(f"[rede] erro no handler TCP: {exc}")
        except OSError:
            pass
        finally:
            try:
                conn.close()
            except OSError:
                pass
