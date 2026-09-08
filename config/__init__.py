"""Catálogo estático de nós (lido só na inicialização)."""

from __future__ import annotations

import json
from dataclasses import dataclass
from pathlib import Path


@dataclass(frozen=True)
class NodeInfo:
    id: int
    host: str
    porta: int


@dataclass
class ClusterConfig:
    grupo_multicast: str
    porta_grupo: int
    nos: list[NodeInfo]

    def ids(self) -> list[int]:
        return [n.id for n in self.nos]

    def by_id(self) -> dict[int, NodeInfo]:
        return {n.id: n for n in self.nos}

    def get(self, node_id: int) -> NodeInfo:
        info = self.by_id().get(node_id)
        if info is None:
            raise KeyError(f"nó {node_id} não está no catálogo")
        return info


def load_config(path: str | Path) -> ClusterConfig:
    data = json.loads(Path(path).read_text(encoding="utf-8"))
    nos = [NodeInfo(id=int(n["id"]), host=str(n["host"]), porta=int(n["porta"])) for n in data["nos"]]
    nos.sort(key=lambda n: n.id)
    return ClusterConfig(
        grupo_multicast=str(data["grupo_multicast"]),
        porta_grupo=int(data["porta_grupo"]),
        nos=nos,
    )


def build_config(
    n: int,
    host: str = "127.0.0.1",
    base_port: int = 5000,
    grupo: str = "224.1.1.1",
    porta_grupo: int = 5007,
) -> dict:
    if n < 1:
        raise ValueError("n deve ser >= 1")
    return {
        "grupo_multicast": grupo,
        "porta_grupo": porta_grupo,
        "nos": [{"id": i, "host": host, "porta": base_port + i} for i in range(1, n + 1)],
    }
