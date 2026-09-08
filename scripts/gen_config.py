#!/usr/bin/env python3
"""Gera o catálogo estático de N nós sem alterar o código dos processos."""

from __future__ import annotations

import argparse
import json
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
sys.path.insert(0, str(ROOT))

from config import build_config


def main() -> None:
    parser = argparse.ArgumentParser(description="Gera config/nos.json para N nós")
    parser.add_argument("--n", type=int, default=3, help="quantidade de nós (>= 1)")
    parser.add_argument(
        "--out",
        default=str(ROOT / "config" / "nos.json"),
        help="arquivo de saída",
    )
    args = parser.parse_args()
    data = build_config(args.n)
    out = Path(args.out)
    out.parent.mkdir(parents=True, exist_ok=True)
    out.write_text(json.dumps(data, indent=2, ensure_ascii=False) + "\n", encoding="utf-8")
    print(f"escreveu {args.n} nós em {out}")


if __name__ == "__main__":
    main()
