#!/usr/bin/env bash
# Sobe N nós independentes (processos OS). Uso: ./scripts/run.sh [N] [--bg]
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
cd "$ROOT"

N="${1:-3}"
MODE="${2:-}"

python3 scripts/gen_config.py --n "$N" --out config/nos.json

if [[ "$MODE" == "--bg" || "$N" -gt 6 ]]; then
  mkdir -p logs
  PIDS=()
  for i in $(seq 1 "$N"); do
    python3 node.py --id "$i" --config config/nos.json < /dev/null > "logs/no_${i}.log" 2>&1 &
    PIDS+=("$!")
  done
  echo "${PIDS[*]}" > logs/pids.txt
  echo "Iniciados $N nós em background."
  echo "Logs: logs/no_*.log"
  echo "Parar: kill \$(cat logs/pids.txt)"
  exit 0
fi

if command -v tmux >/dev/null 2>&1; then
  SESSION="chat-sd"
  tmux has-session -t "$SESSION" 2>/dev/null && tmux kill-session -t "$SESSION"
  tmux new-session -d -s "$SESSION" -n chat "cd '$ROOT' && python3 node.py --id 1 --config config/nos.json"
  for i in $(seq 2 "$N"); do
    tmux split-window -t "$SESSION" "cd '$ROOT' && python3 node.py --id $i --config config/nos.json"
    tmux select-layout -t "$SESSION" tiled
  done
  tmux attach -t "$SESSION"
  exit 0
fi

echo "tmux não encontrado. Abra $N terminais na raiz do repositório:"
for i in $(seq 1 "$N"); do
  echo "  python3 node.py --id $i --config config/nos.json"
done
