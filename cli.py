"""Interface de terminal do nó (requisito R5)."""

from __future__ import annotations

import sys
from collections.abc import Callable

HELP = """
Comandos:
  /msg <id> <texto>   mensagem privada (unicast TCP)
  /group <texto>      mensagem de grupo (multicast UDP + ordem total)
  /snapshot           captura estado global (Chandy-Lamport)
  /status             vetor, líder, seq, buffer, ordens local e global
  /crash              simula queda (para heartbeat / eleição)
  /help               esta ajuda
  /quit               encerra este processo
""".strip()


def format_status(
    node_id: int,
    leader_id,
    crashed: bool,
    last_seq: int,
    next_seq,
    vector: list[int],
    vector_ids: list[int],
    buffer_groups: int,
    buffer_seqs: int,
    unsequenced: int,
    local_order: list[str],
    global_order: list[str],
    privadas: list[str],
) -> str:
    papel = "LÍDER" if leader_id == node_id and not crashed else f"líder={leader_id}"
    if crashed:
        papel = "CRASHED"
    lines = [
        "=" * 60,
        f" Nó {node_id} | {papel} | last_seq={last_seq} | next_seq={next_seq}",
        f" Vetor {vector_ids}: {vector}",
        f" Buffer: {buffer_groups} GROUP, {buffer_seqs} SEQ, {unsequenced} sem número global",
        "-" * 60,
        " Ordem local (emissões deste nó):",
    ]
    if local_order:
        lines.extend(f"  {item}" for item in local_order)
    else:
        lines.append("  (vazia)")
    lines.append(" Ordem global (delivery — deve ser igual em todos os nós):")
    if global_order:
        lines.extend(f"  {item}" for item in global_order)
    else:
        lines.append("  (vazia)")
    lines.append(" Privadas:")
    if privadas:
        lines.extend(f"  {item}" for item in privadas)
    else:
        lines.append("  (nenhuma)")
    lines.append("=" * 60)
    return "\n".join(lines)


def run_cli(
    node_id: int,
    send_private: Callable[[int, str], None],
    send_group: Callable[[str], None],
    snapshot: Callable[[], None],
    status: Callable[[], str],
    crash: Callable[[], None],
    shutdown: Callable[[], None],
    log: Callable[[str], None],
) -> None:
    log(f"[cli] nó {node_id} pronto. Digite /help.")
    while True:
        try:
            line = input(f"nó{node_id}> ").strip()
        except EOFError:
            if sys.stdin.isatty():
                log("[cli] encerrando entrada")
                shutdown()
            break
        except KeyboardInterrupt:
            log("[cli] encerrando")
            shutdown()
            break
        if not line:
            continue
        if line in ("/quit", "/exit"):
            shutdown()
            break
        if line == "/help":
            log(HELP)
            continue
        if line == "/status":
            log(status())
            continue
        if line == "/snapshot":
            snapshot()
            continue
        if line == "/crash":
            crash()
            continue
        if line.startswith("/msg "):
            parts = line.split(None, 2)
            if len(parts) < 3:
                log("uso: /msg <id> <texto>")
                continue
            try:
                dest = int(parts[1])
            except ValueError:
                log("id de destino inválido")
                continue
            send_private(dest, parts[2])
            continue
        if line.startswith("/group "):
            texto = line[len("/group ") :].strip()
            if not texto:
                log("uso: /group <texto>")
                continue
            send_group(texto)
            continue
        log("comando desconhecido — /help")
