# Chat distribuído

Sistema de bate-papo entre processos independentes, com **comunicação de grupo em multicast UDP**, **ordem total** definida por um **líder sequenciador** e **eleição Bully**. Mensagens privadas, eleição, retransmissão e snapshot usam **TCP unicast**.

Cada nó é um processo do sistema operacional. O único arquivo compartilhado é o catálogo estático `config/nos.json`, lido na inicialização.

## Requisitos

- Python 3.10+ (apenas biblioteca padrão)
- macOS/Linux; para vários painéis interativos, `tmux` é opcional

## Subir os nós

Na raiz do repositório:

```bash
# gera o catálogo e abre N painéis (3, 8 ou 15)
chmod +x scripts/run.sh
./scripts/run.sh 3

# 15 nós em background (logs em logs/no_*.log)
./scripts/run.sh 15 --bg
```

Ou manualmente:

```bash
python3 scripts/gen_config.py --n 3 --out config/nos.json
python3 node.py --id 1 --config config/nos.json
python3 node.py --id 2 --config config/nos.json
python3 node.py --id 3 --config config/nos.json
```

Endereços padrão em localhost: multicast `224.1.1.1:5007`, unicast `127.0.0.1` porta `5000 + id`.

## Comandos (em cada processo)

| Comando | Efeito |
|---|---|
| `/msg <id> <texto>` | mensagem privada (TCP) |
| `/group <texto>` | mensagem de grupo (UDP + ordem total) |
| `/snapshot` | estado global Chandy-Lamport |
| `/status` | vetor, líder, buffer, ordem local e global |
| `/crash` | simula queda do nó (para heartbeat; dispara Bully) |
| `/help` | ajuda |
| `/quit` | encerra este processo |

A **ordem global** impressa (`[GLOBAL #n]`) deve ser **idêntica** em todos os nós após as mensagens de grupo.

## Arquitetura

- **Grupo:** UDP multicast (`GROUP` + `SEQ`). UDP pode perder datagramas; lacunas geram `NACK` e retransmissão TCP. O líder também reenvia `SEQ` por TCP.
- **Ordem total:** o líder atribui `global_seq` monotônico. Entrega só quando `seq == last_delivered + 1` e o payload já chegou.
- **Causalidade:** relógio vetorial no envio/entrega (exibido na tela e no snapshot); não desempatará a ordem total.
- **Bully:** maior ID vivo vence. Heartbeat do líder; silêncio inicia eleição. Após reeleição, `global_seq` continua de `max(last_seq)+1`.
- **Snapshot:** marcadores Chandy-Lamport nos canais TCP FIFO.

## Demonstração sugerida

1. Subir 3 nós; o de maior ID vira coordenador.
2. Em nós diferentes, `/group` quase ao mesmo tempo; conferir `/status` — filas globais iguais.
3. `/msg 2 olá` a partir do nó 1.
4. `/snapshot` em qualquer nó.
5. `/crash` no líder; outro nó anuncia `COORDINATOR`; novas `/group` continuam a sequência.
6. Repetir com `./scripts/run.sh 15 --bg` e inspecionar `logs/no_*.log`.
