#!/usr/bin/env python3
"""Executa a bateria E1–E9 e grava a saída bruta em evidencias/."""

import os
import shutil
import statistics
import subprocess
import sys
import time
from pathlib import Path

ROOT = Path(__file__).resolve().parent
OUT = ROOT / "evidencias"
os.chdir(ROOT)


def run(cmd, check=False, timeout=180):
    print(f"\n$ {' '.join(cmd)}", flush=True)
    proc = subprocess.run(cmd, text=True, capture_output=True, timeout=timeout)
    if proc.stdout:
        print(proc.stdout, end="" if proc.stdout.endswith("\n") else "\n")
    if proc.stderr:
        print(proc.stderr, end="" if proc.stderr.endswith("\n") else "\n")
    if check and proc.returncode != 0:
        raise SystemExit(f"comando falhou ({proc.returncode}): {' '.join(cmd)}")
    return proc


def s3(*args, timeout=180):
    return run(["docker", "exec", "cliente", "python", "-u", "/s3.py", *args], timeout=timeout)


def garage(*args, timeout=120):
    return run(["docker", "exec", "garage1", "/garage", *args], timeout=timeout)


def save(name, text):
    path = OUT / name
    path.parent.mkdir(parents=True, exist_ok=True)
    path.write_text(text)
    print(f"[gravado] {path.relative_to(ROOT)}")


class Log:
    def __init__(self, name):
        self.name = name
        self.chunks = []
        self._stdout = sys.stdout
        self._stderr = sys.stderr

    def write(self, data):
        self.chunks.append(data)
        self._stdout.write(data)
        self._stdout.flush()

    def flush(self):
        self._stdout.flush()

    def __enter__(self):
        sys.stdout = self
        sys.stderr = self
        return self

    def __exit__(self, *_exc):
        sys.stdout = self._stdout
        sys.stderr = self._stderr
        save(self.name, "".join(self.chunks))


def du():
    proc = run(["du", "-sh", "dados/g1/data", "dados/g2/data", "dados/g3/data"])
    return proc.stdout


def wait_healthy(seconds=40):
    deadline = time.time() + seconds
    last = ""
    while time.time() < deadline:
        proc = run(["docker", "exec", "garage1", "/garage", "status"])
        last = proc.stdout
        if last.count("172.30.0.") >= 3 and "UNAVAILABLE" not in last and "FAILED" not in last.split("HEALTHY")[-1]:
            return last
        time.sleep(2)
    print("cluster não voltou a mostrar 3 nós saudáveis no tempo esperado")
    return last


def netem(container, delay_ms):
    script = f"""
set -eu
apk add --no-cache iproute2 >/dev/null
iface=$(ip -o -4 addr show | awk '/172\\.30\\./ {{print $2; exit}}')
tc qdisc del dev "$iface" root 2>/dev/null || true
if [ "{delay_ms}" != "0" ]; then
  tc qdisc add dev "$iface" root netem delay {delay_ms}ms
fi
echo "container={container} iface=$iface delay_ms={delay_ms}"
tc qdisc show dev "$iface"
ip -4 addr show
"""
    return run(
        [
            "docker", "run", "--rm",
            "--network", f"container:{container}",
            "--cap-add", "NET_ADMIN",
            "alpine:3.20",
            "sh", "-c", script,
        ],
        timeout=120,
    )


def ping_from(container, target):
    return run(
        [
            "docker", "run", "--rm",
            "--network", f"container:{container}",
            "alpine:3.20",
            "ping", "-c", "4", "-W", "2", target,
        ],
        timeout=40,
    )


def bench_five(node, label):
    texts = []
    for i in range(1, 6):
        print(f"\n## {label} repetição {i}/5 no nó {node}")
        proc = s3(node, "bench", "20", timeout=180)
        texts.append(proc.stdout + proc.stderr)
    return "\n".join(texts)


def parse_bench(text):
    puts, gets = [], []
    for line in text.splitlines():
        if line.startswith("BENCH "):
            fields = dict(part.split("=", 1) for part in line.split() if "=" in part)
            puts.append(float(fields["put_mean_ms"]))
            gets.append(float(fields["get_mean_ms"]))
    def summarize(values):
        if not values:
            return "sem amostras"
        mean = statistics.fmean(values)
        sd = statistics.pstdev(values) if len(values) > 1 else 0.0
        samples = ", ".join(f"{v:.2f}" for v in values)
        return f"n={len(values)} amostras_ms=[{samples}] média={mean:.2f} desvio={sd:.2f}"
    return f"PUT (média de cada bench): {summarize(puts)}\nGET (média de cada bench): {summarize(gets)}\n"


def ambiente():
    with Log("00-ambiente.txt"):
        run(["sw_vers"])
        run(["sysctl", "-n", "hw.memsize", "hw.ncpu"])
        run(["df", "-h", "/"])
        run(["docker", "version"])
        run(["docker", "compose", "version"])
        run(["python3", "--version"])
        run(["docker", "exec", "garage1", "/garage", "--version"])
        run(["docker", "exec", "cliente", "python", "-c", "import boto3; print(boto3.__version__)"])
        run(["docker", "compose", "ps"])
        run(["docker", "stats", "--no-stream", "--format",
             "table {{.Name}}\t{{.MemUsage}}\t{{.CPUPerc}}"])


def e1():
    with Log("E1-topologia.txt"):
        garage("status")
        garage("layout", "show")
        run(["docker", "compose", "ps"])


def e2():
    with Log("E2-caminho.txt"):
        print("## previsão: qualquer nó aceita PUT/GET e os blocos existem nos três diretórios")
        print("## du antes")
        du()
        s3("1", "put", "caminho/objeto.txt", "escrito-pelo-garage1")
        s3("3", "get", "caminho/objeto.txt")
        print("## du depois")
        du()
        garage("stats")


def e3():
    with Log("E3-replicacao.txt"):
        print("## previsão: arquivo de 4 MiB aparece nos três data dirs; latência de PUT > GET")
        print("## du antes")
        du()
        s3("1", "putmb", "replicacao/arquivo.bin", "4", timeout=120)
        print("## du depois")
        du()
        text = bench_five("2", "bench")
        print("\n## resumo das 5 repetições")
        print(parse_bench(text))


def e4():
    with Log("E4-consistencia.txt"):
        print("## previsão: no modo consistent, leitura imediata nos outros nós devolve o valor novo")
        s3("1", "fanout", "consistencia/objeto.txt", "20", timeout=180)
        s3("1", "lscheck", "10", timeout=180)


def e5():
    with Log("E5-falha-quorum.txt"):
        print("## previsão: 2 nós ativos continuam; 1 nó ativo perde o quórum de 2")
        print("\n## 3 nós ativos")
        s3("1", "put", "falha/tres.txt", "ok-tres")
        s3("2", "get", "falha/tres.txt")
        print("\n## parar garage3 (minoria)")
        run(["docker", "stop", "garage3"], check=True)
        time.sleep(2)
        s3("1", "put", "falha/dois.txt", "ok-dois", timeout=60)
        s3("2", "get", "falha/dois.txt", timeout=60)
        print("\n## parar garage2 (resta 1 nó)")
        run(["docker", "stop", "garage2"], check=True)
        time.sleep(2)
        s3("1", "put", "falha/um.txt", "nao-deve", timeout=60)
        s3("1", "get", "falha/dois.txt", timeout=60)
        print("\n## religar garage2 e garage3")
        run(["docker", "start", "garage2", "garage3"], check=True)
        wait_healthy(60)
        s3("3", "get", "falha/dois.txt", timeout=60)


def e6_e7():
    with Log("E6-E7-particao-recuperacao.txt"):
        print("## previsão E6: lado majoritário lê e escreve; lado minoritário não atinge quórum")
        print("## desconectar garage3 da rede interna, mantendo a rede de clientes")
        run(["docker", "network", "disconnect", "sdlab-interna", "garage3"], check=True)
        time.sleep(2)
        print("\n## PUT/GET pelo lado majoritário (garage1)")
        s3("1", "put", "particao/majoritario.txt", "escrito-na-maioria", timeout=60)
        s3("1", "get", "particao/majoritario.txt", timeout=60)
        print("\n## PUT/GET pelo lado minoritário (garage3)")
        s3("3", "put", "particao/minoritario.txt", "escrito-na-minoria", timeout=60)
        s3("3", "get", "particao/majoritario.txt", timeout=60)
        s3("3", "get", "caminho/objeto.txt", timeout=60)
        print("\n## previsão E7: ao reconectar, o objeto da maioria chega ao garage3")
        started = time.perf_counter()
        run([
            "docker", "network", "connect", "sdlab-interna", "garage3",
            "--ip", "172.30.0.23",
        ], check=True)
        deadline = time.time() + 90
        recovered = False
        while time.time() < deadline:
            proc = s3("3", "get", "particao/majoritario.txt", timeout=40)
            if "GET ok" in proc.stdout and "escrito-na-maioria" in proc.stdout:
                recovered = True
                break
            time.sleep(2)
        elapsed = time.perf_counter() - started
        print(f"RECUPERACAO recovered={str(recovered).lower()} elapsed_s={elapsed:.2f}")
        print("\n## repair tables e blocks")
        t0 = time.perf_counter()
        garage("repair", "--all-nodes", "--yes", "tables", timeout=180)
        garage("repair", "--all-nodes", "--yes", "blocks", timeout=180)
        print(f"REPAIR_WALL_S={time.perf_counter() - t0:.2f}")
        time.sleep(3)
        du()
        garage("status")
        s3("2", "get", "particao/majoritario.txt")


def e8():
    with Log("E8-coordenacao.txt"):
        print("## previsão: não há líder; em PUTs concorrentes sobrevive um dos dois valores (LWW)")
        garage("status")
        winners = []
        for i in range(8):
            key = f"concorrencia/chave-{i}.txt"
            p1 = subprocess.Popen(
                ["docker", "exec", "cliente", "python", "-u", "/s3.py", "1", "put", key, f"A{i}"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            p2 = subprocess.Popen(
                ["docker", "exec", "cliente", "python", "-u", "/s3.py", "2", "put", key, f"B{i}"],
                text=True, stdout=subprocess.PIPE, stderr=subprocess.STDOUT,
            )
            out1, _ = p1.communicate(timeout=60)
            out2, _ = p2.communicate(timeout=60)
            print(out1, end="" if out1.endswith("\n") else "\n")
            print(out2, end="" if out2.endswith("\n") else "\n")
            time.sleep(0.3)
            got = s3("3", "get", key, timeout=40)
            line = got.stdout
            winner = "A" if f"value=A{i}" in line else "B" if f"value=B{i}" in line else "?"
            winners.append(winner)
            print(f"RACE i={i} winner={winner}")
        print("RACE_WINNERS " + " ".join(winners))
        garage("layout", "show")
        garage("layout", "history")


def e9():
    with Log("E9-geodistribuicao.txt"):
        print("## previsão: com atrasos 50/100/200 ms, a latência de escrita segue o 2º caminho mais rápido")
        print("## linha de base sem atraso, 5 x bench 20 via garage1")
        base = bench_five("1", "baseline")
        print(parse_bench(base))
        print("\n## aplicar netem na interface interna")
        delays = {"garage1": 50, "garage2": 100, "garage3": 200}
        for name, delay in delays.items():
            netem(name, delay)
        print("\n## RTT a partir do netns de cada nó")
        ping_from("garage1", "172.30.0.22")
        ping_from("garage1", "172.30.0.23")
        ping_from("garage2", "172.30.0.23")
        print("\n## bench com os três nós e atrasos, via garage1 (50 ms)")
        all_up = bench_five("1", "atraso-tres-nos")
        print(parse_bench(all_up))
        print("\n## parar garage2 (100 ms): o quórum passa a incluir garage3 (200 ms)")
        run(["docker", "stop", "garage2"], check=True)
        time.sleep(2)
        slow = bench_five("1", "sem-garage2")
        print(parse_bench(slow))
        print("\n## religar garage2 e parar garage3 (200 ms)")
        run(["docker", "start", "garage2"], check=True)
        run(["docker", "stop", "garage3"], check=True)
        time.sleep(3)
        # o qdisc do garage2 foi perdido no stop/start; reaplicar
        netem("garage2", 100)
        mid = bench_five("1", "sem-garage3")
        print(parse_bench(mid))
        print("\n## restaurar")
        run(["docker", "start", "garage3"], check=True)
        time.sleep(2)
        for name in ("garage1", "garage2", "garage3"):
            netem(name, 0)
        wait_healthy(40)


def main():
    if shutil.which("docker") is None:
        raise SystemExit("docker não está no PATH")
    OUT.mkdir(exist_ok=True)
    phases = sys.argv[1:] or ["all"]
    if "all" in phases:
        phases = ["ambiente", "e1", "e2", "e3", "e4", "e8", "e5", "e6", "e9"]
    table = {
        "ambiente": ambiente,
        "e1": e1,
        "e2": e2,
        "e3": e3,
        "e4": e4,
        "e5": e5,
        "e6": e6_e7,
        "e7": e6_e7,
        "e8": e8,
        "e9": e9,
    }
    for phase in phases:
        print(f"\n======== {phase} ========")
        table[phase]()


if __name__ == "__main__":
    main()
