# Trabalho 2 — S3 com Garage (ênfase B, geodistribuição)

Laboratório local do roteiro de Sistemas Distribuídos (Univali): três nós Garage `v2.3.0` e um cliente Python com `boto3`.

## Topologia

| Nó | Rede interna (RPC 3901) | Rede de clientes (S3 3900) | Zona | Capacidade |
| --- | --- | --- | --- | --- |
| garage1 | 172.30.0.21 | 172.31.0.21 | dz1 | 1 GB |
| garage2 | 172.30.0.22 | 172.31.0.22 | dz2 | 1 GB |
| garage3 | 172.30.0.23 | 172.31.0.23 | dz3 | 1 GB |
| cliente | — | 172.31.0.100 | — | — |

Administração do Garage: porta 3903, só dentro da rede do contêiner. Replicação `replication_factor = 3`, modo `consistent`.

## Subir

```sh
chmod +x preparar.sh iniciar.sh
./iniciar.sh
```

`iniciar.sh` gera o segredo RPC, sobe os nós, conecta o cluster, aplica o layout versão 1, cria o bucket `sdlab` e a chave `sd-key`, e sobe o cliente.

## Cliente

```sh
docker exec cliente python /s3.py 1 put teste.txt v1
docker exec cliente python /s3.py 3 get teste.txt
docker exec cliente python /s3.py 2 ls
docker exec cliente python /s3.py 2 bench 20
```

O primeiro argumento é o nó de entrada (1, 2 ou 3). Qualquer nó aceita a chamada e a encaminha às réplicas da chave.

## Experimentos

```sh
python3 experimentos.py
```

A saída bruta fica em `evidencias/`. A ordem é E1, E2, E3, E4, E8, E5, E6+E7 e E9, para que falha, partição e atraso de rede aconteçam depois das medições de cluster saudável.

## Parar

```sh
docker compose down
```

`dados/` guarda os blocos e os metadados. Apague essa pasta, `segredos/` e `.env` para recomeçar do zero.
