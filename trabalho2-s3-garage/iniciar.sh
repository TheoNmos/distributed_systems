#!/bin/sh
# Sobe os três nós, conecta o cluster, aplica o layout e cria bucket/chave.
set -eu
cd "$(dirname "$0")"

./preparar.sh
docker compose up -d garage1 garage2 garage3

wait_cli() {
  name="$1"
  i=0
  while [ "$i" -lt 45 ]; do
    if docker exec "$name" /garage node id >/tmp/garage-node-id.txt 2>/dev/null; then
      return 0
    fi
    i=$((i + 1))
    sleep 1
  done
  echo "nó $name não respondeu ao CLI" >&2
  docker logs "$name" | tail -80 >&2
  exit 1
}

wait_cli garage1
wait_cli garage2
wait_cli garage3

node_ref() {
  docker exec "$1" /garage node id | tr -d '\r' | awk '/@/ { print; exit }'
}

id1=$(node_ref garage1)
id2=$(node_ref garage2)
id3=$(node_ref garage3)
echo "garage1 $id1"
echo "garage2 $id2"
echo "garage3 $id3"

docker exec garage1 /garage node connect "$id2" || true
docker exec garage1 /garage node connect "$id3" || true

i=0
while [ "$i" -lt 30 ]; do
  status=$(docker exec garage1 /garage status || true)
  echo "$status"
  healthy=$(printf '%s\n' "$status" | awk 'BEGIN{h=0} /HEALTHY/{h=1} h && /172\.30\.0\./{c++} END{print c+0}')
  if [ "$healthy" -ge 3 ]; then
    break
  fi
  i=$((i + 1))
  sleep 1
done

hex() {
  printf '%s\n' "$1" | awk -F@ '{ print $1 }'
}

docker exec garage1 /garage layout assign -z dz1 -c 1GB "$(hex "$id1")"
docker exec garage1 /garage layout assign -z dz2 -c 1GB "$(hex "$id2")"
docker exec garage1 /garage layout assign -z dz3 -c 1GB "$(hex "$id3")"
docker exec garage1 /garage layout show
docker exec garage1 /garage layout apply --version 1
docker exec garage1 /garage status

if ! docker exec garage1 /garage bucket info sdlab >/dev/null 2>&1; then
  docker exec garage1 /garage bucket create sdlab
fi

key_out=$(docker exec garage1 /garage key create sd-key)
printf '%s\n' "$key_out"
key_id=$(printf '%s\n' "$key_out" | awk -F': ' '/Key ID/ { gsub(/^[ \t]+/, "", $2); print $2 }')
secret=$(printf '%s\n' "$key_out" | awk -F': ' '/Secret key/ { gsub(/^[ \t]+/, "", $2); print $2 }')
if [ -z "$key_id" ] || [ -z "$secret" ]; then
  echo "não foi possível ler a chave S3" >&2
  exit 1
fi

docker exec garage1 /garage bucket allow --read --write --owner sdlab --key "$key_id"

cat > .env <<EOF
AWS_ACCESS_KEY_ID=${key_id}
AWS_SECRET_ACCESS_KEY=${secret}
AWS_DEFAULT_REGION=garage
S3_BUCKET=sdlab
EOF
chmod 600 .env

docker compose up -d --force-recreate cliente

i=0
while [ "$i" -lt 90 ]; do
  if docker exec cliente sh -c 'test -f /tmp/ready && python -c "import boto3"' >/dev/null 2>&1; then
    echo "cliente pronto"
    exit 0
  fi
  i=$((i + 1))
  sleep 2
done
echo "cliente não instalou o boto3 a tempo" >&2
docker logs cliente | tail -40 >&2
exit 1
