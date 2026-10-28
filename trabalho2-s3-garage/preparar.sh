#!/bin/sh
# Gera o segredo RPC compartilhado e os três arquivos de configuração.
set -eu
cd "$(dirname "$0")"

mkdir -p dados/g1 dados/g2 dados/g3 segredos

if [ ! -f segredos/rpc_secret ]; then
  openssl rand -hex 32 > segredos/rpc_secret
fi
if [ ! -f segredos/admin_token ]; then
  openssl rand -hex 32 > segredos/admin_token
fi

RPC=$(cat segredos/rpc_secret)
ADMIN=$(cat segredos/admin_token)

write_toml() {
  node="$1"
  rpc_ip="$2"
  cat > "garage${node}.toml" <<EOF
metadata_dir = "/var/lib/garage/meta"
data_dir = "/var/lib/garage/data"
db_engine = "sqlite"
replication_factor = 3
consistency_mode = "consistent"

rpc_bind_addr = "${rpc_ip}:3901"
rpc_public_addr = "${rpc_ip}:3901"
rpc_secret = "${RPC}"

bootstrap_peers = [
  "172.30.0.21:3901",
  "172.30.0.22:3901",
  "172.30.0.23:3901",
]

[s3_api]
s3_region = "garage"
api_bind_addr = "0.0.0.0:3900"
root_domain = ".s3.garage.localhost"

[admin]
api_bind_addr = "0.0.0.0:3903"
admin_token = "${ADMIN}"
EOF
}

write_toml 1 172.30.0.21
write_toml 2 172.30.0.22
write_toml 3 172.30.0.23
echo "configuração gerada (garage1.toml, garage2.toml, garage3.toml)"
