#!/bin/sh
set -eu

target_dir=${1:-/certs}
mkdir -p "$target_dir"

set_permissions() {
  chown 911:911 \
    "$target_dir/ca.crt" \
    "$target_dir/ldap.crt" \
    "$target_dir/ldap.key" \
    "$target_dir/dhparam.pem"
  chmod 0644 "$target_dir/ca.crt" "$target_dir/ldap.crt" "$target_dir/dhparam.pem"
  chmod 0640 "$target_dir/ldap.key"
}

complete=true
for file in ca.crt ldap.crt ldap.key dhparam.pem; do
  if [ ! -s "$target_dir/$file" ]; then
    complete=false
  fi
done

if [ "$complete" = true ]; then
  set_permissions
  exit 0
fi

work_dir=$(mktemp -d "$target_dir/.generate.XXXXXX")
cleanup() {
  rm -rf "$work_dir"
}
trap cleanup EXIT HUP INT TERM
umask 077

openssl req -x509 -newkey rsa:2048 -sha256 -nodes -days 30 \
  -subj "/CN=Agent Hub QA CA" \
  -keyout "$work_dir/ca.key" \
  -out "$work_dir/ca.crt"

openssl req -newkey rsa:2048 -sha256 -nodes \
  -subj "/CN=openldap" \
  -keyout "$work_dir/ldap.key" \
  -out "$work_dir/ldap.csr"

cat > "$work_dir/server.ext" <<'EOF'
basicConstraints=CA:FALSE
keyUsage=digitalSignature,keyEncipherment
extendedKeyUsage=serverAuth
subjectAltName=DNS:openldap,DNS:localhost,IP:127.0.0.1
EOF

openssl x509 -req -sha256 -days 30 \
  -in "$work_dir/ldap.csr" \
  -CA "$work_dir/ca.crt" \
  -CAkey "$work_dir/ca.key" \
  -CAcreateserial \
  -extfile "$work_dir/server.ext" \
  -out "$work_dir/ldap.crt"

openssl dhparam -out "$work_dir/dhparam.pem" 2048

for file in ca.crt ldap.crt ldap.key dhparam.pem; do
  mv "$work_dir/$file" "$target_dir/$file"
done

set_permissions
