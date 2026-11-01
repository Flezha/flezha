#!/bin/sh
# Certificates for Erlang distribution over mutual TLS, for the options file
# beside this script (dist_tls.conf).
#
#   make-certs.sh ca <dir>
#     Creates the distribution authority, <dir>/ca.pem and <dir>/ca.key, and
#     refuses to overwrite one. Use it for distribution only: any certificate
#     it signs joins the cluster with the cookie. Keep ca.key off the
#     controllers.
#
#   make-certs.sh node <dir> <name> [--dns NAME]... [--ip ADDR]...
#     Creates <dir>/<name>/ holding node.pem, node.key and a copy of ca.pem,
#     the three files dist_tls.conf reads from /etc/flezha/dist/. The
#     certificate carries serverAuth and clientAuth, and the names given in its
#     subject alternative name, which must include the host part of the node's
#     name (flezha@<host>): a peer dialling the node checks it.
#
# Every file is written readable by its owner only. DAYS sets the validity in
# days (default 825).
set -eu

umask 077
days=${DAYS:-825}

usage() {
  echo "usage: $0 ca <dir>" >&2
  echo "       $0 node <dir> <name> [--dns NAME]... [--ip ADDR]..." >&2
  exit 2
}

die() {
  echo "make-certs.sh: $*" >&2
  exit 1
}

new_key() {
  openssl genpkey -algorithm EC -pkeyopt ec_paramgen_curve:P-256 -out "$1" 2>/dev/null
}

make_ca() {
  dir=$1
  if [ -e "$dir/ca.key" ] || [ -e "$dir/ca.pem" ]; then
    die "$dir already holds an authority; refusing to overwrite it"
  fi
  mkdir -p "$dir"
  new_key "$dir/ca.key"
  openssl req -x509 -new -key "$dir/ca.key" -sha256 -days "$days" \
    -subj "/CN=Flezha distribution CA" \
    -addext "basicConstraints=critical,CA:TRUE" \
    -addext "keyUsage=critical,keyCertSign,cRLSign" \
    -out "$dir/ca.pem"
}

make_node() {
  dir=$1
  name=$2
  shift 2
  if [ ! -f "$dir/ca.key" ] || [ ! -f "$dir/ca.pem" ]; then
    die "no authority in $dir; run: $0 ca $dir"
  fi
  case $name in
    '' | */* | .*) die "a node name is one path segment, not '$name'" ;;
  esac

  san=""
  while [ $# -gt 0 ]; do
    [ $# -ge 2 ] || usage
    case $1 in
      --dns) san="$san,DNS:$2" ;;
      --ip) san="$san,IP:$2" ;;
      *) usage ;;
    esac
    shift 2
  done
  [ -n "$san" ] || die "name the node's host with --dns or --ip"

  out="$dir/$name"
  [ ! -e "$out" ] || die "$out exists; refusing to overwrite it"
  mkdir -p "$out"
  new_key "$out/node.key"

  ext="$out/.ext"
  printf '%s\n' \
    "basicConstraints=critical,CA:FALSE" \
    "keyUsage=critical,digitalSignature" \
    "extendedKeyUsage=serverAuth,clientAuth" \
    "subjectAltName=${san#,}" >"$ext"
  openssl req -new -key "$out/node.key" -subj "/CN=$name" |
    openssl x509 -req -CA "$dir/ca.pem" -CAkey "$dir/ca.key" \
      -set_serial "0x$(openssl rand -hex 16)" -sha256 -days "$days" \
      -extfile "$ext" -out "$out/node.pem" 2>/dev/null
  rm -f "$ext"
  cp "$dir/ca.pem" "$out/ca.pem"
}

[ $# -ge 2 ] || usage
case $1 in
  ca) [ $# -eq 2 ] || usage; make_ca "$2" ;;
  node) [ $# -ge 3 ] || usage; shift; make_node "$@" ;;
  *) usage ;;
esac
