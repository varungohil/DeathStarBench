#!/usr/bin/env bash
#
# Deploy socialNetwork on Docker Swarm with one business microservice per node
# (each with its DB/cache co-located). Global observability (node-exporter,
# otel-collector) still runs on all nodes.
#
# Run from a Swarm manager with the repo at the same path on all nodes:
#   ./scripts/deploy_swarm_one_per_node.sh [stack-name]
#
set -euo pipefail

STACK_NAME="${1:-social-network}"
SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"
SOURCE_COMPOSE="${ROOT_DIR}/docker-compose-swarm.yml"
GENERATED_COMPOSE="${ROOT_DIR}/docker-compose-swarm-placed.yml"

log() { printf '==> %s\n' "$*"; }
err() { printf 'ERROR: %s\n' "$*" >&2; }

require_swarm_manager() {
  if [[ "$(docker info --format '{{.Swarm.ControlAvailable}}' 2>/dev/null || true)" != "true" ]]; then
    err "This node is not a Swarm manager."
    exit 1
  fi
}

get_nodes() {
  docker node ls --format '{{.Hostname}}' | sort -t- -k2 -n
}

generate_placed_compose() {
  python3 - "$SOURCE_COMPOSE" "$GENERATED_COMPOSE" <<'PY'
import re
import sys
from pathlib import Path

source = Path(sys.argv[1])
dest = Path(sys.argv[2])
text = source.read_text()

GLOBAL_SERVICES = {"node-exporter", "otel-collector"}

# One business microservice per node (plus its DB/cache). nginx-web-server and
# media-frontend are omitted (unused). 16 pinned tiers + 3 spare on a 19-node cluster.
NODE_GROUPS = [
    ["frontend"],
    ["compose-post-service"],
    ["home-timeline-service", "home-timeline-redis"],
    ["user-timeline-service", "user-timeline-mongodb", "user-timeline-redis"],
    ["post-storage-service", "post-storage-mongodb", "post-storage-memcached"],
    ["user-service", "user-mongodb", "user-memcached"],
    ["social-graph-service", "social-graph-mongodb", "social-graph-redis"],
    ["media-service", "media-mongodb", "media-memcached"],
    ["url-shorten-service", "url-shorten-mongodb", "url-shorten-memcached"],
    ["text-service"],
    ["unique-id-service"],
    ["user-mention-service"],
    ["cassandra", "cassandra-schema"],
    ["jaeger", "jaeger-query"],
    ["prometheus"],
    ["grafana"],
]

import os
nodes = [n for n in os.environ.get("SWARM_NODES", "").splitlines() if n.strip()]
if not nodes:
    raise SystemExit("SWARM_NODES is empty")

mapping = {}
for idx, group in enumerate(NODE_GROUPS):
    host = nodes[idx % len(nodes)]
    for svc in group:
        mapping[svc] = host

text = re.sub(r"^\s*container_name:.*\n", "", text, flags=re.M)
text = re.sub(r"^\s*restart:\s+always\s*\n", "", text, flags=re.M)

def inject_deploy(service: str, hostname: str) -> None:
    global text
    marker = f"\n  {service}:"
    start = text.find(marker)
    if start == -1:
        raise SystemExit(f"service not found: {service}")
    next_svc = text.find("\n  ", start + len(marker))
    if next_svc == -1:
        block = text[start:]
        rest = ""
    else:
        block = text[start:next_svc]
        rest = text[next_svc:]

    host_line = f'          - node.hostname == {hostname}\n'
    if "placement:" in block and "constraints:" in block:
        block = re.sub(
            r"(\s+placement:\n\s+constraints:\n)(?:\s+- node\.hostname == .+\n)+",
            rf"\1{host_line}",
            block,
            count=1,
        )
    elif re.search(r"\n\s+deploy:\n", block):
        block = re.sub(
            r"(\n\s+deploy:\n)",
            rf"\1      placement:\n        constraints:\n{host_line}",
            block,
            count=1,
        )
    else:
        m = re.search(r"\n    [a-zA-Z0-9_-]+:", block)
        insert_at = m.start() if m else len(block)
        deploy = (
            "\n    deploy:\n"
            "      replicas: 1\n"
            "      restart_policy:\n"
            "        condition: any\n"
            "      placement:\n"
            "        constraints:\n"
            f"{host_line}"
        )
        block = block[:insert_at] + deploy + block[insert_at:]

    text = text[:start] + block + rest

for svc, host in mapping.items():
    inject_deploy(svc, host)

dest.write_text(text)
print("Node groups:")
for idx, group in enumerate(NODE_GROUPS):
    host = nodes[idx % len(nodes)]
    short = host.split(".")[0]
    print(f"  {short}: {', '.join(group)}")
PY
}

wait_for_service_running() {
  local service="$1"
  local timeout="${2:-300}"
  local elapsed=0
  while [[ "$elapsed" -lt "$timeout" ]]; do
    local running desired
    running="$(docker service ps "$service" --filter desired-state=running --format '{{.CurrentState}}' 2>/dev/null | grep -ci running || true)"
    desired="$(docker service ls --filter name="$service" --format '{{.Replicas}}' 2>/dev/null | awk -F/ '{print $2}' || echo 0)"
    if [[ "$running" -ge "$desired" && "$desired" -gt 0 ]]; then
      return 0
    fi
    sleep 3
    elapsed=$((elapsed + 3))
  done
  docker service ps "$service" --no-trunc || true
  return 1
}

main() {
  require_swarm_manager
  cd "$ROOT_DIR"

  log "Setting kernel.perf_event_paranoid=0 on all nodes"
  "${SCRIPT_DIR}/setup_swarm_perf_paranoid.sh"

  mapfile -t nodes < <(get_nodes)
  if [[ ${#nodes[@]} -eq 0 ]]; then
    err "No Swarm nodes found"
    exit 1
  fi
  log "Found ${#nodes[@]} Swarm node(s)"

  export SWARM_NODES
  SWARM_NODES="$(printf '%s\n' "${nodes[@]}")"
  log "Generating ${GENERATED_COMPOSE}"
  generate_placed_compose

  log "Deploying stack '${STACK_NAME}'"
  docker stack deploy --compose-file "$GENERATED_COMPOSE" --resolve-image always "$STACK_NAME"

  log "Waiting for cassandra and jaeger"
  wait_for_service_running "${STACK_NAME}_cassandra" 600 || err "cassandra slow to start"
  wait_for_service_running "${STACK_NAME}_jaeger" 300 || true
  wait_for_service_running "${STACK_NAME}_jaeger-query" 300 || true

  log "Stack services:"
  docker stack services "$STACK_NAME"
  log "Running task placement:"
  docker stack ps "$STACK_NAME" --filter desired-state=running --format 'table {{.Name}}\t{{.Node}}\t{{.CurrentState}}'
  log "Done. Frontend: http://<node-0-ip>:12345"
}

main "$@"
