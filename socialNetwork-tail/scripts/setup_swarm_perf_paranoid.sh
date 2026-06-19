#!/usr/bin/env bash
#
# Set kernel.perf_event_paranoid=0 on every Docker Swarm node so node-exporter
# can collect PMU counters (--collector.perf).
#
# Run from a Swarm manager:
#   ./scripts/setup_swarm_perf_paranoid.sh
#
# Methods:
#   docker  (default) Run a one-shot global Swarm service with --privileged --pid host
#   ssh             SSH to each node hostname from `docker node ls` and run sudo sysctl
#   local           Only configure the machine this script runs on

set -euo pipefail

METHOD="${METHOD:-docker}"
SERVICE_NAME="perf-event-paranoid-setup"
SYSCTL_CMD=(sysctl -w kernel.perf_event_paranoid=0)

usage() {
  cat <<EOF
Usage: $(basename "$0") [OPTIONS]

Set kernel.perf_event_paranoid=0 on Swarm nodes for node-exporter PMU collection.

Options:
  -m, --method METHOD   docker (default), ssh, or local
  -h, --help            Show this help

Environment:
  METHOD                Same as --method
  SSH_USER              SSH user when using --method ssh (default: current user)
  SSH_OPTS              Extra ssh options (default: -o BatchMode=yes -o ConnectTimeout=15)

Examples:
  $(basename "$0")
  $(basename "$0") --method ssh
  SSH_USER=ubuntu $(basename "$0") --method ssh
EOF
}

log() {
  printf '==> %s\n' "$*"
}

err() {
  printf 'ERROR: %s\n' "$*" >&2
}

require_swarm_manager() {
  if ! command -v docker >/dev/null 2>&1; then
    err "docker is not installed or not in PATH"
    exit 1
  fi

  local state
  state="$(docker info --format '{{.Swarm.ControlAvailable}}' 2>/dev/null || true)"
  if [[ "$state" != "true" ]]; then
    err "This node is not a Swarm manager. Run from a manager node."
    exit 1
  fi
}

run_local() {
  log "Setting kernel.perf_event_paranoid=0 on $(hostname -s)"
  if [[ "$(id -u)" -eq 0 ]]; then
    "${SYSCTL_CMD[@]}"
  else
    sudo "${SYSCTL_CMD[@]}"
  fi
}

run_via_ssh() {
  require_swarm_manager

  local ssh_user="${SSH_USER:-${USER}}"
  local ssh_opts="${SSH_OPTS:--o BatchMode=yes -o ConnectTimeout=15}"
  local -a nodes=()
  local failed=0

  mapfile -t nodes < <(docker node ls --format '{{.Hostname}}' | sort -u)

  if [[ ${#nodes[@]} -eq 0 ]]; then
    err "No Swarm nodes found"
    exit 1
  fi

  log "Configuring ${#nodes[@]} node(s) via SSH as ${ssh_user}"

  for host in "${nodes[@]}"; do
    log "${host}"
    # shellcheck disable=SC2086
    if ssh ${ssh_opts} "${ssh_user}@${host}" "sudo ${SYSCTL_CMD[*]}"; then
      printf '    OK\n'
    else
      err "Failed on ${host}"
      failed=$((failed + 1))
    fi
  done

  if [[ "$failed" -gt 0 ]]; then
    err "${failed} node(s) failed"
    exit 1
  fi
}

wait_for_global_service() {
  local service="$1"
  local timeout="${2:-120}"
  local elapsed=0

  log "Waiting for global service '${service}' to complete on all nodes (timeout ${timeout}s)"

  while [[ "$elapsed" -lt "$timeout" ]]; do
    local pending running failed
    pending="$(docker service ps "$service" --format '{{.CurrentState}}' 2>/dev/null | grep -ciE 'pending|preparing|starting|ready' || true)"
    running="$(docker service ps "$service" --format '{{.CurrentState}}' 2>/dev/null | grep -ci 'running' || true)"
    failed="$(docker service ps "$service" --format '{{.CurrentState}}' 2>/dev/null | grep -ci 'failed' || true)"

    if [[ "$failed" -gt 0 ]]; then
      err "One or more tasks failed:"
      docker service ps "$service" --no-trunc || true
      return 1
    fi

    if [[ "$pending" -eq 0 && "$running" -eq 0 ]]; then
      log "Service task status:"
      docker service ps "$service" || true
      return 0
    fi

    sleep 2
    elapsed=$((elapsed + 2))
  done

  err "Timed out waiting for service '${service}'"
  docker service ps "$service" --no-trunc || true
  return 1
}

run_via_docker() {
  require_swarm_manager

  if docker service inspect "$SERVICE_NAME" >/dev/null 2>&1; then
    log "Removing existing service '${SERVICE_NAME}'"
    docker service rm "$SERVICE_NAME" >/dev/null
    sleep 2
  fi

  log "Creating global service '${SERVICE_NAME}' on all Swarm nodes"
  docker service create \
    --name "$SERVICE_NAME" \
    --mode global \
    --restart-condition none \
    --privileged \
    --pid host \
    alpine sh -c "${SYSCTL_CMD[*]} && echo OK on \$(hostname -s)"

  if wait_for_global_service "$SERVICE_NAME"; then
    log "Logs:"
    docker service logs "$SERVICE_NAME" 2>/dev/null || true
  fi

  log "Removing service '${SERVICE_NAME}'"
  docker service rm "$SERVICE_NAME" >/dev/null
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -m|--method)
      METHOD="$2"
      shift 2
      ;;
    -h|--help)
      usage
      exit 0
      ;;
    *)
      err "Unknown option: $1"
      usage
      exit 1
      ;;
  esac
done

case "$METHOD" in
  local)
    run_local
    ;;
  ssh)
    run_via_ssh
    ;;
  docker)
    run_via_docker
    ;;
  *)
    err "Invalid method '${METHOD}'. Use docker, ssh, or local."
    exit 1
    ;;
esac

log "Done"
