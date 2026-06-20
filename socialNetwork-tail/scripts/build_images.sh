#!/usr/bin/env bash
#
# Build (and optionally push) socialNetwork app images for tail sampling:
#   - deps base image    -> thrift-microservice-deps:jammy (docker/thrift-microservice-deps/cpp/Dockerfile)
#   - C++ microservices  -> sn-services-otel            (root Dockerfile)
#   - Go frontend        -> social-network-frontend-go-otel (frontend/Dockerfile)
#
# In a multi-node Swarm the deploy uses `--resolve-image always`, which PULLS
# from a registry. So rebuilt images must be pushed to a registry every node can
# reach (use --push), otherwise the cluster keeps running the old images.
#
# Usage:
#   ./scripts/build_images.sh [options]
#
# Options:
#   -r, --registry <ns>   Image namespace/registry prefix (default: varungohil)
#   -t, --tag <tag>       Image tag (default: latest)
#   -p, --push            Push images after building
#       --deps-only       Build only the thrift-microservice-deps base image
#       --services-only   Build only the C++ services image (implies deps unless --no-deps)
#       --frontend-only   Build only the Go frontend image
#       --no-deps         Skip deps build when building services
#       --no-cache        Pass --no-cache to docker build
#   -h, --help            Show this help
#
# Examples:
#   ./scripts/build_images.sh                       # build both, tag varungohil/*:latest
#   ./scripts/build_images.sh -r myrepo -t tail -p  # build myrepo/*:tail and push
#
set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")" && pwd)"
ROOT_DIR="$(cd "${SCRIPT_DIR}/.." && pwd)"

REGISTRY="varungohil"
TAG="latest"
PUSH=false
BUILD_DEPS=true
BUILD_SERVICES=true
BUILD_FRONTEND=true
NO_CACHE=""

DEPS_IMAGE_NAME="thrift-microservice-deps"
DEPS_TAG="jammy"
SERVICES_IMAGE_NAME="sn-services-otel"
FRONTEND_IMAGE_NAME="social-network-frontend-go-otel"

log() { printf '==> %s\n' "$*"; }
err() { printf 'ERROR: %s\n' "$*" >&2; }

usage() {
  awk 'NR==1{next} /^#/{sub(/^# ?/,""); print; next} {exit}' "${BASH_SOURCE[0]}"
  exit "${1:-0}"
}

while [[ $# -gt 0 ]]; do
  case "$1" in
    -r|--registry) REGISTRY="$2"; shift 2 ;;
    -t|--tag)      TAG="$2"; shift 2 ;;
    -p|--push)     PUSH=true; shift ;;
    --deps-only) BUILD_DEPS=true; BUILD_SERVICES=false; BUILD_FRONTEND=false; shift ;;
    --services-only) BUILD_FRONTEND=false; shift ;;
    --frontend-only) BUILD_DEPS=false; BUILD_SERVICES=false; shift ;;
    --no-deps) BUILD_DEPS=false; shift ;;
    --no-cache)    NO_CACHE="--no-cache"; shift ;;
    -h|--help)     usage 0 ;;
    *) err "Unknown option: $1"; usage 1 ;;
  esac
done

DEPS_IMAGE="${REGISTRY}/${DEPS_IMAGE_NAME}:${DEPS_TAG}"
SERVICES_IMAGE="${REGISTRY}/${SERVICES_IMAGE_NAME}:${TAG}"
FRONTEND_IMAGE="${REGISTRY}/${FRONTEND_IMAGE_NAME}:${TAG}"

build_image() {
  local image="$1"; local context="$2"; local dockerfile="$3"
  log "Building ${image}"
  docker build ${NO_CACHE} -t "${image}" -f "${dockerfile}" "${context}"
  if [[ "${PUSH}" == "true" ]]; then
    log "Pushing ${image}"
    docker push "${image}"
  fi
}

cd "${ROOT_DIR}"

if [[ "${BUILD_DEPS}" == "true" ]]; then
  build_image "${DEPS_IMAGE}" "${ROOT_DIR}/docker/thrift-microservice-deps/cpp" "${ROOT_DIR}/docker/thrift-microservice-deps/cpp/Dockerfile"
fi

if [[ "${BUILD_SERVICES}" == "true" ]]; then
  # C++ services: build context is the repo root, Dockerfile at repo root.
  build_image "${SERVICES_IMAGE}" "${ROOT_DIR}" "${ROOT_DIR}/Dockerfile"
fi

if [[ "${BUILD_FRONTEND}" == "true" ]]; then
  # Go frontend: build context and Dockerfile under frontend/.
  build_image "${FRONTEND_IMAGE}" "${ROOT_DIR}/frontend" "${ROOT_DIR}/frontend/Dockerfile"
fi

log "Done."
[[ "${BUILD_DEPS}" == "true" ]]      && log "  deps:     ${DEPS_IMAGE}"
[[ "${BUILD_SERVICES}" == "true" ]]  && log "  services: ${SERVICES_IMAGE}"
[[ "${BUILD_FRONTEND}" == "true" ]]  && log "  frontend: ${FRONTEND_IMAGE}"
if [[ "${PUSH}" != "true" ]]; then
  log "Images built locally only. In multi-node Swarm, re-run with --push so all nodes can pull them."
fi
