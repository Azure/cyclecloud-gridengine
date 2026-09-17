#!/usr/bin/env bash
set -euo pipefail

usage() {
  echo "Usage: $0 [--scalelib FILE | FILE] (build locally without publishing)"
}

scalelib=
case $# in
  0) ;;
  1)
    if [[ "$1" == --help ]]; then
      usage
      exit 0
    fi
    [[ "$1" != --* ]] || { usage >&2; exit 1; }
    scalelib=$1
    ;;
  2)
    [[ "$1" == --scalelib ]] || { usage >&2; exit 1; }
    scalelib=$2
    [[ -n "$scalelib" ]] || { usage >&2; exit 1; }
    ;;
  *) usage >&2; exit 1 ;;
esac
if [[ -n "$scalelib" && ( ! -f "$scalelib" || ! -r "$scalelib" ) ]]; then
  echo "Scalelib archive is not a readable file: $scalelib" >&2
  exit 1
fi

source_dir=$(cd -- "$(dirname -- "${BASH_SOURCE[0]}")" && pwd)
command -v docker >/dev/null || { echo "Docker is required." >&2; exit 1; }
docker info >/dev/null
mkdir -p "$source_dir/build"
staging=$(mktemp -d "$source_dir/build/docker-package.XXXXXX")
trap 'rm -rf -- "$staging"' EXIT
mkdir "$staging/output"
docker_args=()
if [[ -n "$scalelib" ]]; then
  mkdir "$staging/input"
  cp -- "$scalelib" "$staging/input/"
  docker_args+=(--mount "type=bind,source=$staging/input,target=/input,readonly"
    --env "CYCLECLOUD_SCALELIB=/input/$(basename -- "$scalelib")")
fi

echo "Building the release workflow's package and SGE artifacts in Docker."
tar -C "$source_dir" --exclude=.git --exclude=.venv --exclude=.testvenv --exclude=venv \
  --exclude=__pycache__ --exclude='*.egg-info' --exclude=build --exclude=dist \
  -cf - . |
  docker run --rm -i --platform linux/amd64 \
    --mount "type=bind,source=$staging/output,target=/output" \
    "${docker_args[@]}" \
    ubuntu:24.04 /bin/bash -e -o pipefail -c '
      mkdir /work
      tar -xf - -C /work
      cd /work
      export DEBIAN_FRONTEND=noninteractive
      apt-get update
      apt-get install -y python3 python3-yaml curl ca-certificates sudo
      python3 util/local_release.py
      chown "$1:$2" /output/*
    ' -- "$(id -u)" "$(id -g)"

mkdir -p "$source_dir/dist"
mv -- "$staging/output/"* "$source_dir/dist/"
echo "Build complete: $source_dir/dist (nothing published)."