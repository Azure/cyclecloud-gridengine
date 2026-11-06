#!/bin/bash
set -euo pipefail
export LC_ALL=C

fail() {
    printf 'Scheduler namespace: %s\n' "$*" >&2
    exit 1
}

[[ $# -eq 1 && -n "$1" ]] || fail "Usage: $0 <cluster-name>"
cluster_name=$1

mountpoint -q /sched-main || fail "/sched-main is not a mounted external filesystem"

# Use one hyphen per non-ASCII character.
safe_cluster_name=$(printf '%s' "$cluster_name" |
    sed 's/[\x80-\xBF]//g' |
    tr -c 'a-zA-Z0-9-' '-' |
    tr 'A-Z' 'a-z')
safe_cluster_name=${safe_cluster_name:0:80}
cluster_name_digest=$(printf '%s' "$cluster_name" | sha256sum)
cluster_name_digest=${cluster_name_digest%% *}
cluster_sched_root="/sched-main/${safe_cluster_name}-${cluster_name_digest}"

if mountpoint -q /sched; then
    source_identity=$(stat -Lc '%d:%i' "$cluster_sched_root")
    target_identity=$(stat -Lc '%d:%i' /sched)
    [[ "$source_identity" == "$target_identity" ]] ||
        fail "/sched is already mounted from a different directory; refusing to replace it"
    printf 'Scheduler namespace: /sched already exposes %s\n' "$cluster_sched_root"
    exit 0
fi

mkdir -p /sched
mkdir -p "$cluster_sched_root"
chown root:root "$cluster_sched_root" /sched
chmod 0755 "$cluster_sched_root" /sched
mount --bind "$cluster_sched_root" /sched

printf 'Scheduler namespace: mounted %s at /sched\n' "$cluster_sched_root"
