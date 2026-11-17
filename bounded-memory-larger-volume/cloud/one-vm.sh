#!/usr/bin/env bash
# One client VM next to a LaserData Cloud cluster, used for every test in turn.
#
#   cloud/one-vm.sh up                       create the VM, copy the kit and the cluster file, install
#   cloud/one-vm.sh run <test> [rep]         run one test (ramp, backlog-2|10|30|40|50, producer, restart), fetch results
#   cloud/one-vm.sh down                     delete the VM
#
# Env: PROJECT, CLUSTER_ENV (file with LASER_CONNECTION_STRING), ZONE (us-central1-a; use a zone in your cluster's region),
# MACHINE (n2-standard-8), LABEL (results/<label>), SCALE_RATE, WORKERS, RAMP_STEPS.
set -euo pipefail
HERE=$(cd "$(dirname "$0")/.." && pwd)
PROJECT=${PROJECT:?set PROJECT}; ZONE=${ZONE:-us-central1-a}; MACHINE=${MACHINE:-n2-standard-8}
VM=${VM:-scale-client-1}; LABEL=${LABEL:-cloud-large-3node}
g() { gcloud --project "$PROJECT" --quiet "$@"; }
sshv() { g compute ssh "$VM" --zone "$ZONE" --command "$*"; }
case "${1:-}" in
  up)
    CLUSTER_ENV=${CLUSTER_ENV:?set CLUSTER_ENV}
    g compute instances create "$VM" --zone "$ZONE" --machine-type "$MACHINE" \
      --image-family ubuntu-2404-lts-amd64 --image-project ubuntu-os-cloud --boot-disk-size 30GB \
      --labels purpose=laserdata-footprint-scale --metadata startup-script='#!/bin/bash
apt-get update -q && DEBIAN_FRONTEND=noninteractive apt-get install -y -q python3-venv iptables && touch /opt/apt-ready' >/dev/null
    until sshv "test -f /opt/apt-ready" >/dev/null 2>&1; do sleep 10; done
    KIT=$(mktemp -d)/kit.tgz; tar czf "$KIT" -C "$HERE" --exclude results --exclude __pycache__ .
    g compute scp --zone "$ZONE" "$KIT" "$CLUSTER_ENV" "$VM:/tmp/" >/dev/null
    sshv "sudo mkdir -p /opt/fp/kit && sudo chown -R \$USER /opt/fp && tar xzf /tmp/kit.tgz -C /opt/fp/kit && \
      mv /tmp/$(basename "$CLUSTER_ENV") /opt/fp/cluster.env && chmod 600 /opt/fp/cluster.env && \
      python3 -m venv /opt/fp/venv && /opt/fp/venv/bin/pip install -q -r /opt/fp/kit/requirements.txt && echo ready" | tail -1
    ;;
  run)
    T=${2:?test}; REP=${3:-1}; OUT=/opt/fp/out-$T-r$REP
    sshv "rm -f /opt/fp/done; cd /opt/fp && nohup setsid bash -c 'set -a; . /opt/fp/cluster.env; set +a; \
      env TEST=$T PYTHON=/opt/fp/venv/bin/python FOOTPRINT_OUT=$OUT SCALE_RATE=${SCALE_RATE:-10000} \
      WORKERS=${WORKERS:-6} RAMP_STEPS=${RAMP_STEPS:-} bash kit/run.sh > $OUT.log 2>&1; touch /opt/fp/done' \
      > /dev/null 2>&1 < /dev/null &" >/dev/null 2>&1 &
    STARTER=$!; sleep 20; kill $STARTER 2>/dev/null || true
    echo "started $T rep $REP at $(date -u +%T)"
    until sshv "test -f /opt/fp/done" >/dev/null 2>&1; do sleep 30; done
    D="$HERE/results/$LABEL/rep-$REP-$T"; mkdir -p "$D"
    sshv "cp $OUT.log $OUT/run.log; rm -f $OUT/seen-*.bin; tar czf /tmp/out.tgz -C $OUT ." >/dev/null
    g compute scp --zone "$ZONE" "$VM:/tmp/out.tgz" "$D/" >/dev/null
    tar xzf "$D/out.tgz" -C "$D" && rm "$D/out.tgz"
    tail -4 "$D/run.log"; echo "finished $T rep $REP at $(date -u +%T) -> $D"
    ;;
  down)
    g compute instances delete "$VM" --zone "$ZONE" >/dev/null 2>&1 || true
    echo "test VMs left: $(g compute instances list --filter='labels.purpose=laserdata-footprint-scale' --format='value(name)' | wc -l | tr -d ' ')"
    ;;
  *) echo "usage: $0 up | run <test> [rep] | down"; exit 2 ;;
esac
