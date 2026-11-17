#!/usr/bin/env bash
# Runs one test from a client VM against a LaserData Cloud cluster.
#
#   TEST=backlog-50 ./run.sh        consumer backlog test at 50 partitions (backlog-<partitions>)
#   TEST=ramp ./run.sh              find a rate the cluster sustains (run this first)
#   TEST=producer ./run.sh          producer at SCALE_RATE, 60 s network outage halfway
#   TEST=restart ./run.sh           4 GiB on disk, producer + consumer live, graceful restart from the console
#   FOOTPRINT_SCALE=0.1 ...         a 10% trial
#
# Needs LASER_CONNECTION_STRING for the cluster (never written to the results).
# Writes to $FOOTPRINT_OUT: samples.csv (client memory and CPU, once a second), events.jsonl,
# one JSON per step, env.json.
set -uo pipefail
HERE=$(cd "$(dirname "$0")" && pwd)
: "${LASER_CONNECTION_STRING:?set LASER_CONNECTION_STRING}"
export FOOTPRINT_OUT=${FOOTPRINT_OUT:-$HERE/results/local} FOOTPRINT_SCALE=${FOOTPRINT_SCALE:-1}
TEST=${TEST:?set TEST to ramp, backlog-<partitions> or producer}
WORKERS=${WORKERS:-6}
PY=${PYTHON:-python3}
OUT=$FOOTPRINT_OUT
rm -rf "$OUT" && mkdir -p "$OUT"
CLUSTER_HOST=${LASER_CONNECTION_STRING##*@}
{
  echo "{"
  echo "  \"cluster\": \"LaserData Cloud, cluster mode, 3 nodes\", \"cluster_region\": \"$(echo "$CLUSTER_HOST" | cut -d. -f2-3)\","
  echo "  \"laser_sdk\": \"$($PY -c 'import importlib.metadata as m; print(m.version("laser-sdk"))')\","
  echo "  \"python\": \"$($PY -c 'import platform; print(platform.python_version())')\","
  echo "  \"client_host_cpu\": \"$(lscpu | sed -n 's/^Model name:\s*//p' | head -1)\", \"client_cores\": $(nproc), \"client_memory_mib\": $(free -m | awk '/Mem:/{print $2}'),"
  echo "  \"client_zone\": \"$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/zone 2>/dev/null | sed 's#.*/##')\","
  echo "  \"client_machine\": \"$(curl -fs -H 'Metadata-Flavor: Google' http://metadata.google.internal/computeMetadata/v1/instance/machine-type 2>/dev/null | sed 's#.*/##')\","
  echo "  \"test\": \"$TEST\", \"scale\": $FOOTPRINT_SCALE, \"workers\": $WORKERS, \"rate\": ${SCALE_RATE:-40000},"
  echo "  \"started\": \"$(date -u +%FT%TZ)\""
  echo "}"
} > "$OUT/env.json"
cat "$OUT/env.json"
$PY "$HERE/footprint/sampler.py" --out "$OUT" &
SAMPLER=$!
run() { "$PY" "$HERE/footprint/$@"; }
STATUS=0
case "$TEST" in
  backlog-*)
    P=${TEST#backlog-}
    run backlog.py cleanup --partitions "$P" >/dev/null 2>&1 || true
    run backlog.py preload --partitions "$P" || STATUS=1
    printf 'backlog\tsettle-%sp' "$P" > "$OUT/phase"; sleep 10
    run backlog.py consume --partitions "$P" || STATUS=1
    run backlog.py cleanup --partitions "$P" || true
    ;;
  restart)
    run restart.py cleanup >/dev/null 2>&1 || true
    run restart.py preload || STATUS=1
    run restart.py consume & CPID=$!
    sleep 5
    run restart.py produce & PPID_=$!
    run restart.py orchestrate || STATUS=1
    wait "$PPID_" || STATUS=1
    wait "$CPID" || STATUS=1
    run restart.py cleanup || true
    ;;
  ramp)
    run ramp.py ${RAMP_STEPS:+--steps $RAMP_STEPS} || STATUS=1
    ;;
  producer)
    run producer_outage.py cleanup >/dev/null 2>&1 || true
    PIDS=()
    for i in $(seq 0 $((WORKERS - 1))); do run producer_outage.py consume --worker "$i" & PIDS+=($!); done
    sleep 5
    run producer_outage.py produce || STATUS=1
    for p in "${PIDS[@]}"; do wait "$p"; done
    run producer_outage.py verify --workers "$WORKERS" || STATUS=1
    run producer_outage.py cleanup || true
    ;;
esac
printf 'done\tdone' > "$OUT/phase"
touch "$OUT/stop"; wait $SAMPLER
echo "status $STATUS"
exit $STATUS
