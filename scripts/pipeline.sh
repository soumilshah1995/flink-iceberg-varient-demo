#!/usr/bin/env bash
# Flink SQL pipeline: Kafka orders_variant -> Iceberg demo.orders_variant
#
# Usage:
#   ./scripts/pipeline.sh register   # Iceberg namespace + Kafka topic
#   ./scripts/pipeline.sh start      # submit streaming job (fresh table)
#   ./scripts/pipeline.sh start --keep   # submit job, keep existing Iceberg table
#   ./scripts/pipeline.sh pause      # cancel job, keep table + Kafka offsets
#   ./scripts/pipeline.sh stop       # cancel job + drop Iceberg table
#   ./scripts/pipeline.sh run        # register + start + produce sample data
set -euo pipefail

ROOT="$(cd "$(dirname "$0")/.." && pwd)"
JOB_NAME="kafka-iceberg-variant-sql"
REST="${ICEBERG_REST_URI:-http://localhost:8181}"
KAFKA_TOPIC="orders_variant"
PIPELINE_SQL="$ROOT/sql/kafka-iceberg-variant/pipeline.sql"

usage() {
  cat <<EOF
Usage: $0 {register|start|pause|stop|run} [options]

  register          Create Iceberg demo namespace and Kafka topic
  start [--keep]    Submit Flink SQL pipeline (--keep = do not drop Iceberg table)
  pause             Cancel running job; keep Iceberg table and Kafka offsets
  stop              Cancel job and drop Iceberg table demo.orders_variant
  run               register + start + produce 20 sample Kafka messages

Examples:
  docker compose up -d --build
  ./scripts/pipeline.sh run
  python3 simple_fanout_test.py
EOF
}

ensure_stack() {
  if ! docker ps --format '{{.Names}}' | grep -qx jobmanager; then
    echo "Start the stack first: docker compose up -d --build"
    exit 1
  fi
}

run_sql() {
  local file="$1"
  docker cp "$file" jobmanager:/tmp/variant-sql.sql
  docker exec jobmanager /opt/flink/bin/sql-client.sh embedded -f /tmp/variant-sql.sql
}

find_job_id() {
  docker exec jobmanager /opt/flink/bin/flink list 2>/dev/null \
    | grep "$JOB_NAME" \
    | sed -E 's/.* : ([a-f0-9]+) :.*/\1/' \
    | head -1
}

cancel_job() {
  local job_id
  job_id="$(find_job_id || true)"
  if [[ -n "${job_id:-}" ]]; then
    echo "Cancelling job $job_id ($JOB_NAME)..."
    docker exec jobmanager /opt/flink/bin/flink cancel "$job_id"
  else
    echo "No running job matching '$JOB_NAME'."
  fi
}

drop_table() {
  echo "Dropping Iceberg table demo.orders_variant (if any)..."
  curl -sf -X DELETE "$REST/v1/namespaces/demo/tables/orders_variant" \
    >/dev/null || true
}

register() {
  ensure_stack
  echo "Registering Iceberg namespace demo at $REST ..."
  curl -sf -X POST "$REST/v1/namespaces" \
    -H 'Content-Type: application/json' \
    -d '{"namespace": ["demo"]}' \
    && echo "Namespace demo created." \
    || echo "Namespace demo may already exist (continuing)."

  echo "Creating Kafka topic: $KAFKA_TOPIC"
  docker exec broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 \
    --create \
    --if-not-exists \
    --topic "$KAFKA_TOPIC" \
    --partitions 1 \
    --replication-factor 1

  docker exec broker /opt/kafka/bin/kafka-topics.sh \
    --bootstrap-server broker:9092 \
    --list | grep -x "$KAFKA_TOPIC" || true
  echo "Register done."
}

start_pipeline() {
  local keep_table="${1:-false}"
  ensure_stack
  cancel_job || true

  if [[ "$keep_table" == "true" ]]; then
    echo "Keeping existing Iceberg table (resume from Kafka offsets)."
  else
    drop_table
  fi

  echo "Submitting Flink SQL pipeline..."
  run_sql "$PIPELINE_SQL"

  if ! docker exec jobmanager /opt/flink/bin/flink list 2>/dev/null | grep -q "$JOB_NAME"; then
    echo "ERROR: Flink SQL job did not start. Check output above for [ERROR] lines."
    exit 1
  fi
  echo "Pipeline started ($JOB_NAME)."
}

pause_pipeline() {
  ensure_stack
  cancel_job
  echo "Paused: job cancelled; Iceberg table and Kafka offsets kept."
  echo "Resume with: ./scripts/pipeline.sh start --keep"
}

stop_pipeline() {
  ensure_stack
  cancel_job || true
  drop_table
  echo "Stopped: job cancelled and Iceberg table dropped."
}

run_all() {
  register
  start_pipeline false
  echo "Producing sample data..."
  docker exec python python /workspace/producer.py
  echo
  echo "Done. Query with Flink SQL client or: python3 simple_fanout_test.py"
}

cmd="${1:-}"
shift || true

case "$cmd" in
  register) register ;;
  start)
    keep="false"
    if [[ "${1:-}" == "--keep" ]]; then
      keep="true"
    fi
    start_pipeline "$keep"
    ;;
  pause) pause_pipeline ;;
  stop) stop_pipeline ;;
  run) run_all ;;
  -h|--help|help|"") usage ;;
  *)
    echo "Unknown command: $cmd"
    usage
    exit 1
    ;;
esac
