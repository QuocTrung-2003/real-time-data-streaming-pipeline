#!/usr/bin/env bash

set -e

BASE_DIR="$(cd "$(dirname "${BASH_SOURCE[0]}")/../infrastructure" && pwd)"
cd "$BASE_DIR"

# -------- UI Helpers --------
log() {
    printf "\n\033[1;34m>>> %s\033[0m\n" "$1"
}

ok() {
    printf "\033[0;32m✔ %s\033[0m\n" "$1"
}

warn() {
    printf "\033[1;33m! %s\033[0m\n" "$1"
}

fail() {
    printf "\033[0;31m✖ %s\033[0m\n" "$1"
    exit 1
}

# -------- Core Steps --------
cleanup_state() {
    log "Resetting streaming state"
    rm -rf ./tmp/checkpoints/* || true
    ok "Checkpoint directory cleared"
}

shutdown_stack() {
    log "Tearing down existing containers"
    docker compose down --remove-orphans
    ok "Old services stopped"
}

boot_infra() {
    log "Booting core infrastructure (Kafka KRaft mode + monitoring)"

    docker compose up -d \
        kafka-broker \
        kafka-exporter \
        postgres \
        prometheus \
        grafana \
        metrics-consumer

    warn "Waiting for Kafka to stabilize (~10s)"
    sleep 10
    ok "Infrastructure is up"
}

boot_services() {
    log "Launching ingestion & processing layer"

    docker compose up -d --build ingestion_service processing_service

    ok "Streaming services started"
}

boot_api() {
    log "Starting API layer"

    docker compose up -d --build api_service

    ok "API service is live"
}

# -------- Optional Log Viewer --------
follow_logs() {
    echo ""
    echo "Select logs to follow:"
    echo "  [A] Ingestion"
    echo "  [B] Processing"
    echo "  [C] API"
    echo "  [D] All"
    echo "  [Q] Quit"

    read -r -p "Your choice: " opt

    case "$opt" in
        A|a)
            docker compose logs -f ingestion_service
            ;;
        B|b)
            docker compose logs -f processing_service
            ;;
        C|c)
            docker compose logs -f api_service
            ;;
        D|d)
            docker compose logs -f ingestion_service processing_service api_service
            ;;
        Q|q)
            echo "Leaving services running in background."
            ;;
        *)
            warn "Unknown option. No logs attached."
            ;;
    esac
}

# -------- Entry Point --------
main() {
    echo "======================================"
    echo "   CLIMAPULSE DATA PLATFORM LAUNCHER  "
    echo "======================================"

    cleanup_state
    shutdown_stack
    boot_infra
    boot_services
    boot_api

    echo ""
    echo "System ready:"
    echo "  • API        → http://localhost:8000"
    echo "  • Grafana    → http://localhost:3000"
    echo "  • Prometheus → http://localhost:9090"

    follow_logs
}

main "$@"

#nano ~/.docker/config.json
#"credsStore": "desktop.exe"
#{}