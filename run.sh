#!/bin/bash

# =============================================================================
# DataChecker Application Manager Script
# =============================================================================
# This script manages the lifecycle of DataChecker applications including
# source, sink, and check components.
# =============================================================================

set -e

# Script directory
SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"

# Default JVM parameters
DEFAULT_JVM_PARAMS="-Xmx1G -Xms1G -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled -XX:+HeapDumpOnOutOfMemoryError"

# Application mode arguments
ARG_SOURCE="--source"
ARG_SINK="--sink"

# Default Kafka configuration (empty means disabled)
DEFAULT_KAFKA_BOOTSTRAP_SERVERS=""
DEFAULT_KAFKA_TOPIC=""
DEFAULT_KAFKA_KEY_PREFIX="data_check"

# =============================================================================
# Usage Information
# =============================================================================
function show_usage() {
    cat << EOF
Usage: $0 COMMAND [OPTIONS]

Commands:
    start [WORKSPACE_DIR] [OPTIONS] [JVM_PARAMS]
        Start all DataChecker applications
    stop [WORKSPACE_DIR]
        Stop all DataChecker applications
    status [WORKSPACE_DIR]
        Check status of all DataChecker applications

Options:
    --kafka-bootstrap-servers <servers>
        Kafka bootstrap servers address (e.g., 127.0.0.1:9092)
    --kafka-topic <topic>
        Kafka topic for alert logs
    --kafka-key-prefix <prefix>
        Prefix for Kafka message keys (default: data_check)

JVM Parameters:
    Custom JVM parameters can be passed after all other options
    Default: $DEFAULT_JVM_PARAMS

Examples:
    # Start with default settings (Kafka disabled)
    $0 start

    # Start with custom workspace
    $0 start /path/to/workspace

    # Start with Kafka enabled
    $0 start --kafka-bootstrap-servers <kafka_bootstrap_servers> --kafka-topic <kafka_topic>

    # Start with Kafka and custom prefix
    $0 start --kafka-bootstrap-servers <kafka_bootstrap_servers> --kafka-topic <kafka_topic> --kafka-key-prefix my_check

    # Start with Kafka and custom JVM params
    $0 start --kafka-bootstrap-servers <kafka_bootstrap_servers> --kafka-topic <kafka_topic> -Xmx2G -Xms2G

    # Stop applications
    $0 stop

    # Check status
    $0 status

EOF
}

# =============================================================================
# Directory Operations
# =============================================================================

function create_directory() {
    local dir="$1"
    if [ ! -d "$dir" ]; then
        mkdir -p "$dir"
        echo "Created directory: $dir"
    fi
}

function find_jar_files() {
    local pattern="$1"
    local files=$(find "$SCRIPT_DIR" -maxdepth 1 -name "$pattern" 2>/dev/null | head -n 10)
    if [ -z "$files" ]; then
        echo "Error: No files found matching pattern: $pattern" >&2
        return 1
    fi
    echo "$files"
}

function get_pid_dir() {
    local workspace_dir="$1"
    if [ -n "$workspace_dir" ]; then
        echo "$workspace_dir/pid"
    else
        echo "$SCRIPT_DIR"
    fi
}

# =============================================================================
# Process Management
# =============================================================================

function stop_process() {
    local pid_file="$1"
    local process_name="$2"
    local ps_pattern="$3"

    echo "Stopping $process_name..."

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            kill "$pid" 2>/dev/null || true
            sleep 2
            if ps -p "$pid" > /dev/null 2>&1; then
                kill -9 "$pid" 2>/dev/null || true
            fi
            echo "Killed $process_name process with PID: $pid"
        else
            echo "$process_name process $pid is not running"
        fi
        rm -f "$pid_file"
    else
        local pids=$(ps -ef 2>/dev/null | grep "$ps_pattern" | grep -v grep | awk '{print $2}')
        if [ -n "$pids" ]; then
            for pid in $pids; do
                kill "$pid" 2>/dev/null || true
                sleep 2
                if ps -p "$pid" > /dev/null 2>&1; then
                    kill -9 "$pid" 2>/dev/null || true
                fi
                echo "Killed $process_name process with PID: $pid"
            done
        else
            echo "$process_name is not running"
        fi
    fi
}

function check_process_status() {
    local pid_file="$1"
    local process_name="$2"
    local ps_pattern="$3"

    echo "Checking $process_name..."

    if [ -f "$pid_file" ]; then
        local pid=$(cat "$pid_file")
        if ps -p "$pid" > /dev/null 2>&1; then
            echo "$process_name is running with PID: $pid"
        else
            echo "$process_name PID file exists but process is not running"
            rm -f "$pid_file"
        fi
    else
        local pids=$(ps -ef 2>/dev/null | grep "$ps_pattern" | grep -v grep | awk '{print $2}')
        if [ -n "$pids" ]; then
            echo "$process_name is running with PIDs: $pids"
        else
            echo "$process_name is not running"
        fi
    fi
}

# =============================================================================
# Application Startup
# =============================================================================

function start_process() {
    local process_name="$1"
    local jar_file="$2"
    local args="$3"
    local heap_dump_path="$4"
    local kafka_key="$5"
    local spring_config="$6"
    local logs_dir="$7"
    local config_dir="$8"
    local common_params="$9"
    local script_dir="${10}"

    echo "Starting $process_name..."

    local log_file="$logs_dir/$(echo "$process_name" | tr ' ' '-').log"

    create_directory "$(dirname "$heap_dump_path")"
    create_directory "$(dirname "$log_file")"

    local kafka_params=""
    if [ -n "$KAFKA_BOOTSTRAP_SERVERS" ] && [ -n "$KAFKA_TOPIC" ]; then
        kafka_params="-Dkafka.key=$kafka_key -Denable.alert.log.collection=true -Dkafka.bootstrapServers=$KAFKA_BOOTSTRAP_SERVERS -Dkafka.topic=$KAFKA_TOPIC"
    fi

    local java_cmd="nohup java $common_params $kafka_params -Dspring.config.additional-location=\"$spring_config\" -XX:HeapDumpPath=\"$heap_dump_path\" -jar \"$script_dir/$jar_file\" $args"

    if [ -n "$config_dir" ]; then
        eval "$java_cmd" > /dev/null 2>&1 &
    else
        eval "$java_cmd" > "$log_file" 2>&1 &
    fi

    local pid=$!
    if [ $? -ne 0 ]; then
        echo "Error: Failed to start $process_name" >&2
        return 1
    fi

    echo "$process_name started with PID: $pid"
    sleep 2
    echo "$pid"
}

function init_directories() {
    local workspace_dir="$1"
    local result_dir="$SCRIPT_DIR/check_result/result"
    local config_dir=""
    local logs_dir="$SCRIPT_DIR/logs"
    local pid_dir="$SCRIPT_DIR"
    local lib_dir="$SCRIPT_DIR/lib"

    if [ -n "$workspace_dir" ]; then
        result_dir="$workspace_dir/check_result/result"
        config_dir="$workspace_dir/config"
        logs_dir="$workspace_dir/logs"
        pid_dir="$workspace_dir/pid"
    fi

    create_directory "$result_dir"
    create_directory "$lib_dir"
    create_directory "$logs_dir"
    create_directory "$pid_dir"

    echo "$result_dir $config_dir $logs_dir $pid_dir $lib_dir"
}

function find_and_validate_jar_files() {
    local check_file=$(find_jar_files "datachecker-check-*.jar")
    if [ $? -ne 0 ] || [ -z "$check_file" ]; then
        echo "Error: No datachecker-check JAR file found" >&2
        return 1
    fi

    local app_check_file=$(basename "$check_file")

    local extract_files=$(find_jar_files "datachecker-extract-*.jar")
    if [ $? -ne 0 ] || [ -z "$extract_files" ]; then
        echo "Error: No datachecker-extract JAR file found" >&2
        return 1
    fi

    local app_extract_file=$(basename "$extract_files")

    echo "$app_check_file $app_extract_file"
}

function display_config() {
    local workspace_dir="$1"
    local result_dir="$2"
    local config_dir="$3"
    local logs_dir="$4"
    local pid_dir="$5"
    local lib_dir="$6"
    local jvm_params="$7"

    echo "=============================================="
    echo "       DataChecker Configuration"
    echo "=============================================="
    echo "  Script directory: $SCRIPT_DIR"
    echo "  Workspace: ${workspace_dir:-Default}"
    echo "  Result directory: $result_dir"
    echo "  Config directory: ${config_dir:-Default}"
    echo "  Lib directory: $lib_dir"
    echo "  Logs directory: $logs_dir"
    echo "  PID directory: $pid_dir"
    echo "  JVM parameters: $jvm_params"
    echo "=============================================="
}

function save_pids() {
    local pid_dir="$1"
    local source_pid="$2"
    local sink_pid="$3"
    local check_pid="$4"

    echo "$source_pid" > "$pid_dir/source.pid"
    echo "$sink_pid" > "$pid_dir/sink.pid"
    echo "$check_pid" > "$pid_dir/check.pid"
    echo "Process IDs saved to PID files in $pid_dir"
    echo "All datachecker applications started successfully!"
}

# =============================================================================
# Main Start Function
# =============================================================================

function start_apps() {
    local workspace_dir="${1:-}"
    shift

    parse_arguments "start" "$@"
    extract_jvm_params "$@"

    read -r result_dir config_dir logs_dir pid_dir lib_dir <<< $(init_directories "$workspace_dir")

    if [ -n "$workspace_dir" ]; then
        echo "Using workspace directory: $workspace_dir"
    fi

    display_kafka_config
    display_config "$workspace_dir" "$result_dir" "$config_dir" "$logs_dir" "$pid_dir" "$lib_dir" "$JVM_PARAMS"

    local common_params="$JVM_PARAMS -Dloader.path=\"$lib_dir\""

    local app_files=$(find_and_validate_jar_files)
    if [ $? -ne 0 ]; then
        echo "$app_files" >&2
        return 1
    fi

    read -r app_check_file app_extract_file <<< "$app_files"

    local source_kafka_key="${KAFKA_KEY_PREFIX}_source"
    local source_pid=$(start_process "datachecker-extract source instance" "$app_extract_file" "$ARG_SOURCE" \
        "$result_dir/heap_source.hprof" "$source_kafka_key" "$config_dir/datacheck/application-source.yml" \
        "$logs_dir" "$config_dir" "$common_params" "$SCRIPT_DIR")

    if [ $? -ne 0 ] || [ -z "$source_pid" ]; then
        echo "Error: Failed to start source instance" >&2
        return 1
    fi

    local sink_kafka_key="${KAFKA_KEY_PREFIX}_sink"
    local sink_pid=$(start_process "datachecker-extract sink instance" "$app_extract_file" "$ARG_SINK" \
        "$result_dir/heap_sink.hprof" "$sink_kafka_key" "$config_dir/datacheck/application-sink.yml" \
        "$logs_dir" "$config_dir" "$common_params" "$SCRIPT_DIR")

    if [ $? -ne 0 ] || [ -z "$sink_pid" ]; then
        echo "Error: Failed to start sink instance, stopping source..." >&2
        stop_process "" "datachecker-extract source" "$app_extract_file.*$ARG_SOURCE"
        return 1
    fi

    local check_kafka_key="${KAFKA_KEY_PREFIX}_check"
    local check_pid=$(start_process "datachecker-check instance" "$app_check_file" "" \
        "$result_dir/heap.hprof" "$check_kafka_key" "$config_dir/datacheck/application.yml" \
        "$logs_dir" "$config_dir" "$common_params" "$SCRIPT_DIR")

    if [ $? -ne 0 ] || [ -z "$check_pid" ]; then
        echo "Error: Failed to start check instance, stopping source and sink..." >&2
        stop_process "" "datachecker-extract source" "$app_extract_file.*$ARG_SOURCE"
        stop_process "" "datachecker-extract sink" "$app_extract_file.*$ARG_SINK"
        return 1
    fi

    save_pids "$pid_dir" "$source_pid" "$sink_pid" "$check_pid"
}

# =============================================================================
# Argument Parsing
# =============================================================================

KAFKA_BOOTSTRAP_SERVERS=""
KAFKA_TOPIC=""
KAFKA_KEY_PREFIX="$DEFAULT_KAFKA_KEY_PREFIX"
JVM_PARAMS="$DEFAULT_JVM_PARAMS"

function parse_arguments() {
    local cmd="$1"
    shift

    while [[ $# -gt 0 ]]; do
        case "$1" in
            --kafka-bootstrap-servers)
                KAFKA_BOOTSTRAP_SERVERS="$2"
                shift 2
                ;;
            --kafka-topic)
                KAFKA_TOPIC="$2"
                shift 2
                ;;
            --kafka-key-prefix)
                KAFKA_KEY_PREFIX="$2"
                shift 2
                ;;
            --help|-h)
                show_usage
                exit 0
                ;;
            *)
                break
                ;;
        esac
    done
}

function extract_jvm_params() {
    JVM_PARAMS="$DEFAULT_JVM_PARAMS"

    local jvm_args=()
    while [[ $# -gt 0 ]]; do
        case "$1" in
            --kafka-*)
                shift 2
                ;;
            *)
                jvm_args+=("$1")
                shift
                ;;
        esac
    done

    if [ ${#jvm_args[@]} -gt 0 ]; then
        JVM_PARAMS="${jvm_args[*]}"
    fi
}

function display_kafka_config() {
    echo "----------------------------------------------"
    if [ -n "$KAFKA_BOOTSTRAP_SERVERS" ] && [ -n "$KAFKA_TOPIC" ]; then
        echo "  Kafka Bootstrap Servers: $KAFKA_BOOTSTRAP_SERVERS"
        echo "  Kafka Topic: $KAFKA_TOPIC"
        echo "  Kafka Key Prefix: $KAFKA_KEY_PREFIX"
    else
        echo "  Kafka Configuration: Disabled"
    fi
    echo "----------------------------------------------"
}

# =============================================================================
# Application Control Functions
# =============================================================================

function stop_apps() {
    local workspace_dir="$1"
    local pid_dir=$(get_pid_dir "$workspace_dir")

    echo "Stopping datachecker applications..."
    if [ -n "$workspace_dir" ]; then
        echo "Using workspace directory: $workspace_dir"
        echo "Looking for PID files in: $pid_dir"
    else
        echo "Using default PID directory: $pid_dir"
    fi

    stop_process "$pid_dir/check.pid" "datachecker-check" "datachecker-check"
    stop_process "$pid_dir/source.pid" "datachecker-extract source" "datachecker-extract.*$ARG_SOURCE"
    stop_process "$pid_dir/sink.pid" "datachecker-extract sink" "datachecker-extract.*$ARG_SINK"
}

function status_apps() {
    local workspace_dir="$1"
    local pid_dir=$(get_pid_dir "$workspace_dir")

    echo "Checking datachecker application status..."
    if [ -n "$workspace_dir" ]; then
        echo "Using workspace directory: $workspace_dir"
        echo "Looking for PID files in: $pid_dir"
    else
        echo "Using default PID directory: $pid_dir"
    fi

    check_process_status "$pid_dir/check.pid" "datachecker-check" "datachecker-check"
    check_process_status "$pid_dir/source.pid" "datachecker-extract source" "datachecker-extract.*$ARG_SOURCE"
    check_process_status "$pid_dir/sink.pid" "datachecker-extract sink" "datachecker-extract.*$ARG_SINK"
}

# =============================================================================
# Main Entry Point
# =============================================================================

function main() {
    if [ $# -eq 0 ]; then
        show_usage
        exit 1
    fi

    case "$1" in
        start)
            shift
            local workspace_dir=""
            local other_args=()

            if [ -n "$1" ] && [[ ! "$1" =~ ^- ]]; then
                workspace_dir="$1"
                shift
            fi

            start_apps "$workspace_dir" "$@"
            ;;
        stop)
            stop_apps "$2"
            ;;
        status)
            status_apps "$2"
            ;;
        help|--help|-h)
            show_usage
            ;;
        *)
            echo "Error: Unknown command '$1'" >&2
            echo ""
            show_usage
            exit 1
            ;;
    esac
}

main "$@"