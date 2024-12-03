#!/bin/bash

run_path=$(cd `dirname $0`; pwd)

function parse_app_info() {
    local file_name="$1"
    if [[ $file_name =~ ^(datachecker-check|datachecker-extract)-([0-9]+\.[0-9]+\.[0-9]+(\.[a-zA-Z0-9]+)?)\.jar$ ]]; then
        local app_name="${BASH_REMATCH[1]}"
        local app_version="${BASH_REMATCH[2]}"
        echo "$app_name"
        echo "$app_version"
    else
        echo "Invalid file name format: $file_name"
        exit 1
    fi
}

check_file=$(find $run_path -maxdepth 1 -name "datachecker-check-*.jar" | head -n 1)
if [ -n "$check_file" ]; then
    IFS='-' read -ra parts <<< "$(basename $check_file)"
    app_check_name=$(basename $check_file)
    app_check_version="${parts[-1]%.jar}"
else
    echo "No datachecker-check application file found."
    exit 1
fi

extract_files=$(find $run_path -maxdepth 1 -name "datachecker-extract-*.jar")
if [ ${#extract_files} -gt 0 ]; then
    app_extract_names=()
    app_extract_versions=()
    for extract_file in $extract_files; do
        IFS='-' read -ra parts <<< "$(basename $extract_file)"
        app_extract_names+=("$(basename $extract_file)")
        app_extract_versions+=("${parts[-1]%.jar}")
    done
else
    echo "No datachecker-extract application file found."
    exit 1
fi

extract_source="--source"
extract_sink="--sink"

function start_apps() {
    echo "Starting datachecker-check application..."
    sleep 1s
    nohup java -jar $run_path/$app_check_name > /dev/null 2>&1 &
    echo "datachecker-check started with PID: $!"
    sleep 2s
    echo "Starting datachecker-extract applications..."
    nohup java -jar $run_path/${app_extract_names[0]} $extract_source > /dev/null 2>&1 &
    echo "datachecker-extract instance $extract_source started with PID: $!"
     sleep 2s
    nohup java -jar $run_path/${app_extract_names[0]} $extract_sink > /dev/null 2>&1 &
    echo "datachecker-extract instance $extract_sink started with PID: $!"
    sleep 2s
}

function stop_apps() {
    echo "Stopping datachecker-check application..."
    pids=$(ps -ef | grep "$run_path/$app_check_name" | grep -v grep | awk '{print $2}')
    if [ -n "$pids" ]; then
        for pid in $pids; do
            kill $pid
            sleep 2s
            echo "Killed datachecker-check process with PID: $pid"
        done
    else
        echo "datachecker-check application is not running."
    fi

    echo "Stopping datachecker-extract applications..."
    for ((i = 0; i < 2; i++)); do
        extract_name=${app_extract_names[$i]}
        pids=$(ps -ef | grep "$run_path/$extract_name" | grep -v grep | awk '{print $2}')
        if [ -n "$pids" ]; then
            for pid in $pids; do
                kill $pid
                sleep 2s
                echo "Killed datachecker-extract instance $((i + 1)) process with PID: $pid"
            done
        else
            echo "datachecker-extract instance $((i + 1)) is not running."
        fi
    done
}

case "$1" in
    "start")
        start_apps
        ;;
    "stop")
        stop_apps
        ;;
    *)
        echo "Usage: $0 {start|stop}"
        exit 1
        ;;
esac