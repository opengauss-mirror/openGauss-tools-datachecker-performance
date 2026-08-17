#!/bin/bash
# 统一启停脚本：管理 extract 和 check 两个jar服务
# Usage: sh run.sh [start|stop|restart|status]

# ====================== 配置区 ======================
EXTRACT_JAR=datachecker-extract-7.0.0-RC3.jar
CHECK_JAR=datachecker-check-7.0.0-RC3.jar
CONFIG_PATH=config
run_path=$(cd `dirname $0`; pwd)
JAVA_OPTS="-Xmx1G -Xms1G -XX:+UseG1GC -XX:MaxGCPauseMillis=100 -XX:+ParallelRefProcEnabled"
# ====================================================

# 使用帮助
usage() {
    echo "Usage: sh $0 [start|stop|restart|status]"
    echo "功能说明："
    echo "  start   启动 extract(source+sink) + check 全部服务"
    echo "  stop    停止所有正在运行的服务进程"
    echo "  restart 重启全部服务"
    echo "  status  查看所有服务运行状态"
    exit 1
}

# 查询指定jar的所有pid
get_pids() {
    local jar_name=$1
    ps -ef | grep "${run_path}" | grep "${jar_name}" | grep -v grep | awk '{print $2}'
}

# 查看全部服务状态
status_all() {
    echo "==================== Service Status ===================="
    # extract状态
    extract_pids=$(get_pids ${EXTRACT_JAR})
    if [ -z "${extract_pids}" ]; then
        echo "[${EXTRACT_JAR}] NOT running"
    else
        echo "[${EXTRACT_JAR}] running, pids: ${extract_pids}"
    fi

    # check状态
    check_pids=$(get_pids ${CHECK_JAR})
    if [ -z "${check_pids}" ]; then
        echo "[${CHECK_JAR}] NOT running"
    else
        echo "[${CHECK_JAR}] running, pids: ${check_pids}"
    fi
    echo "========================================================"
}

# 启动全部服务
start_all() {
    echo ">>> Starting ${EXTRACT_JAR} source & sink ..."
    extract_pids=$(get_pids ${EXTRACT_JAR})
    if [ -n "${extract_pids}" ]; then
        echo "${EXTRACT_JAR} already running, skip start"
    else
        # 启动source
        nohup java ${JAVA_OPTS} -jar ${run_path}/${EXTRACT_JAR} --source >/dev/null 2>&1 &
        # 启动sink
        nohup java ${JAVA_OPTS} -jar ${run_path}/${EXTRACT_JAR} --sink >/dev/null 2>&1 &
        sleep 1
        echo "${EXTRACT_JAR} source and sink start success"
    fi

    echo -e "\n>>> Starting ${CHECK_JAR} ..."
    check_pids=$(get_pids ${CHECK_JAR})
    if [ -n "${check_pids}" ]; then
        echo "${CHECK_JAR} already running, skip start"
    else
        nohup java ${JAVA_OPTS} -jar ${run_path}/${CHECK_JAR} >/dev/null 2>&1 &
        sleep 1
        echo "${CHECK_JAR} start success"
    fi

    echo -e "\n>>> All services start finished, execute status to check"
}

# 停止全部服务
stop_all() {
    echo ">>> Stopping all running service processes ..."

    # 停止extract所有进程
    extract_pids=$(get_pids ${EXTRACT_JAR})
    if [ -n "${extract_pids}" ]; then
        echo "kill ${EXTRACT_JAR} pids: ${extract_pids}"
        kill -15 ${extract_pids}
        sleep 1
    else
        echo "${EXTRACT_JAR} is not running"
    fi

    # 停止check进程
    check_pids=$(get_pids ${CHECK_JAR})
    if [ -n "${check_pids}" ]; then
        echo "kill ${CHECK_JAR} pids: ${check_pids}"
        kill -15 ${check_pids}
        sleep 1
    else
        echo "${CHECK_JAR} is not running"
    fi

    echo ">>> Stop all finished"
}

# 重启全部服务
restart_all() {
    echo "===== Restart all services ====="
    stop_all
    echo -e "\nWait 2s before start..."
    sleep 2
    start_all
    echo "===== Restart complete ====="
}

# 入口分发
case "$1" in
    "start")
        start_all
        ;;
    "stop")
        stop_all
        ;;
    "status")
        status_all
        ;;
    "restart")
        restart_all
        ;;
    *)
        usage
        ;;
esac