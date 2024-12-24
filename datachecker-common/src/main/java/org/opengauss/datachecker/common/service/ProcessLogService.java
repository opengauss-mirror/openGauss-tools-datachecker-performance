/*
 * Copyright (c) 2022-2022 Huawei Technologies Co.,Ltd.
 *
 * openGauss is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *
 *           http://license.coscl.org.cn/MulanPSL2
 *
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.common.service;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.annotation.JSONType;

import lombok.Data;
import lombok.experimental.Accessors;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;
import java.util.HashMap;
import java.util.Map;

/**
 * ProcessLogService
 *
 * @author ：wangchao
 * @date ：Created in 2023/11/17
 * @since ：11
 */
@Service
public class ProcessLogService {
    private static final String check_exec_history = "exec_check_history.log";
    private static final String source_history = "exec_source_history.log";
    private static final String sink_history = "exec_sink_history.log";
    private static final String processLog = "process.pid";
    private static final String event_start = "start";
    private static final String event_stop = "stop";
    private static final Logger log = LogUtils.getLogger();
    private String logPath = null;
    private String logRootPath = null;
    private static final Map<Endpoint, String> execHistoryLogs = new HashMap<>();

    static {
        execHistoryLogs.put(Endpoint.CHECK, check_exec_history);
        execHistoryLogs.put(Endpoint.SOURCE, source_history);
        execHistoryLogs.put(Endpoint.SINK, sink_history);
    }

    public void saveProcessLog() {
        saveProcessLog(event_start);

    }

    public void saveProcessHistoryLogging(String tableName, int order) {
        ProcessingLog processingLog = new ProcessingLog().setEndpoint(ConfigCache.getEndPoint())
            .setTable(tableName)
            .setOrder(order)
            .setFinishedTime(LocalDateTime.now());
        if (logRootPath == null) {
            logRootPath = ConfigCache.getCheckResult();
        }
        String historyPath = logRootPath + execHistoryLogs.get(processingLog.endpoint);
        FileUtils.writeAppendFile(historyPath, JSONObject.toJSONString(processingLog) + System.lineSeparator());
    }

    public void saveStopProcessLog() {
        saveProcessLog(event_stop);
    }

    private void saveProcessLog(String event) {
        try {
            String name = ManagementFactory.getRuntimeMXBean().getName();
            String pid = name.split("@")[0];
            ProcessLog logProcess = new ProcessLog().setEndpoint(ConfigCache.getEndPoint())
                .setPid(pid)
                .setEvent(event)
                .setExecTime(LocalDateTime.now());
            if (logPath == null) {
                logPath = ConfigCache.getCheckResult() + processLog;
            }
            FileUtils.writeAppendFile(logPath, JSONObject.toJSONString(logProcess) + System.lineSeparator());
        } catch (Exception ex) {
            log.error("{}save process log error: {} ", ErrorCode.PROCESS_LOG, ex.getMessage());
        }
    }

    @Data
    @Accessors(chain = true)
    @JSONType(orders = {"endpoint", "event", "pid", "execTime"})
    class ProcessLog {
        Endpoint endpoint;
        String pid;
        LocalDateTime execTime;
        String event;
    }

    @Data
    @Accessors(chain = true)
    @JSONType(orders = {"endpoint", "table", "order", "finishedTime"})
    class ProcessingLog {
        Endpoint endpoint;
        String table;
        int order;
        LocalDateTime finishedTime;
    }
}
