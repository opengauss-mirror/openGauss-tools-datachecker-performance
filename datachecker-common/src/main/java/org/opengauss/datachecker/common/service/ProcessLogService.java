package org.opengauss.datachecker.common.service;

import com.alibaba.fastjson.JSONObject;
import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.FileUtils;
import org.springframework.stereotype.Service;

import java.lang.management.ManagementFactory;
import java.time.LocalDateTime;

/**
 * @author ：wangchao
 * @date ：Created in 2023/11/17
 * @since ：11
 */
@Service
public class ProcessLogService {
    private static final String processLog = "process.pid";
    private static final String event_start = "start";
    private static final String event_stop = "stop";

    public void saveProcessLog() {
        saveProcessLog(event_start);
    }

    public void saveStopProcessLog() {
        saveProcessLog(event_stop);
    }

    private void saveProcessLog(String event) {
        String name = ManagementFactory.getRuntimeMXBean()
                                       .getName();
        String pid = name.split("@")[0];
        ProcessLog log = new ProcessLog().setEndpoint(ConfigCache.getEndPoint())
                                         .setPid(pid)
                                         .setEvent(event)
                                         .setExecTime(LocalDateTime.now());
        String logPath = ConfigCache.getCheckResult() + processLog;
        FileUtils.writeAppendFile(logPath, JSONObject.toJSONString(log) + System.lineSeparator());
    }

    @Data
    @Accessors(chain = true)
    class ProcessLog {
        Endpoint endpoint;
        String pid;
        LocalDateTime execTime;
        String event;
    }
}
