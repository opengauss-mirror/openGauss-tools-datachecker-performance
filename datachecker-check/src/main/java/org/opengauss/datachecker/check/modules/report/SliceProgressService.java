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

package org.opengauss.datachecker.check.modules.report;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.report.CheckProgress;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

import static org.opengauss.datachecker.check.modules.report.SliceProgressService.CheckProgressStatus.END;
import static org.opengauss.datachecker.check.modules.report.SliceProgressService.CheckProgressStatus.PROGRESS;
import static org.opengauss.datachecker.check.modules.report.SliceProgressService.CheckProgressStatus.START;

/**
 * SliceProgressService
 *
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Service
public class SliceProgressService {
    private static final Logger log = LogUtils.getBusinessLogger();
    private static final Map<String, Set<Integer>> TABLE_SLICE = new ConcurrentHashMap<>();
    private static final Map<String, Long> TABLE_ROW_COUNT = new ConcurrentHashMap<>();
    private static final String PROCESS_LOG_NAME = "progress.log";
    private final Lock lock = new ReentrantLock();

    private static int completedTableCount = 0;
    private static int totalTable = 0;

    private final CheckProgress checkProgress = new CheckProgress();
    private String logFileFullPath;

    /**
     * start progressing
     */
    public void startProgressing() {
        lock.lock();
        try {
            checkProgress.setMode(ConfigCache.getCheckMode())
                         .setStatus(START)
                         .setStartTime(LocalDateTime.now());
            checkProgress.setCurrentTime(checkProgress.getStartTime());
            createProgressLog();
            ConfigCache.put(ConfigConstants.START_LOCAL_TIME, checkProgress.getStartTime());
        } catch (Exception exception) {
            log.error("start progressing error ", exception);
        } finally {
            lock.unlock();
        }
    }

    /**
     * update slice progress
     *
     * @param table  table name
     * @param sTotal table slice count
     * @param sNo    table slice no
     * @param count  slice row count
     */
    public void updateProgress(String table, int sTotal, int sNo, long count) {
        lock.lock();
        try {
            updateTableSliceProgress(table, sNo);
            updateCompletedTableProgress(table, sTotal);
            updateTableRowCountProgress(table, count);
            refreshCheckProgress(count);
            refreshProgressLog();
        } catch (Exception exception) {
            log.error("start progressing error ", exception);
        } finally {
            lock.unlock();
        }
    }

    public CheckProgress getCheckProgress() {
        return checkProgress;
    }

    private void refreshCheckProgress(long count) {
        checkProgress.setTableCount(totalTable)
                     .setCompleteCount(completedTableCount)
                     .setRows(count)
                     .setCurrentTime(LocalDateTime.now())
                     .setTotal(checkProgress.getTotal() + count);
        long cost = Duration.between(checkProgress.getStartTime(), checkProgress.getCurrentTime())
                            .toSeconds();
        checkProgress.setCost(cost);
        checkProgress.setAvgSpeed((int) (checkProgress.getTotal() / (cost == 0 ? 1 : cost)));
        if (completedTableCount == totalTable) {
            checkProgress.setEndTime(checkProgress.getCurrentTime());
            checkProgress.setStatus(END);
        } else {
            checkProgress.setStatus(PROGRESS);
        }
    }

    private void updateTableRowCountProgress(String table, long count) {
        TABLE_ROW_COUNT.compute(table, (key, value) -> value == null ? count : value + count);
    }

    public void updateTotalTableCount(int totalTableCount) {
        lock.lock();
        try {
            totalTable = totalTableCount;
        } catch (Exception exception) {
            log.error("start progressing error ", exception);
        } finally {
            lock.unlock();
        }
    }

    private void updateCompletedTableProgress(String table, int sTotal) {
        if (TABLE_SLICE.get(table)
                       .size() == sTotal) {
            completedTableCount++;
        }
    }

    private void updateTableSliceProgress(String table, int sNo) {
        TABLE_SLICE.compute(table, (key, value) -> {
            if (value == null) {
                value = new HashSet<>();
            }
            value.add(sNo);
            return value;
        });
    }

    private void refreshProgressLog() {
        String content = JsonObjectUtil.formatSec(checkProgress) + System.lineSeparator();
        FileUtils.writeAppendFile(logFileFullPath, content);
    }

    private void createProgressLog() {
        logFileFullPath = ConfigCache.getCheckResult() + PROCESS_LOG_NAME;
        String content = JsonObjectUtil.formatSec(checkProgress) + System.lineSeparator();
        FileUtils.writeFile(logFileFullPath, content);
    }

    /**
     * Verify progress status constant
     */
    interface CheckProgressStatus {
        short START = 1;
        short PROGRESS = 2;
        short END = 3;
    }
}
