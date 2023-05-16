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

import lombok.SneakyThrows;
import org.opengauss.datachecker.check.cache.CheckRateCache;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.report.CheckProgress;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicReference;
import java.util.function.UnaryOperator;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Service
@ConditionalOnBean({CheckEnvironment.class, EndpointMetaDataManager.class, TableStatusRegister.class})
public class ProgressService {
    private static final String PROCESS_LOG_NAME = "progress.log";
    private static final String PROGRESS = "progress";
    private AtomicReference<CheckProgress> progressRef = new AtomicReference<>();
    private String logFileFullPath;
    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private TableStatusRegister tableStatusRegister;
    @Resource
    private CheckRateCache checkRateCache;
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;
    private final ScheduledExecutorService executorService = ThreadUtil.newSingleThreadScheduledExecutor();

    /**
     * Schedule loading scheduled tasks
     */
    public void progressing() {
        progressRef.set(new CheckProgress());
        if (isFullMode()) {
            executorService.scheduleWithFixedDelay(() -> {
                Thread.currentThread().setName(PROGRESS);
                if (progressRef.get().getTableCount() == 0) {
                    if (endpointMetaDataManager.getCheckTaskCount() > 0) {
                        initProgress(endpointMetaDataManager.getCheckTaskCount());
                    }
                } else {
                    refreshCompleteProgress(tableStatusRegister.getCheckedCount());
                }
                if (progressRef.get().getStatus() == CheckProgressStatus.END) {
                    ThreadUtil.sleepHalfSecond();
                    executorService.shutdownNow();
                }
            }, 1, 1, TimeUnit.SECONDS);
        }
    }

    /**
     * Get the progress and return the latest progress information when the scheduled task is closed
     *
     * @param completeCount completeCount
     * @return progress
     */
    @SneakyThrows
    public CheckProgress getCheckProgress(int completeCount) {
        if (isFullMode()) {
            while (!isComplete()) {
                ThreadUtil.sleepHalfSecond();
            }
        } else {
            refreshCompleteProgress(completeCount);
        }
        return progressRef.get();
    }

    private boolean isFullMode() {
        return Objects.equals(checkEnvironment.getCheckMode(), CheckMode.FULL);
    }

    private boolean isComplete() {
        return progressRef.get().getStatus() == CheckProgressStatus.END && progressRef.get().getEndTime() != null;
    }

    /**
     * init progress
     *
     * @param tableCount tableCount
     */
    private void initProgress(int tableCount) {
        progressRef.updateAndGet(initProgressUnaryOperator(tableCount));
        createProgressLog();
    }

    private UnaryOperator<CheckProgress> initProgressUnaryOperator(int tableCount) {
        return (v) -> {
            final LocalDateTime now = LocalDateTime.now();
            return v.setTableCount(tableCount).setMode(checkEnvironment.getCheckMode()).setStartTime(now)
                    .setCurrentTime(now).setStatus(CheckProgressStatus.START);
        };
    }

    private UnaryOperator<CheckProgress> refreshProgressUnaryOperator(int completeCount) {
        final LocalDateTime now = LocalDateTime.now();
        return (process) -> {
            process.setCompleteCount(completeCount);
            process.setCost(calcCost(now));
            process.setSpeed(checkRateCache.getCurrentSecondSpeed());
            process.setAvgSpeed(checkRateCache.getAvgSpeed(process.getCost()));
            process.setTotal(checkRateCache.getTotal());
            process.setTotalRows(checkRateCache.getTotalRows());
            process.setCurrentTime(now);
            if (completeCount == process.getTableCount()) {
                process.setEndTime(now);
                process.setStatus(CheckProgressStatus.END);
            } else {
                process.setStatus(CheckProgressStatus.PROGRESS);
            }
            return process;
        };
    }

    public synchronized void refreshCompleteProgress(int completeCount) {
        progressRef.updateAndGet(refreshProgressUnaryOperator(completeCount));
        appendProgressLog();
    }

    private void appendProgressLog() {
        String content = JsonObjectUtil.formatSec(progressRef.get()) + System.lineSeparator();
        FileUtils.writeAppendFile(logFileFullPath, content);
    }

    private void createProgressLog() {
        final String exportCheckPath = checkEnvironment.getExportCheckPath();
        logFileFullPath = exportCheckPath + File.separatorChar + "result" + File.separatorChar + PROCESS_LOG_NAME;
        String content = JsonObjectUtil.formatSec(progressRef.get()) + System.lineSeparator();
        FileUtils.writeFile(logFileFullPath, content);
    }

    private long calcCost(LocalDateTime now) {
        return Duration.between(progressRef.get().getStartTime(), now).toSeconds();
    }

    public void resetProgress(int tableCount) {
        initProgress(tableCount);
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
