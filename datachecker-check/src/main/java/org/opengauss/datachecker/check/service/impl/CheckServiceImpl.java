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

package org.opengauss.datachecker.check.service.impl;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.cache.TopicRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.modules.check.DataCheckService;
import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.service.CheckTableStructureService;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.check.event.KafkaTopicDeleteProvider;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.check.CheckProgress;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.CommonException;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;
import java.util.stream.IntStream;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Service(value = "checkService")
public class CheckServiceImpl implements CheckService {
    private static final Logger log = LogUtils.getLogger();
    /**
     * Verification task start flag
     * <p>
     * Whether full or incremental verification, only one can be performed at a time.
     * Only after the local full or incremental verification is completed, that is,
     * {@code started}=false, can the next one be executed.
     * Otherwise, exit directly and wait until the current verification process is completed,
     * and then exit automatically.
     * <p>
     * The method of forcibly exiting the current verification process is not provided for the time being.
     */
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean CHECKING = new AtomicBoolean(true);

    /**
     * Process signature
     */
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();
    private static final AtomicReference<CheckProgress> CHECK_PROGRESS_REFERENCE = new AtomicReference<>();

    /**
     * Verify polling thread name
     */
    private static final String SELF_CHECK_POLL_THREAD_NAME = "check-polling-thread";
    private static final String START_MESSAGE = "the execution time of %s process is %s";

    @Autowired
    private FeignClientService feignClientService;
    @Autowired
    private TableStatusRegister tableStatusRegister;
    @Resource
    private TopicRegister topicRegister;
    @Resource
    private DataCheckService dataCheckService;
    @Autowired
    private EndpointMetaDataManager endpointMetaDataManager;
    @Autowired
    private CheckTableStructureService checkTableStructureService;
    @Resource
    private CheckEnvironment checkEnvironment;
    @Value("${data.check.auto-clean-environment}")
    private boolean isAutoCleanEnvironment = true;
    @Resource
    private KafkaTopicDeleteProvider kafkaTopicDeleteProvider;

    /**
     * Enable verification service
     *
     * @param checkMode check Mode
     */
    @Override
    public String start(CheckMode checkMode) {
        Assert.isTrue(checkEnvironment.isLoadMetaSuccess(), "current meta data is loading, please wait a moment");
        log.info(CheckMessage.CHECK_SERVICE_STARTING, checkEnvironment.getCheckMode().getCode());
        Assert.isTrue(Objects.equals(CheckMode.FULL, checkEnvironment.getCheckMode()),
            "current check mode is " + CheckMode.INCREMENT.getDescription() + " , not start full check.");
        if (STARTED.compareAndSet(false, true)) {
            ExportCheckResult.backCheckResultDirectory();
            try {
                startCheckFullMode();
                // Wait for the task construction to complete, and start the task polling thread
                startCheckPollingThread();
            } catch (CheckingException ex) {
                cleanCheck();
                throw new CheckingException(ex.getMessage());
            }
        } else {
            String message = String.format(CheckMessage.CHECK_SERVICE_START_ERROR, checkMode.getDescription());
            log.error(message);
            cleanCheck();
            throw new CheckingException(message);
        }
        return String.format(START_MESSAGE, PROCESS_SIGNATURE.get(), JsonObjectUtil.formatTime(LocalDateTime.now()));
    }

    interface CheckMessage {
        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_STARTING = "check service is starting, start check mode is [{}]";

        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_START_ERROR = "check service is running, current check mode is [%s] , exit.";
    }

    /**
     * Turn on full calibration mode
     */
    private void startCheckFullMode() {
        String processNo = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        // Source endpoint task construction
        final List<ExtractTask> extractTasks = feignClientService.buildExtractTaskAllTables(Endpoint.SOURCE, processNo);
        log.info("check full mode : build extract task source {}", processNo);
        // Sink endpoint task construction
        feignClientService.buildExtractTaskAllTables(Endpoint.SINK, processNo, extractTasks);
        log.info("check full mode : build extract task sink {}", processNo);
        checkTableStructureService.check(processNo);
        // Perform all tasks
        feignClientService.execExtractTaskAllTables(Endpoint.SOURCE, processNo);
        feignClientService.execExtractTaskAllTables(Endpoint.SINK, processNo);
        log.info("check full mode : exec extract task (source and sink ) {}", processNo);
        kafkaTopicDeleteProvider.deleteTopicIfTableCheckedCompleted();
        PROCESS_SIGNATURE.set(processNo);
    }

    /**
     * Data verification polling thread
     * It is used to monitor the completion status of data extraction tasks in real time.
     * When the status of a data extraction task changes to complete, start a data verification independent thread.
     * And start the current task to verify the data.
     */
    public void startCheckPollingThread() {
        checkTableWithSyncExtracting();
    }

    private void checkTableWithExtractEnd() {
        if (tableStatusRegister.isExtractCompleted() && CHECKING.get()) {
            log.info("check polling processNo={}, extract task complete. start checking....", PROCESS_SIGNATURE.get());
            CHECKING.set(false);
            final List<String> checkTableList = endpointMetaDataManager.getCheckTableList();
            if (CollectionUtils.isEmpty(checkTableList)) {
                log.info("");
            }
            checkTableList.forEach(this::startCheckTableThread);
        }
    }

    private void checkTableWithSyncExtracting() {

        if (!tableStatusRegister.isCheckCompleted()) {
            String tableName = tableStatusRegister.completedTablePoll();
            if (StringUtils.isNotEmpty(tableName)) {
                log.info("start checking thread of table {}", tableName);
                startCheckTableThread(tableName);
            }
        }
    }

    private void startCheckTableThread(String tableName) {
        final TableMetadata tableMetadata = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, tableName);
        if (Objects.isNull(tableMetadata)) {
            log.error("can not find table={} meta data, checking skipped", tableName);
            return;
        }
        Topic topic = topicRegister.getTopic(tableName);
        if (Objects.isNull(topic)) {
            log.error("table={} does not register its topic info, checking skipped", tableName);
            return;
        }
        String process = getCurrentCheckProcess();
        tableStatusRegister.initPartitionsStatus(tableName, topic.getPtnNum());
        IntStream.range(0, topic.getPtnNum()).forEach(idxPartition -> {
            // Verify the data according to the table name and Kafka partition
            dataCheckService.checkTableData(process, tableName, idxPartition, topic.getPtnNum());
        });
        kafkaTopicDeleteProvider.addTableToDropTopic(tableName);
    }

    private void completeProgressBar(ScheduledExecutorService scheduledExecutor) {
        CheckProgress process = CHECK_PROGRESS_REFERENCE.get();
        final CheckProgress newProcess = tableStatusRegister.extractProgress();
        if (!Objects.equals(process, newProcess)) {
            CHECK_PROGRESS_REFERENCE.set(newProcess);
            log.info("The verification status :{}", CHECK_PROGRESS_REFERENCE.get());
        }
        // The current task completes the verification and resets the task status
        if (tableStatusRegister.isCheckCompleted()) {
            log.info("The verification is completed, reset status :{}", tableStatusRegister.get());
            log.debug("The verification is completed, reset check partitions status: {}",
                tableStatusRegister.getTablePartitionsStatusCache());
            if (isAutoCleanEnvironment) {
                log.info("completes the verification and resets the check environment");
                cleanCheck();
                feignClientService.cleanTask(Endpoint.SOURCE);
                feignClientService.cleanTask(Endpoint.SINK);
            }
            scheduledExecutor.shutdownNow();
        }
    }

    /**
     * Query the currently executed process number
     *
     * @return process number
     */
    @Override
    public String getCurrentCheckProcess() {
        return PROCESS_SIGNATURE.get();
    }

    /**
     * Clean up the verification environment
     */
    @Override
    public synchronized void cleanCheck() {
        final String processNo = PROCESS_SIGNATURE.get();
        if (Objects.nonNull(processNo)) {
            cleanBuildTask(processNo);
        }
        ThreadUtil.sleep(3000);
        PROCESS_SIGNATURE.set(null);
        STARTED.set(false);
        CHECKING.set(true);
        log.info("clear and reset the current verification service!");
    }

    private void cleanBuildTask(String processNo) {
        try {
            feignClientService.cleanEnvironment(Endpoint.SOURCE, processNo);
            feignClientService.cleanEnvironment(Endpoint.SINK, processNo);
        } catch (CommonException ex) {
            log.error("ignore error:", ex);
        }
        tableStatusRegister.removeAll();
        log.info("The task registry of the verification service clears the data extraction task status information");
    }
}
