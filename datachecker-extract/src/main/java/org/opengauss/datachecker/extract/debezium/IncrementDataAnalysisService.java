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

package org.opengauss.datachecker.extract.debezium;

import feign.FeignException;
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.exception.ExtractException;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.util.List;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.atomic.AtomicLong;

/**
 * IncrementDataAnalysisService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/4
 * @since ：11
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class IncrementDataAnalysisService {
    /**
     * Single thread scheduled task - execute check polling thread
     */
    private static ScheduledExecutorService SCHEDULED_EXECUTOR = null;
    private static final int DEBEZIUM_TIME_PERIOD_UNIT = 60000;

    private final ExtractProperties extractProperties;
    private final DataConsolidationService consolidationService;
    private final CheckingFeignClient checkingFeignClient;

    /**
     * Used to record the offset of the last consumption of the incremental verification topic data,
     * which is the starting point of the next data consumption
     */
    private volatile AtomicLong lastOffSetAtomic = new AtomicLong(0L);

    /**
     * It is used to record the last execution time of the incremental verification topic data,
     * which is the starting point of the execution cycle of the next data consumption task
     */
    private volatile Long lastTimestamp = System.currentTimeMillis();
    @Resource
    private ShutdownService shutdownService;
    /**
     * Start the initialization load to verify the topic offset
     */
    public void startIncrDataAnalysis() {
        if (extractProperties.isDebeziumEnable() && consolidationService.isSourceEndpoint()) {
            SCHEDULED_EXECUTOR = ThreadUtil.newSingleThreadScheduledExecutor();
            shutdownService.addExecutorService(SCHEDULED_EXECUTOR);
            log.info("Start incremental verification analysis");
            verificationConfiguration();
            // Start the initialization load to verify the topic offset
            setLastTimestampAtomicCurrentTime();
            dataAnalysis();
        }
    }

    private void verificationConfiguration() {
        log.info("Incremental verification configuration parameter check");
        final int debeziumTimePeriod = extractProperties.getDebeziumTimePeriod();
        final int debeziumNumPeriod = extractProperties.getDebeziumNumPeriod();
        final int defaultPeriod = extractProperties.getDebeziumNumDefaultPeriod();
        Assert.isTrue(debeziumTimePeriod > 0,
            "Debezium incremental migration verification, the time period should be greater than 0");
        Assert.isTrue(debeziumNumPeriod >= defaultPeriod, "Debezium incremental migration verification statistics:"
            + "the value of the number of incremental change records should be greater than " + defaultPeriod);
    }

    /**
     * Incremental log data record extraction scheduling task
     */
    private void dataAnalysis() {
        log.info("Start the incremental verification data analysis thread");
        SCHEDULED_EXECUTOR
            .scheduleWithFixedDelay(peekDebeziumTopicRecordOffset(), DataNumAnalysisThreadConstant.INITIAL_DELAY,
                DataNumAnalysisThreadConstant.DELAY, TimeUnit.SECONDS);
    }

    /**
     * peekDebeziumTopicRecordOffset
     *
     * @return Incremental log data record extraction scheduling task thread
     */
    private Runnable peekDebeziumTopicRecordOffset() {
        return () -> {
            Thread.currentThread().setName(DataNumAnalysisThreadConstant.NAME);
            try {
                checkingFeignClient.health();
                dataNumAnalysis();
                dataTimeAnalysis();
            } catch (FeignException ex) {
                log.error("check service has an error occurred. {}", ex.getMessage());
            } catch (ExtractException ex) {
                log.error("peek debezium topic record offset has an error occurred,", ex);
            } catch (Exception ex) {
                log.error("unkown error occurred,", ex);
            }
        };
    }

    /**
     * Incremental log data extraction and time latitude management
     */
    private void dataTimeAnalysis() {
        long time = System.currentTimeMillis();
        final int debeziumTimePeriod = extractProperties.getDebeziumTimePeriod();
        if ((time - lastTimestamp) >= debeziumTimePeriod * DEBEZIUM_TIME_PERIOD_UNIT) {
            // Set the start calculation time point of the next time execution cycle
            lastTimestamp = time;
            final int defaultSize = extractProperties.getDebeziumNumDefaultPeriod();
            final List<SourceDataLog> debeziumTopicRecords = consolidationService.getDebeziumTopicRecords(defaultSize);
            if (CollectionUtils.isNotEmpty(debeziumTopicRecords)) {
                log.info("Incremental log data , time latitude debeziumTopicRecords={}", debeziumTopicRecords.size());
                checkingFeignClient.notifySourceIncrementDataLogs(debeziumTopicRecords);
                lastOffSetAtomic.addAndGet(debeziumTopicRecords.size());
            }
        }

    }

    /**
     * Incremental log data extraction, quantity and latitude management
     */
    private void dataNumAnalysis() {
        final int offset = consolidationService.getDebeziumTopicRecordEndOffSet();
        // Verify whether the data volume threshold dimension scenario trigger conditions are met
        if ((offset - lastOffSetAtomic.get()) >= extractProperties.getDebeziumNumPeriod()) {
            // When the data volume threshold is reached,
            // the data is extracted and pushed to the verification service.
            final List<SourceDataLog> debeziumTopicRecords = consolidationService.getDebeziumTopicRecords(offset);
            if (CollectionUtils.isNotEmpty(debeziumTopicRecords)) {
                log.info("Incremental log data, quantity latitude :start={},end={},count={}", lastOffSetAtomic.get(),
                    offset, debeziumTopicRecords.size());
                checkingFeignClient.notifySourceIncrementDataLogs(debeziumTopicRecords);
                lastOffSetAtomic.addAndGet(debeziumTopicRecords.size());
            }
            // Trigger data volume threshold dimension scenario - update time threshold
            setLastTimestampAtomicCurrentTime();
        }
    }

    private void setLastTimestampAtomicCurrentTime() {
        lastTimestamp = System.currentTimeMillis();
    }

    interface DataNumAnalysisThreadConstant {
        /**
         * Data analysis thread pool thread name
         */
        String NAME = "DataAnalysisThread";

        /**
         * Data analysis thread pool initialization delay time
         */
        long INITIAL_DELAY = 0L;

        /**
         * Data analysis thread pool latency
         */
        long DELAY = 10L;
    }
}
