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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.WorkerSwitch;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * DebeziumWorker
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
public class DebeziumWorker implements Runnable {
    private static final Logger log = LogUtils.getLogger();
    private static final AtomicBoolean PAUSE_OR_RESUME = new AtomicBoolean(WorkerSwitch.RESUME);
    private static final AtomicBoolean RUNNING = new AtomicBoolean(true);
    private static final AtomicInteger POLL_BATCH_COUNT = new AtomicInteger();
    private static final AtomicInteger RETRY_POLL_EMPTY = new AtomicInteger();
    private static final int DEFAULT_MAX_BATCH_SIZE = 1000;
    private static final int RETRY_TIMES = 3;
    private static final String NAME = "DebeziumWorker";

    private int maxBatchCount;
    private DebeziumConsumerListener debeziumConsumerListener;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private KafkaConsumer<String, Object> consumer = null;

    /**
     * DebeziumWorker
     *
     * @param debeziumConsumerListener debeziumConsumerListener
     * @param kafkaConsumerConfig      kafkaConsumerConfig
     */
    public DebeziumWorker(DebeziumConsumerListener debeziumConsumerListener, KafkaConsumerConfig kafkaConsumerConfig) {
        this.debeziumConsumerListener = debeziumConsumerListener;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
        int maxBatchSize = debeziumConsumerListener.getMaxBatchSize();
        this.maxBatchCount = maxBatchSize <= 0 ? DEFAULT_MAX_BATCH_SIZE : maxBatchSize;
    }

    @Override
    public void run() {
        Thread.currentThread()
                .setName(NAME);
        log.info("The Debezium message listener task has started");
        consumer = kafkaConsumerConfig.getDebeziumConsumer();
        while (RUNNING.get()) {
            if (Objects.equals(PAUSE_OR_RESUME.get(), WorkerSwitch.RESUME)) {
                log.debug("Debezium message listener is resume");
                doConsumerRecord(consumer.poll(Duration.ofMillis(50)));
            } else {
                ThreadUtil.sleep(WorkerSwitch.SLEEP_TIME);
            }
        }
        log.info("The Debezium message listener task has closed");
    }

    private void doConsumerRecord(ConsumerRecords<String, Object> records) {
        if (records.count() > 0) {
            log.info("consumer record count={}", records.count());
            for (ConsumerRecord<String, Object> record : records) {
                try {
                    debeziumConsumerListener.listen(record);
                } catch (Exception ex) {
                    log.error("{}DebeziumWorker unknown error, message,{},{}", ErrorCode.DEBEZIUM_WORKER, record.toString(), ex);
                }
            }
            POLL_BATCH_COUNT.addAndGet(records.count());
            if (POLL_BATCH_COUNT.get() > maxBatchCount) {
                PAUSE_OR_RESUME.set(WorkerSwitch.PAUSE);
                POLL_BATCH_COUNT.set(0);
            }
            RETRY_POLL_EMPTY.set(0);
        } else {
            log.info("consumer record count=0 retry {}", RETRY_POLL_EMPTY.get());
            if (RETRY_POLL_EMPTY.incrementAndGet() > RETRY_TIMES) {
                RETRY_POLL_EMPTY.set(0);
                PAUSE_OR_RESUME.set(WorkerSwitch.PAUSE);
            }
        }
    }

    public void close() {
        RUNNING.set(false);
    }

    @PreDestroy
    public void preDestroy() {
        consumer.close();
    }

    /**
     * pause or resume the worker thread
     *
     * @param pauseOrResume pauseOrResume
     */
    public void switchPauseOrResume(Boolean pauseOrResume) {
        PAUSE_OR_RESUME.set(pauseOrResume);
    }
}
