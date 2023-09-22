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
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;

import javax.annotation.PreDestroy;
import java.time.Duration;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;

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
    private static final AtomicBoolean RUNNING = new AtomicBoolean(false);
    private static final String NAME = "DebeziumWorker";
    private DebeziumConsumerListener debeziumConsumerListener;
    private KafkaConsumerConfig kafkaConsumerConfig;
    private KafkaConsumer<String, Object> consumer = null;

    public DebeziumWorker(DebeziumConsumerListener debeziumConsumerListener, KafkaConsumerConfig kafkaConsumerConfig) {
        this.debeziumConsumerListener = debeziumConsumerListener;
        this.kafkaConsumerConfig = kafkaConsumerConfig;
    }

    @Override
    public void run() {
        Thread.currentThread().setName(NAME);
        log.info("The Debezium message listener task has started");
        consumer = kafkaConsumerConfig.getDebeziumConsumer();
        while (RUNNING.get()) {
            if (Objects.equals(PAUSE_OR_RESUME.get(), WorkerSwitch.RESUME)) {
                ConsumerRecords<String, Object> records = consumer.poll(Duration.ofMillis(50));
                if (records.count() > 0) {
                    log.info("consumer record count={}", records.count());
                }
                for (ConsumerRecord<String, Object> record : records) {
                    try {
                        debeziumConsumerListener.listen(record);
                    } catch (Exception ex) {
                        log.error("DebeziumWorker unknown error, message,{},{}", record.toString(), ex);
                    }
                }
            } else {
                log.debug("Debezium message listener is paused");
                ThreadUtil.sleep(WorkerSwitch.SLEEP_TIME);
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
