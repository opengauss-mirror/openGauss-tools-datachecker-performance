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

package org.opengauss.datachecker.check.modules.check;

import com.alibaba.fastjson.JSON;

import cn.hutool.core.thread.ThreadUtil;

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.exception.CheckConsumerPollEmptyException;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.util.LogUtils;

import java.time.Duration;
import java.util.*;

/**
 * KafkaConsumerHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/31
 * @since ：11
 */
public class KafkaConsumerHandler {
    private static final Logger log = LogUtils.getLogger(KafkaConsumerHandler.class);
    private static final int KAFKA_CONSUMER_POLL_DURATION = 20;
    private static final int MAX_CONSUMER_POLL_TIMES = 50;
    private static final int MAX_CONSUMER_BACKOFF = 1000;

    private final Object dataListLock = new Object();
    private KafkaConsumer<String, String> kafkaConsumer;

    /**
     * Constructor
     *
     * @param consumer consumer
     * @param retryTimes retryTimes
     */
    public KafkaConsumerHandler(KafkaConsumer<String, String> consumer, int retryTimes) {
        kafkaConsumer = consumer;
    }

    /**
     * Constructor
     *
     * @param consumer consumer
     */
    public KafkaConsumerHandler(KafkaConsumer<String, String> consumer) {
        kafkaConsumer = consumer;
    }

    /**
     * 获取kafka consumer
     *
     * @return consumer
     */
    public KafkaConsumer<String, String> getConsumer() {
        return kafkaConsumer;
    }

    public void poolTopicPartitionsData(String topic, int partitions, List<RowDataHash> list) {
        final TopicPartition topicPartition = new TopicPartition(topic, partitions);
        kafkaConsumer.assign(List.of(topicPartition));
        long endOfOffset = getEndOfOffset(topicPartition);
        long beginOfOffset = beginningOffsets(topicPartition);
        consumerTopicRecords(list, kafkaConsumer, endOfOffset);
        LogUtils.debug(log, "consumer topic=[{}] partitions=[{}] dataList=[{}] ,beginOfOffset={},endOfOffset={}", topic,
            partitions, list.size(), beginOfOffset, endOfOffset);
    }

    /**
     * consumer poll data from the topic partition, and filter bu slice extend. then add data in the data list.
     *
     * @param topicPartition topicPartition
     * @param sExtend slice extend
     * @param dataList data list
     */
    public void pollTpSliceData(TopicPartition topicPartition, SliceExtend sExtend, List<RowDataHash> dataList) {
        long targetCount = sExtend.getCount();
        long currentCount = 0L;
        int pollEmptyCount = 0;
        int pollSuccessCount = 0;
        kafkaConsumer.assign(List.of(topicPartition));
        kafkaConsumer.seek(topicPartition, sExtend.getStartOffset());
        long startTime = System.currentTimeMillis();
        log.debug("Start pulling data slice: {}, target quantity: {} , topic start offset {}", sExtend.getName(),
            targetCount, sExtend.getStartOffset());
        while (currentCount < targetCount) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(
                Duration.ofMillis(KAFKA_CONSUMER_POLL_DURATION));
            if (records.isEmpty()) {
                pollEmptyCount++;
                if (pollEmptyCount > calculateMaxPollEmptyTimes()) {
                    long costTime = System.currentTimeMillis() - startTime;
                    log.error(
                        "Data pulling failed - slice: {}, pulled: {}/{}, time consumed: {}ms, empty poll count: {}",
                        sExtend.getName(), currentCount, targetCount, costTime, pollEmptyCount);
                    throw new CheckConsumerPollEmptyException(String.format(Locale.getDefault(),
                        "Failed to pull data for slice %s: obtained %d/%d records, %d consecutive empty polls",
                        sExtend.getName(), currentCount, targetCount, pollEmptyCount));
                }
                long sleepTime = calculateBackoffTime(pollEmptyCount);
                ThreadUtil.sleep(sleepTime);
                continue;
            }
            pollEmptyCount = 0;
            pollSuccessCount += records.count();
            int batchProcessed = processRecordsBatch(records, sExtend, dataList);
            currentCount += batchProcessed;
            if (pollSuccessCount > 0 && (pollSuccessCount % 100 == 0 || currentCount >= targetCount)) {
                commitOffsetsAndLogProgress(sExtend, pollSuccessCount, currentCount, targetCount, startTime);
            }
        }
        long totalTime = System.currentTimeMillis() - startTime;
        log.info("Data pulling completed - slice: {}, count: {}, time consumed: {}ms", sExtend.getName(),
                currentCount, totalTime);
    }

    private int processRecordsBatch(ConsumerRecords<String, String> records, SliceExtend sExtend,
        List<RowDataHash> dataList) {
        int processedCount = 0;
        List<RowDataHash> batchList = new ArrayList<>(records.count());
        for (ConsumerRecord<String, String> record : records) {
            try {
                RowDataHash row = JSON.parseObject(record.value(), RowDataHash.class);
                if (isRecordMatch(row, record.key(), sExtend)) {
                    batchList.add(row);
                    processedCount++;
                }
            } catch (CheckingException e) {
                log.warn("Record parsing failed, skipping - topic: {}, offset: {}", record.topic(), record.offset(), e);
            }
        }
        if (!batchList.isEmpty()) {
            synchronized (dataListLock) {
                dataList.addAll(batchList);
            }
        }
        return processedCount;
    }

    private void commitOffsetsAndLogProgress(SliceExtend sExtend, long pollSuccessCount, long currentCount,
        long targetCount, long startTime) {
        try {
            kafkaConsumer.commitAsync();
            long elapsed = System.currentTimeMillis() - startTime;
            long remaining = targetCount - currentCount;
            log.debug(
                "Pull progress - slice: {}, progress: fetched records {}, matched records: {}/{}, remaining: {}, time"
                    + " consumed: {}ms", sExtend.getName(), pollSuccessCount, currentCount, targetCount, remaining,
                elapsed);
        } catch (CheckingException e) {
            log.warn("slice {} Offset commit failed", sExtend.getName(), e);
        }
    }

    private boolean isRecordMatch(RowDataHash row, String recordKey, SliceExtend sExtend) {
        return row.getSNo() == sExtend.getNo() && StringUtils.equals(recordKey, sExtend.getName());
    }

    private int calculateMaxPollEmptyTimes() {
        return Math.max(MAX_CONSUMER_POLL_TIMES, 20);
    }

    private long calculateBackoffTime(int emptyCount) {
        long baseTime = KAFKA_CONSUMER_POLL_DURATION;
        long backoffTime = baseTime * (long) Math.pow(2, Math.min(emptyCount, 8));
        return Math.min(backoffTime, MAX_CONSUMER_BACKOFF);
    }

    /**
     * Query the Kafka partition data corresponding to the specified table
     *
     * @param topic Kafka topic
     * @param partitions Kafka partitions
     * @param shouldChangeConsumerGroup if true change consumer Group random
     * @return kafka partitions data
     */
    public List<RowDataHash> queryRowData(String topic, int partitions, boolean shouldChangeConsumerGroup) {
        List<RowDataHash> data = new LinkedList<>();
        final TopicPartition topicPartition = new TopicPartition(topic, partitions);
        kafkaConsumer.assign(List.of(topicPartition));
        long endOfOffset = getEndOfOffset(topicPartition);
        long beginOfOffset = beginningOffsets(topicPartition);
        if (shouldChangeConsumerGroup) {
            resetOffsetToBeginning(kafkaConsumer, topicPartition);
        }
        consumerTopicRecords(data, kafkaConsumer, endOfOffset);
        log.debug("consumer topic=[{}] partitions=[{}] dataList=[{}] ,beginOfOffset={},endOfOffset={}", topic,
            partitions, data.size(), beginOfOffset, endOfOffset);
        return data;
    }

    private long getEndOfOffset(TopicPartition topicPartition) {
        final Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.endOffsets(List.of(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    private long beginningOffsets(TopicPartition topicPartition) {
        final Map<TopicPartition, Long> topicPartitionLongMap = kafkaConsumer.beginningOffsets(List.of(topicPartition));
        return topicPartitionLongMap.get(topicPartition);
    }

    private void resetOffsetToBeginning(KafkaConsumer<String, String> consumer, TopicPartition topicPartition) {
        Map<TopicPartition, OffsetAndMetadata> offset = new HashMap<>();
        consumer.seekToBeginning(List.of(topicPartition));
        long position = consumer.position(topicPartition);
        offset.put(topicPartition, new OffsetAndMetadata(position));
        consumer.commitSync(offset);
    }

    private void consumerTopicRecords(List<RowDataHash> data, KafkaConsumer<String, String> kafkaConsumer,
        long endOfOffset) {
        if (endOfOffset == 0) {
            return;
        }
        while (endOfOffset > data.size()) {
            getTopicRecords(data, kafkaConsumer);
        }
    }

    private void getTopicRecords(List<RowDataHash> dataList, KafkaConsumer<String, String> kafkaConsumer) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(
            Duration.ofMillis(KAFKA_CONSUMER_POLL_DURATION));
        consumerRecords.forEach(record -> {
            dataList.add(JSON.parseObject(record.value(), RowDataHash.class));
        });
    }

    /**
     * close consumer
     */
    public void closeConsumer() {
        if (kafkaConsumer != null) {
            kafkaConsumer.close();
            kafkaConsumer = null;
        }
    }
}
