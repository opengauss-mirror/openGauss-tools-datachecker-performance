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

import org.apache.commons.lang3.StringUtils;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.OffsetAndMetadata;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.exception.CheckConsumerPollEmptyException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

import java.time.Duration;
import java.util.*;
import java.util.concurrent.atomic.AtomicLong;

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

    /**
     * Query the Kafka partition data corresponding to the specified table
     *
     * @param topic Kafka topic
     * @param partitions Kafka partitions
     * @return kafka partitions data
     */
    public List<RowDataHash> queryCheckRowData(String topic, int partitions) {
        return queryRowData(topic, partitions, false);
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
     * @param topicPartition topic partition
     * @param sExtend slice extend
     * @param attempts attempts
     */
    public void consumerAssign(TopicPartition topicPartition, SliceExtend sExtend, int attempts) {
        kafkaConsumer.assign(List.of(topicPartition));
        if (attempts == 0) {
            kafkaConsumer.seek(topicPartition, sExtend.getStartOffset());
        }
    }

    /**
     * consumer poll data from the topic partition, and filter bu slice extend. then add data in the data list.
     *
     * @param sExtend slice extend
     * @param dataList data list
     */
    public synchronized void pollTpSliceData(SliceExtend sExtend, List<RowDataHash> dataList) {
        AtomicLong currentCount = new AtomicLong(0);
        int pollEmptyCount = 0;
        while (currentCount.get() < sExtend.getCount()) {
            ConsumerRecords<String, String> records = kafkaConsumer.poll(
                Duration.ofMillis(KAFKA_CONSUMER_POLL_DURATION));
            if (records.count() <= 0) {
                pollEmptyCount++;
                if (pollEmptyCount > MAX_CONSUMER_POLL_TIMES) {
                    throw new CheckConsumerPollEmptyException(sExtend.getName());
                }
                ThreadUtil.sleep(KAFKA_CONSUMER_POLL_DURATION);
                continue;
            }
            pollEmptyCount = 0;
            records.forEach(record -> {
                RowDataHash row = JSON.parseObject(record.value(), RowDataHash.class);
                if (row.getSNo() == sExtend.getNo() && StringUtils.equals(record.key(), sExtend.getName())) {
                    dataList.add(row);
                    currentCount.incrementAndGet();
                }
            });
        }
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
