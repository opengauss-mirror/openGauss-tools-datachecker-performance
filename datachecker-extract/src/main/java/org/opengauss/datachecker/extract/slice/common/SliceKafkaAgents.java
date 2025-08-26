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

package org.opengauss.datachecker.extract.slice.common;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.TopicPartition;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.exception.SendTopicMessageException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.support.SendResult;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.CompletableFuture;

/**
 * SliceKafkaAgents
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class SliceKafkaAgents {
    private static final Logger logger = LogUtils.getLogger(SliceKafkaAgents.class);
    private KafkaTemplate<String, String> kafkaTemplate;
    private KafkaConsumer<String, String> kafkaConsumer;
    private String topicName;
    private String recordKey;
    private int ptn;

    /**
     * construct slice kafka agents
     *
     * @param kafkaTemplate kafka template
     * @param kafkaConsumer kafka consumer
     * @param topicName     topic
     * @param ptn           topic partition
     */
    public SliceKafkaAgents(KafkaTemplate<String, String> kafkaTemplate, KafkaConsumer<String, String> kafkaConsumer,
        String topicName, int ptn) {
        this.ptn = ptn;
        this.topicName = topicName;
        this.kafkaTemplate = kafkaTemplate;
        this.kafkaConsumer = kafkaConsumer;
        this.kafkaConsumer.subscribe(List.of(topicName));
    }

    public void setRecordSendKey(String key) {
        this.recordKey = key;
    }

    /**
     * check topic partition end offset
     *
     * @return offset
     */
    public long checkTopicPartitionEndOffset() {
        TopicPartition topicPartition = new TopicPartition(topicName, ptn);
        Map<TopicPartition, Long> endOffsets = kafkaConsumer.endOffsets(List.of(topicPartition));
        return endOffsets.get(topicPartition);
    }

    public void agentsClosed() {
        if (Objects.nonNull(kafkaConsumer)) {
            String groupId = kafkaConsumer.groupMetadata()
                                          .groupId();
            kafkaConsumer.unsubscribe();
            kafkaConsumer.close();
            LogUtils.warn(logger, "close consumer of {}:{}", topicName, groupId);
        }
    }

    /**
     * send row data hash object to topic
     *
     * @param dataHash data
     */
    public void sendRowData(RowDataHash dataHash) {
        send(dataHash.getKey(), JSON.toJSONString(dataHash));
    }

    private void send(String key, int ptn, String message) {
        try {
            kafkaTemplate.send(new ProducerRecord<>(topicName, ptn, key, message));
        } catch (Exception kafkaException) {
            LogUtils.error(logger, "send kafka [{} , {}] record error {}", topicName, key, kafkaException);
            throw new SendTopicMessageException(
                "send [" + topicName + " , " + key + "] record " + kafkaException.getMessage());
        }
    }

    private void send(String key, String message) {
        send(key, ptn, message);
    }

    /**
     * 发送表校验数据到kafka topic中
     *
     * @param dataHash dataHash
     * @return 返回发送信息
     */
    public CompletableFuture<SendResult<String, String>> sendRowDataSync(RowDataHash dataHash) {
        return sendSync(recordKey, JSON.toJSONString(dataHash));
    }

    private CompletableFuture<SendResult<String, String>> sendSync(String key, String message) {
        try {
            return kafkaTemplate.send(new ProducerRecord<>(topicName, ptn, key, message));
        } catch (Exception kafkaException) {
            LogUtils.error(logger, "send kafka [{} , {}] record error {}", topicName, key, kafkaException);
            throw new SendTopicMessageException(
                "send [" + topicName + " , " + key + "] record " + kafkaException.getMessage());
        }
    }

    /**
     * kafka send flush
     */
    public void flush() {
        kafkaTemplate.flush();
    }
}
