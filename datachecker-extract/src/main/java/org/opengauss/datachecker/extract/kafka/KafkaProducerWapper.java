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

package org.opengauss.datachecker.extract.kafka;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.kafka.core.KafkaTemplate;

import java.util.List;

/**
 * KafkaProducerWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
public class KafkaProducerWapper {
    private static final int DEFAULT_PARTITION = 0;
    private static final int EMPTY = 0;
    private static final int MIN_PARTITION_NUM = 1;

    private KafkaTemplate<String, String> kafkaTemplate;

    /**
     * KafkaProducerWapper build
     *
     * @param kafkaTemplate kafkaTemplate
     */
    public KafkaProducerWapper(KafkaTemplate<String, String> kafkaTemplate) {
        this.kafkaTemplate = kafkaTemplate;
    }

    /**
     * Push the data to the topic corresponding to the specified table in batch
     *
     * @param topic          topic
     * @param recordHashList data
     */
    public int syncSend(Topic topic, List<RowDataHash> recordHashList) {
        if (CollectionUtils.isEmpty(recordHashList)) {
            return EMPTY;
        }
        final int partitions = topic.getPartitions();
        if (partitions <= MIN_PARTITION_NUM) {
            sendRecordToSinglePartitionTopic(recordHashList, topic.getTopicName());
        } else {
            sendMultiPartitionTopic(recordHashList, topic.getTopicName(), partitions);
        }
        return recordHashList.size();
    }

    private void sendRecordToSinglePartitionTopic(List<RowDataHash> recordHashList, String topicName) {
        recordHashList.forEach(record -> {
            record.setPartition(DEFAULT_PARTITION);
            final ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, DEFAULT_PARTITION, record.getPrimaryKey(), JSON.toJSONString(record));
            kafkaTemplate.send(producerRecord);
        });
        kafkaTemplate.flush();
        log.debug("send topic={}, record size :{}", topicName, recordHashList.size());
    }

    private void sendMultiPartitionTopic(List<RowDataHash> recordHashList, String topicName, int partitions) {
        recordHashList.forEach(record -> {
            int partition = calcSimplePartition(record.getPrimaryKeyHash(), partitions);
            record.setPartition(partition);
            ProducerRecord<String, String> producerRecord =
                new ProducerRecord<>(topicName, partition, record.getPrimaryKey(), JSON.toJSONString(record));
            kafkaTemplate.send(producerRecord);
        });
        kafkaTemplate.flush();
    }

    private int calcSimplePartition(long value, int mod) {
        return (int) Math.abs(value % mod);
    }
}
