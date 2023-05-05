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
import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.TopicPartition;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.springframework.stereotype.Component;

import java.time.Duration;
import java.util.LinkedList;
import java.util.List;

/**
 * KafkaConsumerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@Slf4j
@Component
@RequiredArgsConstructor
public class KafkaConsumerService {
    private final KafkaConsumerConfig consumerConfig;

    /**
     * Get the data of the specified topic partition
     *
     * @param tableName  tableName
     * @param partitions partitions
     * @return kafka topic data
     */
    public List<RowDataHash> getTopicRecords(String tableName, int partitions) {
        Topic topic = TopicCache.getTopic(tableName);
        KafkaConsumer<String, String> kafkaConsumer = consumerConfig.getKafkaConsumer(topic.getTopicName(), partitions);
        kafkaConsumer.assign(List.of(new TopicPartition(topic.getTopicName(), partitions)));
        List<RowDataHash> dataList = new LinkedList<>();
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(200));
        consumerRecords.forEach(record -> {
            dataList.add(JSON.parseObject(record.value(), RowDataHash.class));
        });
        log.debug("kafka consumer topic=[{}] partitions=[{}] dataList=[{}]", topic.toString(), partitions,
            dataList.size());
        return dataList;
    }
}
