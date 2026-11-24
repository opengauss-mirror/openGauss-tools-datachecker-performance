/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.check.Pair;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import java.time.Duration;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;

/**
 * ExtractKafkaDataService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/26
 * @since ：11
 */
@Slf4j
@Service
public class ExtractKafkaDataService {
    @Autowired
    private KafkaProperties properties;

    /**
     * checkKafkaTopicData
     *
     * @param topicSource SOURCE(1, "SourceEndpoint")
     * @param topicSink   SINK(2, "SinkEndpoint")
     * @return topic
     */
    public List<String> checkKafkaTopicData(String topicSource, String topicSink) {
        Map<String, RowDataHash> source = new HashMap<>(InitialCapacity.CAPACITY_1);
        List<Pair<RowDataHash, RowDataHash>> sourceRepeatList = new ArrayList<>();
        int sourceCount = getTopicRecords(topicSource, source, sourceRepeatList);
        Map<String, RowDataHash> sink = new HashMap<>(InitialCapacity.CAPACITY_64);
        List<Pair<RowDataHash, RowDataHash>> sinkRepeatList = new ArrayList<>();
        int sinkCount = getTopicRecords(topicSink, sink, sinkRepeatList);
        if (sourceCount == sinkCount && sinkCount > 0) {
            List<String> primaryList = new ArrayList<>(source.keySet());
            primaryList.forEach(primary -> {
                if (source.containsKey(primary) && sink.containsKey(primary)) {
                    RowDataHash sourceRow = source.get(primary);
                    RowDataHash sinkRow = sink.get(primary);
                    if (sourceRow.getPrimaryKeyHash() == sinkRow.getPrimaryKeyHash()) {
                        source.remove(sourceRow.getPrimaryKey());
                        sink.remove(sinkRow.getPrimaryKey());
                    }
                }
            });
            log.info("source={}", sourceCount);
            log.info("sink={}", sinkCount);
            log.info("sourceRepeatList={}", sourceRepeatList);
            log.info("sinkRepeatList={}", sinkRepeatList);
            log.info("source={}", source);
            log.info("sink={}", sink);
            return List.of("source=" + source.size(), "sink=" + sink.size());
        } else {
            return List.of("The source and destination query data are inconsistent,source=" + sourceCount + " sink="
                + sinkCount);
        }
    }

    private int getTopicRecords(String topic, Map<String, RowDataHash> dataMap,
        List<Pair<RowDataHash, RowDataHash>> repeatList) {
        KafkaConsumer<String, String> kafkaConsumer = buildKafkaConsumer(IdGenerator.nextId36());
        kafkaConsumer.subscribe(List.of(topic));
        int consumerRecordCount = consumerAllRecords(kafkaConsumer, dataMap, repeatList);
        kafkaConsumer.close();
        return consumerRecordCount;
    }

    private int consumerAllRecords(KafkaConsumer<String, String> kafkaConsumer, Map<String, RowDataHash> dataMap,
        List<Pair<RowDataHash, RowDataHash>> repeatList) {
        int consumerRecordCount = 0;
        int consumerRecords = getConsumerRecords(kafkaConsumer, dataMap, repeatList);
        consumerRecordCount = consumerRecordCount + consumerRecords;
        while (consumerRecords > 0) {
            consumerRecords = getConsumerRecords(kafkaConsumer, dataMap, repeatList);
            consumerRecordCount = consumerRecordCount + consumerRecords;
        }
        return consumerRecordCount;
    }

    private int getConsumerRecords(KafkaConsumer<String, String> kafkaConsumer, Map<String, RowDataHash> dataMap,
        List<Pair<RowDataHash, RowDataHash>> repeatList) {
        ConsumerRecords<String, String> consumerRecords = kafkaConsumer.poll(Duration.ofMillis(200));
        consumerRecords.forEach(record -> {
            RowDataHash rowDataHash = JSON.parseObject(record.value(), RowDataHash.class);
            if (dataMap.containsKey(rowDataHash.getPrimaryKey())) {
                repeatList.add(Pair.of(dataMap.get(rowDataHash.getPrimaryKey()), rowDataHash));
            } else {
                dataMap.put(rowDataHash.getPrimaryKey(), rowDataHash);
            }
        });
        return consumerRecords.count();
    }

    private KafkaConsumer<String, String> buildKafkaConsumer(String groupId) {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, groupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }
}
