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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.KafkaFuture;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.kafka.KafkaException;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Properties;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * KafkaService
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/12
 * @since ：11
 */
@Slf4j
public class KafkaService {
    private static final int FLUSH_KAFKA_PARALLEL_THRESHOLD = 10000;

    private AdminClient adminClient;
    private KafkaProperties properties;

    public KafkaService(KafkaProperties properties) {
        this.properties = properties;
    }

    /**
     * init Admin Client
     */
    public void initAdminClient() {
        Map<String, Object> props = new HashMap<>(1);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, properties.getBootstrapServers());
        adminClient = KafkaAdminClient.create(props);
        try {
            adminClient.listTopics().listings().get();
        } catch (ExecutionException | InterruptedException ex) {
            log.error("kafkaClient init exception,{}", ex.getMessage());
            throw new KafkaException("kafkaClient init exception");
        }
    }

    /**
     * delete topic
     *
     * @param topicName topicName
     */
    public void deleteTopic(String topicName) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(List.of(topicName));
        Map<String, KafkaFuture<Void>> kafkaFutureMap = deleteTopicsResult.topicNameValues();
        kafkaFutureMap.forEach((topic, future) -> {
            try {
                future.get();
                log.info("topic={} is delete successfull", topic);
            } catch (InterruptedException | ExecutionException e) {
                log.error("topic={} is delete error: {}", topic, e);
            }
        });
    }

    /**
     * send message to kafka
     *
     * @param kafkaProducer  kafkaProducer
     * @param producerRecord producerRecord
     * @param cnt            if cnt >= {@value FLUSH_KAFKA_PARALLEL_THRESHOLD } force flush
     */
    public void sendMessage(KafkaProducer<String, String> kafkaProducer, ProducerRecord<String, String> producerRecord,
        AtomicInteger cnt) {
        kafkaProducer.send(producerRecord, (metadata, exception) -> {
            if (exception != null) {
                log.error("send failed,topic={},key:{} ,partition:{},offset:{}", metadata.topic(), producerRecord.key(),
                    metadata.partition(), metadata.offset(), exception);
            }
        });
        if (cnt.addAndGet(1) >= FLUSH_KAFKA_PARALLEL_THRESHOLD) {
            kafkaProducer.flush();
            log.info("sendMessage flush");
            cnt.set(0);
        }
    }

    /**
     * build KafkaProducer
     *
     * @return KafkaProducer
     */
    public KafkaProducer<String, String> buildKafkaProducer() {
        Properties props = new Properties();
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG,
            properties.getBootstrapServers().stream().collect(Collectors.joining(",")));
        props.put(ProducerConfig.ACKS_CONFIG, properties.getProducer().getAcks());
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, properties.getProducer().getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, properties.getProducer().getValueSerializer());
        return new KafkaProducer<>(props);
    }
}
