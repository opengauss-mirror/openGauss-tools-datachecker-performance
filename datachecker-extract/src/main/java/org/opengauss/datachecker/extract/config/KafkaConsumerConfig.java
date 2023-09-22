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

package org.opengauss.datachecker.extract.config;

import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.serialization.StringDeserializer;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.opengauss.datachecker.extract.debezium.DeserializerAdapter;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Properties;
import java.util.concurrent.ConcurrentHashMap;

/**
 * KafkaConsumerConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Component
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaConsumerConfig {
    private static final Logger log = LogUtils.getLogger();
    private static final Object LOCK = new Object();
    private static final Map<String, KafkaConsumer<String, String>> CONSUMER_MAP = new ConcurrentHashMap<>();

    @Value("${spring.extract.debezium-groupId}")
    private String debeziumGroupId;
    @Value("${spring.extract.debezium-topic}")
    private String debeziumTopic;
    @Resource
    private ExtractProperties extractProperties;
    @Resource
    private KafkaProperties properties;
    private DeserializerAdapter adapter = new DeserializerAdapter();

    /**
     * Obtaining a specified consumer client based on topic.
     *
     * @param topic      topic name
     * @param partitions total number of partitions
     * @return the topic corresponds to the consumer client.
     */
    public KafkaConsumer<String, String> getKafkaConsumer(String topic, int partitions) {
        String consumerKey = topic + "_" + partitions;
        KafkaConsumer<String, String> consumer = CONSUMER_MAP.get(consumerKey);
        if (Objects.isNull(consumer)) {
            synchronized (LOCK) {
                consumer = CONSUMER_MAP.get(consumerKey);
                if (Objects.isNull(consumer)) {
                    consumer = buildKafkaConsumer();
                    CONSUMER_MAP.put(consumerKey, consumer);
                }
            }
        }
        return consumer;
    }

    /**
     * Obtaining a specified consumer client based on topic.
     *
     * @return consumer client.
     */
    public KafkaConsumer<String, Object> getDebeziumConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(ExtConstants.DELIMITER, properties.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, debeziumGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        adapterValueDeserializer(props, extractProperties);
        adapterAvroRegistry(props, extractProperties);
        final KafkaConsumer<String, Object> consumer = new KafkaConsumer<>(props);
        consumer.subscribe(List.of(debeziumTopic));
        return consumer;
    }

    public KafkaConsumer<String, String> createConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(ExtConstants.DELIMITER, properties.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, debeziumGroupId);
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        adapterValueDeserializer(props, extractProperties);
        adapterAvroRegistry(props, extractProperties);
        final KafkaConsumer<String, String> consumer = new KafkaConsumer<>(props);
        return consumer;
    }
    private void adapterValueDeserializer(Properties props, ExtractProperties extractProperties) {
        final Class deserializer = adapter.getDeserializer(extractProperties.getDebeziumSerializer());
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, deserializer);
    }

    private void adapterAvroRegistry(Properties props, ExtractProperties extractProperties) {
        if (adapter.isAvro(extractProperties.getDebeziumSerializer())) {
            props.put(adapter.getAvroSchemaRegistryUrlKey(), extractProperties.getDebeziumAvroRegistry());
        }
    }

    private KafkaConsumer<String, String> buildKafkaConsumer() {
        Properties props = new Properties();
        props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG,
            String.join(ExtConstants.DELIMITER, properties.getBootstrapServers()));
        props.put(ConsumerConfig.GROUP_ID_CONFIG, properties.getConsumer().getGroupId());
        props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, properties.getConsumer().getAutoOffsetReset());
        props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
        return new KafkaConsumer<>(props);
    }

    /**
     * clear KafkaConsumer
     */
    public void cleanKafkaConsumer() {
        CONSUMER_MAP.clear();
        log.info("clear KafkaConsumer");
    }
}
