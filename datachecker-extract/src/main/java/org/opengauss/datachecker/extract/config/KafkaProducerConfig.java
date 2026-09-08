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

import org.apache.kafka.clients.producer.ProducerConfig;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.kafka.annotation.EnableKafka;
import org.springframework.kafka.core.DefaultKafkaProducerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.kafka.core.ProducerFactory;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Component
@EnableKafka
@EnableConfigurationProperties(KafkaProperties.class)
public class KafkaProducerConfig {
    @Autowired
    private KafkaProperties properties;

    public ProducerFactory<String, String> producerFactory() {
        return new DefaultKafkaProducerFactory<>(buildProducerConfig());
    }

    @Bean
    public KafkaTemplate<String, String> kafkaTemplate() {
        return new KafkaTemplate<>(producerFactory());
    }

    private Map<String, Object> buildProducerConfig() {
        // configuration information
        Map<String, Object> props = new HashMap<>(InitialCapacity.CAPACITY_8);
        KafkaProperties.Producer producer = properties.getProducer();
        // kafka server address
        props.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, String.join(",", properties.getBootstrapServers()));
        props.put(ProducerConfig.ACKS_CONFIG, producer.getAcks());
        // sets the serialization processing class for data keys and values.
        props.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, producer.getKeySerializer());
        props.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, producer.getValueSerializer());
        // spring.kafka.properties.* passthrough, such as linger.ms, compression.type and max.in.flight
        // empty map when not configured
        props.putAll(properties.getProperties());
        // spring.kafka.producer.batch-size / buffer-memory; Spring defaults equal kafka defaults (16KB/32MB)
        props.put(ProducerConfig.BATCH_SIZE_CONFIG, (int) producer.getBatchSize().toBytes());
        props.put(ProducerConfig.BUFFER_MEMORY_CONFIG, producer.getBufferMemory().toBytes());
        // creating a kafka producer instance
        return props;
    }
}
