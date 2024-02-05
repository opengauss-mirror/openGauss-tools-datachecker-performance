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

package org.opengauss.datachecker.check.service;

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.config.KafkaConsumerConfig;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.kafka.KafkaException;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

import javax.annotation.PreDestroy;
import javax.annotation.Resource;
import java.time.Duration;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * kafka Topic admin
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Component
public class KafkaServiceManager {
    private static final Logger log = LogUtils.getKafkaLogger();

    private KafkaAdminClient adminClient;
    @Resource
    private KafkaTemplate<String, String> kafkaTemplate;
    @Resource
    private KafkaConsumerConfig kafkaConsumerConfig;

    public KafkaTemplate<String, String> getKafkaTemplate() {
        return kafkaTemplate;
    }

    public KafkaConsumer<String, String> getKafkaConsumer(boolean isNewGroup) {
        Consumer<String, String> consumer;
        if (isNewGroup) {
            consumer = kafkaConsumerConfig.consumerFactory(IdGenerator.nextId36())
                                          .createConsumer();
        } else {
            consumer = kafkaConsumerConfig.consumerFactory()
                                          .createConsumer();
        }
        return (KafkaConsumer<String, String>) consumer;
    }

    /**
     * Initialize Admin Client
     */
    public void initAdminClient() {
        Map<String, Object> props = new HashMap<>(1);
        props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, ConfigCache.getValue(ConfigConstants.KAFKA_SERVERS));
        adminClient = (KafkaAdminClient) KafkaAdminClient.create(props);
        try {
            adminClient.listTopics()
                       .listings()
                       .get();
            log.info("init and listTopics  admin client [{}]", ConfigCache.getValue(ConfigConstants.KAFKA_SERVERS));
        } catch (ExecutionException | InterruptedException ex) {
            log.error("kafka Client link exception: ", ex);
            throw new KafkaException("kafka Client link exception");
        }
    }

    /**
     * Create a Kafka theme. If it exists, it will not be created.
     *
     * @param topic      topic
     * @param partitions partitions
     */
    public boolean createTopic(String topic, int partitions) {
        try {
            CreateTopicsResult topicsResult =
                adminClient.createTopics(List.of(new NewTopic(topic, partitions, (short) 1)));
            topicsResult.values()
                        .get(topic)
                        .get(5, TimeUnit.SECONDS);
            log.info("create topic success , name= [{}] numPartitions = [{}]", topic, partitions);
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            log.error("create tioic error : ", e);
            return false;
        }
    }

    /**
     * Delete topic and support batch
     *
     * @param topics topic
     */
    public void deleteTopic(Collection<String> topics) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(topics);
        Map<String, KafkaFuture<Void>> kafkaFutureMap = deleteTopicsResult.topicNameValues();
        kafkaFutureMap.forEach((topic, future) -> {
            try {
                future.get(1, TimeUnit.SECONDS);
                log.info("topic={} is deleting", topic);
            } catch (InterruptedException | ExecutionException | TimeoutException e) {
                log.warn("topic={} is delete : {}", topic, e.getMessage());
            }
        });
        Set<String> topicList = getKafkaTopicList();
        AtomicBoolean isNotDeleted = new AtomicBoolean(false);
        topics.forEach(topic -> {
            if (topicList.contains(topic)) {
                log.warn("topic does not deleted kafka {}", topic);
                isNotDeleted.set(true);
            }
        });
        if (!isNotDeleted.get()) {
            log.info("topic [{}] has deleted", topics);
        }

    }

    private Set<String> getKafkaTopicList() {
        KafkaFuture<Set<String>> names = adminClient.listTopics()
                                                    .names();
        Set<String> topicList = null;
        try {
            topicList = names.get();
        } catch (InterruptedException | ExecutionException ignored) {
        }
        return topicList;
    }

    @PreDestroy
    public void closeAdminClient() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ZERO);
                log.info("kafkaAdminClient close.");
            } catch (Exception e) {
                log.error("check kafkaAdminClient close error: ", e);
            }
        }
    }
}
