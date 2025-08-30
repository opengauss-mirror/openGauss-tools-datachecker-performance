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

import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.CreateTopicsResult;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.NewTopic;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.kafka.KafkaException;
import org.springframework.stereotype.Component;

import jakarta.annotation.PreDestroy;
import java.time.Duration;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.TimeUnit;
import java.util.concurrent.TimeoutException;
import java.util.stream.Collectors;

/**
 * kafka Topic admin
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/17
 * @since ：11
 */
@Component
public class KafkaAdminService {
    private static final Logger log = LogUtils.getLogger(KafkaAdminService.class);

    private KafkaAdminClient adminClient;

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
            LogUtils.info(log, "init and listTopics  admin client [{}]",
                ConfigCache.getValue(ConfigConstants.KAFKA_SERVERS));
        } catch (ExecutionException | InterruptedException ex) {
            LogUtils.error(log, "kafka Client link exception: ", ex);
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
            LogUtils.info(log, "create topic success , name= [{}] numPartitions = [{}]", topic, partitions);
            return true;
        } catch (InterruptedException | ExecutionException | TimeoutException e) {
            LogUtils.error(log, "create tioic error : ", e);
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
                future.get();
                LogUtils.info(log, "topic={} is delete successfull", topic);
            } catch (InterruptedException | ExecutionException e) {
                LogUtils.error(log, "topic={} is delete error : {}", topic, e);
            }
        });
    }

    /**
     * Gets the topic with the specified prefix
     *
     * @param prefix prefix
     * @return Topic with the specified prefix
     */
    public List<String> getAllTopic(String prefix) {
        try {
            LogUtils.info(log, "get topic from kafka list topics and  prefix [{}]", prefix);
            return adminClient.listTopics()
                              .listings()
                              .get()
                              .stream()
                              .map(TopicListing::name)
                              .filter(name -> name.startsWith(prefix))
                              .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.error(log, "admin client get topic error:", e);
        }
        return new ArrayList<>();
    }

    /**
     * Gets all of the topics
     *
     * @return topics
     */
    public List<String> getAllTopic() {
        try {
            LogUtils.info(log, "get topic from kafka list topics");
            return adminClient.listTopics()
                              .listings()
                              .get()
                              .stream()
                              .map(TopicListing::name)
                              .collect(Collectors.toList());
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.error(log, "admin client get topic error:", e);
        }
        return new ArrayList<>();
    }

    /**
     * Check whether the current topic exists
     *
     * @param topicName topic Name
     * @return Does it exist
     */
    public boolean isTopicExists(String topicName) {
        try {
            LogUtils.debug(log, "check topic [{}] has exists --> check kafka list topics", topicName);
            return adminClient.listTopics()
                              .listings()
                              .get()
                              .stream()
                              .map(TopicListing::name)
                              .anyMatch(name -> name.equalsIgnoreCase(topicName));
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.error(log, "admin client get topic error:", e);
        }
        return false;
    }

    @PreDestroy
    public void closeAdminClient() {
        if (adminClient != null) {
            try {
                adminClient.close(Duration.ZERO);
                LogUtils.info(log, "kafkaAdminClient close.");
            } catch (Exception e) {
                LogUtils.error(log, "check kafkaAdminClient close error: ", e);
            }
        }
    }
}
