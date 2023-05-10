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

import lombok.RequiredArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.extract.config.KafkaConsumerConfig;
import org.springframework.stereotype.Service;

import java.util.List;

/**
 * KafkaManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/10
 * @since ：11
 */
@Slf4j
@RequiredArgsConstructor
@Service
public class KafkaManagerService {
    private final KafkaAdminService kafkaAdminService;
    private final KafkaConsumerConfig kafkaConsumerConfig;

    /**
     * Clear Kafka information
     *
     * @param processNo processNo
     */
    public void cleanKafka(String processNo) {
        log.info("Extract service cleanup Kafka topic mapping information");
        kafkaConsumerConfig.cleanKafkaConsumer();
        log.info("Extract service to clean up Kafka consumer information");
        List<String> topics = kafkaAdminService.getAllTopic(processNo);
        kafkaAdminService.deleteTopic(topics);
        log.info("Extract service cleanup current process ({}) Kafka topics {}", processNo, topics);
        kafkaAdminService.deleteTopic(topics);
        log.info("Extract service cleanup current process ({}) Kafka topics {}", processNo, topics);
    }

    /**
     * Clean up all topics with prefix 前缀TOPIC_EXTRACT_Endpoint_process_ in Kafka
     *
     * @param processNo process
     */
    public void deleteTopic(String processNo) {
        List<String> topics = kafkaAdminService.getAllTopic(processNo);
        kafkaAdminService.deleteTopic(topics);
    }

    /**
     * Clean up all topics
     */
    public void deleteTopic() {
        List<String> topics = kafkaAdminService.getAllTopic();
        kafkaAdminService.deleteTopic(topics);
    }

    /**
     * Delete the specified topic
     *
     * @param topicName topicName
     */
    public void deleteTopicByName(String topicName) {
        kafkaAdminService.deleteTopic(List.of(topicName));
    }
}
