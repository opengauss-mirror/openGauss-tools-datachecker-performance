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

package org.opengauss.datachecker.check.controller;

import io.swagger.v3.oas.annotations.tags.Tag;
import lombok.RequiredArgsConstructor;
import org.opengauss.datachecker.check.cache.TopicRegister;
import org.opengauss.datachecker.check.service.TableKafkaService;
import org.opengauss.datachecker.common.entry.check.TopicRecordInfo;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.validation.constraints.NotEmpty;
import java.util.List;

/**
 * TableKafkaController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Tag(name = "TableKafkaController")
@RestController
@RequiredArgsConstructor
public class TableKafkaController {
    private final TableKafkaService tableKafkaService;
    private final TopicRegister topicRegister;

    /**
     * Refresh the execution status of the data extraction table of the specified task
     *
     * @param topicName      topicName
     * @param partitionTotal partitionTotal
     * @return TopicRecordInfo
     */
    @GetMapping("/table/kafka/consumer/info")
    public List<TopicRecordInfo> getTableKafkaConsumerInfo(
        @RequestParam(value = "topicName") @NotEmpty String topicName,
        @RequestParam(value = "partitionTotal") int partitionTotal) {
        return tableKafkaService.getTableKafkaConsumerInfo(topicName, partitionTotal);
    }

    /**
     * register topic
     *
     * @param table           tableName
     * @param topicPartitions topicPartitions
     * @param endpoint        current endpoint
     * @return topic
     */
    @PostMapping("/register/topic")
    public Topic registerTopic(@RequestParam(value = "tableName") @NotEmpty String table,
        @RequestParam(value = "topicPartitions") int topicPartitions,
        @RequestParam(value = "endpoint") @NonNull Endpoint endpoint) {
        return topicRegister.register(table, topicPartitions, endpoint);
    }
}
