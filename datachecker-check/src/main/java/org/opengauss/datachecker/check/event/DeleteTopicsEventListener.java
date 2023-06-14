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

package org.opengauss.datachecker.check.event;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.collections4.CollectionUtils;
import org.apache.kafka.clients.admin.AdminClient;
import org.apache.kafka.clients.admin.AdminClientConfig;
import org.apache.kafka.clients.admin.DeleteTopicsResult;
import org.apache.kafka.clients.admin.KafkaAdminClient;
import org.apache.kafka.clients.admin.TopicListing;
import org.apache.kafka.common.KafkaFuture;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.context.ApplicationListener;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Slf4j
@Component
public class DeleteTopicsEventListener implements ApplicationListener<DeleteTopicsEvent> {
    private static final int DELETE_RETRY_TIMES = 3;
    @Resource
    private FeignClientService feignClient;
    @Value("${spring.kafka.bootstrap-servers}")
    private String bootstrapServers;
    private AdminClient adminClient = null;
    private AtomicInteger retryTimes = new AtomicInteger(0);

    @Override
    public void onApplicationEvent(DeleteTopicsEvent event) {
        try {
            final Object source = event.getSource();
            initAdminClient();
            final DeleteTopics deleteOption = (DeleteTopics) source;
            deleteTopic(deleteOption.getTopicList());
            feignClient.notifyCheckTableFinished(Endpoint.SOURCE, deleteOption.getTableName());
            feignClient.notifyCheckTableFinished(Endpoint.SINK, deleteOption.getTableName());
            log.info("delete topic event : {}", event.getMessage());
        } catch (Exception exception) {
            log.error("delete topic has error ", exception);
        }
    }

    private void deleteTopic(List<String> deleteTopicList) {
        DeleteTopicsResult deleteTopicsResult = adminClient.deleteTopics(deleteTopicList);
        final KafkaFuture<Void> kafkaFuture = deleteTopicsResult.all();
        retryTimes.incrementAndGet();
        try {
            kafkaFuture.get();
            List<String> checkedList = adminClient.listTopics().listings().get().stream().map(TopicListing::name)
                                                  .filter(deleteTopicList::contains).collect(Collectors.toList());
            if (CollectionUtils.isNotEmpty(checkedList)) {
                if (retryTimes.get() <= DELETE_RETRY_TIMES) {
                    deleteTopic(checkedList);
                } else {
                    log.error("retry to delete {} topic error : delete too many times(3) ", checkedList);
                }
            }
        } catch (InterruptedException | ExecutionException ignore) {
        }
    }

    private void initAdminClient() {
        if (this.adminClient == null) {
            Map<String, Object> props = new HashMap<>(1);
            props.put(AdminClientConfig.BOOTSTRAP_SERVERS_CONFIG, bootstrapServers);
            this.adminClient = KafkaAdminClient.create(props);
        }
    }
}
