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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.concurrent.BasicThreadFactory;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.beans.BeansException;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ScheduledThreadPoolExecutor;
import java.util.concurrent.TimeUnit;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Service
public class KafkaTopicDeleteProvider implements ApplicationContextAware {
    private static final Logger logKafka = LogUtils.geKafkaLogger();
    private static volatile Map<String, DeleteTopics> deleteTableMap = new ConcurrentHashMap<>();

    private ApplicationContext applicationContext;
    @Resource
    private DataCheckProperties properties;
    @Resource
    private TaskManagerService taskManagerService;
    private volatile String process;

    /**
     * Initialize Admin Client and process
     *
     * @param process process
     */
    public void init(String process) {
        this.process = process;
    }

    public void addTableToDropTopic(String tableName) {
        if (properties.getAutoDeleteTopic() == DeleteMode.DELETE_NO.code) {
            return;
        }
        final DeleteTopics deleteTopics = new DeleteTopics();
        deleteTopics.setCanDelete(false);
        deleteTopics.setTableName(tableName);
        deleteTopics.setTopicList(List.of(TopicUtil.buildTopicName(process, Endpoint.SOURCE, tableName),
            TopicUtil.buildTopicName(process, Endpoint.SINK, tableName)));
        deleteTableMap.put(tableName, deleteTopics);
    }

    public void deleteTopicIfAllCheckedCompleted() {
        if (properties.getAutoDeleteTopic() == DeleteMode.DELETE_END.code) {
            startDeleteTopicSchedule();
        }
    }

    public void deleteTopicIfTableCheckedCompleted() {
        if (properties.getAutoDeleteTopic() == DeleteMode.DELETE_IMMEDIATELY.code) {
            startDeleteTopicSchedule();
        }
    }

    private void startDeleteTopicSchedule() {
        ScheduledExecutorService scheduledExecutor = new ScheduledThreadPoolExecutor(1, new BasicThreadFactory.
            Builder().namingPattern("delete-topic-thread").build());
        scheduledExecutor.scheduleWithFixedDelay(this::deleteTopicFromCache, 3L, 1, TimeUnit.SECONDS);
    }

    private synchronized void deleteTopicFromCache() {
        List<DeleteTopics> deleteOptions = new LinkedList<>();
        deleteTableMap.forEach((table, deleteOption) -> {
            deleteOption.setCanDelete(taskManagerService.isChecked(table));
            if (deleteOption.isCanDelete()) {
                deleteOptions.add(deleteOption);
            }
        });
        if (CollectionUtils.isNotEmpty(deleteOptions)) {
            deleteOptions.forEach(deleteOption -> {
                logKafka.info("publish delete-topic-event table = [{}] ,  current-pending-quantity = [{}]",
                    deleteOption.getTableName(), deleteTableMap.size());
                applicationContext.publishEvent(new DeleteTopicsEvent(deleteOption, deleteOption.toString()));
                deleteTableMap.remove(deleteOption.getTableName());
            });
            deleteOptions.clear();
        }
    }

    @Override
    public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
        this.applicationContext = applicationContext;
    }

    enum DeleteMode {
        /**
         * not delete any topic
         */
        DELETE_NO(0),
        /**
         * delete topic if a table is checked complete.
         */
        DELETE_END(1),
        /**
         * immediately delete
         */
        DELETE_IMMEDIATELY(2);

        private final int code;

        DeleteMode(int code) {
            this.code = code;
        }
    }
}
