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
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.config.DataCheckProperties;
import org.opengauss.datachecker.check.modules.task.TaskManagerService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
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
import java.util.concurrent.TimeUnit;

/**
 * KafkaTopicDeleteProvider
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Service
public class KafkaTopicDeleteProvider implements ApplicationContextAware {
    private static final Logger logKafka = LogUtils.getKafkaLogger();
    private static volatile Map<String, DeleteTopics> deleteTableMap = new ConcurrentHashMap<>();

    private ApplicationContext applicationContext;
    @Resource
    private ShutdownService shutdownService;
    @Resource
    private CustomEventHistory customEventHistory;
    @Resource
    private DataCheckProperties properties;
    @Resource
    private TaskManagerService taskManagerService;

    public void addTableToDropTopic(String tableName) {
        addTableToDropTopic(tableName, false);
    }

    public void addTableToDropTopic(String tableName, boolean immediately) {
        if (properties.getAutoDeleteTopic() == DeleteMode.DELETE_NO.code) {
            return;
        }
        String process = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        final DeleteTopics deleteTopics = new DeleteTopics();
        deleteTopics.setCanDelete(immediately);
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

    /**
     * wait delete topic event complete
     *
     * @return complete
     */
    public boolean waitDeleteTopicsEventCompleted() {
        while (!deleteTableMap.isEmpty() || !customEventHistory.checkAllEventCompleted()) {
            ThreadUtil.sleepOneSecond();
            logKafka.warn("wait delete topic event complete ...");
        }
        return true;
    }

    /**
     * deleteTopicIfTableCheckedCompleted
     */
    public void deleteTopicIfTableCheckedCompleted() {
        if (properties.getAutoDeleteTopic() == DeleteMode.DELETE_IMMEDIATELY.code) {
            startDeleteTopicSchedule();
        }
    }

    private void startDeleteTopicSchedule() {
        ScheduledExecutorService scheduledExecutor =
            ThreadUtil.newSingleThreadScheduledExecutor("delete-topic-scheduled");
        shutdownService.addExecutorService(scheduledExecutor);
        scheduledExecutor.scheduleWithFixedDelay(this::deleteTopicFromCache, 3L, 1, TimeUnit.SECONDS);
    }

    private synchronized void deleteTopicFromCache() {
        List<DeleteTopics> deleteOptions = new LinkedList<>();
        deleteTableMap.forEach((table, deleteOption) -> {
            if (!deleteOption.isCanDelete()) {
                deleteOption.setCanDelete(taskManagerService.isChecked(table));
            }
            if (deleteOption.isCanDelete()) {
                deleteOptions.add(deleteOption);
            }
        });
        if (CollectionUtils.isNotEmpty(deleteOptions)) {
            deleteOptions.forEach(deleteOption -> {
                logKafka.info("publish delete-topic-event table = [{}] ,  current-pending-quantity = [{}]",
                    deleteOption.getTableName(), deleteTableMap.size());
                DeleteTopicsEvent deleteTopicsEvent = new DeleteTopicsEvent(deleteOption, deleteOption.toString());
                customEventHistory.addEvent(deleteTopicsEvent);
                applicationContext.publishEvent(deleteTopicsEvent);
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
