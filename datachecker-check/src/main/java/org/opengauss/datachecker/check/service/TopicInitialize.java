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

import org.apache.commons.collections4.CollectionUtils;
import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.constant.Constants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;

/**
 * TopicInitialize
 *
 * @author ：wangchao
 * @date ：Created in 2024/3/9
 * @since ：11
 */
@Service
public class TopicInitialize {
    private static final Logger log = LogUtils.getLogger(TopicInitialize.class);

    @Resource
    private KafkaServiceManager kafkaServiceManager;
    private String checkPointSwapTopicName = null;
    private final List<String> topicList = new LinkedList<>();

    /**
     * <pre>
     * 执行Topic初始化
     * 创建check point swap topic
     * 创建校验数据topic
     * </pre>
     *
     * @param processNo 进程号
     */
    public void initialize(String processNo) {
        kafkaServiceManager.initAdminClient();
        checkPointSwapTopicName = getCheckPointSwapTopicName(processNo);
        createCheckPointSwapTopic();
        int maximumTopicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
        maximumTopicSize = maximumTopicSize > 0 ? maximumTopicSize : 1;
        for (int i = 0; i < maximumTopicSize; i++) {
            topicList.add(createTopic(processNo, Endpoint.SOURCE, i));
            topicList.add(createTopic(processNo, Endpoint.SINK, i));
        }
    }

    /**
     * 获取检查点交换topic名称
     *
     * @return checkPointSwapTopicName
     */
    public String getCheckPointSwapTopicName() {
        return checkPointSwapTopicName;
    }

    private void createCheckPointSwapTopic() {
        kafkaServiceManager.createTopic(checkPointSwapTopicName, 1);
        LogUtils.info(log, "create check point swap topic {}", checkPointSwapTopicName);
    }

    private String getCheckPointSwapTopicName(String process) {
        return String.format(Constants.SWAP_POINT_TOPIC_TEMP, process);
    }

    /**
     * <pre>
     * 删除topic
     * 删除检查点交换Topic
     * 删除数据校验数据存储Topic
     * </pre>
     */
    public void drop() {
        if (StringUtils.isNotEmpty(checkPointSwapTopicName)) {
            kafkaServiceManager.deleteTopic(List.of(checkPointSwapTopicName));
            LogUtils.info(log, "drop check point swap topic [{}]", checkPointSwapTopicName);
        }
        if (CollectionUtils.isNotEmpty(topicList)) {
            topicList.forEach(topic -> kafkaServiceManager.deleteTopic(List.of(topic)));
            LogUtils.info(log, "drop data check fixed topic name {}", topicList);
        }
    }

    private String createTopic(String process, Endpoint endpoint, int no) {
        String topicName = TopicUtil.createMoreFixedTopicName(process, endpoint, no);
        kafkaServiceManager.createTopic(topicName, 1);
        LogUtils.info(log, "create data check fixed topic name {}", topicName);
        return topicName;
    }
}
