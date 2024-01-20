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

package org.opengauss.datachecker.extract.slice;

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.opengauss.datachecker.extract.cache.TopicCache;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.kafka.KafkaAdminService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * SliceRegister
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
@Component
public class SliceRegister {
    @Resource
    private CheckingFeignClient checkingClient;
    @Resource
    private KafkaAdminService kafkaAdminService;

    /**
     * register slice to check service
     *
     * @param sliceList slice
     */
    public void batchRegister(List<SliceVo> sliceList) {
        List<SliceVo> tableSliceTmpList = new ArrayList<>();
        sliceList.forEach(sliceVo -> {
            tableSliceTmpList.add(sliceVo);
            if (tableSliceTmpList.size() >= 10) {
                checkingClient.batchRegisterSlice(tableSliceTmpList);
                tableSliceTmpList.clear();
            }
        });
        if (tableSliceTmpList.size() > 0) {
            checkingClient.batchRegisterSlice(tableSliceTmpList);
        }
    }

    /**
     * register table topic to check service
     *
     * @param tableName table
     * @param ptnNum    ptnNum
     * @return true | false
     */
    public boolean registerTopic(String tableName, int ptnNum) {
        Topic topic = TopicCache.getTopic(tableName);
        if (Objects.nonNull(topic)) {
            return true;
        }
        if (!TopicCache.canCreateTopic(ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE))) {
            return false;
        }
        topic = checkingClient.registerTopic(tableName, ptnNum, ConfigCache.getEndPoint());
        if (kafkaAdminService.createTopic(topic.getTopicName(ConfigCache.getEndPoint()), topic.getPtnNum())) {
            TopicCache.add(topic);
            return true;
        } else {
            return false;
        }
    }

    /**
     * check table has registered topic
     *
     * @param table table
     * @return true | false
     */
    public boolean checkTopicRegistered(String table) {
        return Objects.nonNull(TopicCache.getTopic(table));
    }

    /**
     * start table checkpoint monitor
     */
    public void startCheckPointMonitor() {
        checkingClient.startCheckPointMonitor();
    }

    /**
     * stop table checkpoint monitor
     *
     * @param endpoint endpoint
     */
    public void stopCheckPointMonitor(Endpoint endpoint) {
        checkingClient.stopCheckPointMonitor(endpoint);
    }
}