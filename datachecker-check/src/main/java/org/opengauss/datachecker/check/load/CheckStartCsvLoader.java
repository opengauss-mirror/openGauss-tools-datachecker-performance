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

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.event.KafkaTopicDeleteProvider;
import org.opengauss.datachecker.check.modules.report.SliceProgressService;
import org.opengauss.datachecker.check.service.CsvProcessManagement;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.check.service.TopicInitialize;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.Objects;

/**
 * CheckStartLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Order(198)
@Service
public class CheckStartCsvLoader extends AbstractCheckLoader {
    private static final String CSV_CHECK_COMPLETED = "csv check completed";
    @Resource
    private FeignClientService feignClient;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private KafkaTopicDeleteProvider kafkaTopicDeleteProvider;
    @Resource
    private CsvProcessManagement csvProcessManagement;
    @Resource
    private TopicInitialize topicInitialize;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(CheckMode.CSV, ConfigCache.getCheckMode())) {
            LogUtils.info(log, "start data check csv");
            String processNo = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
            topicInitialize.initialize(processNo);
            sliceProgressService.startProgressing();
            int count = feignClient.fetchCsvCheckTableCount(Endpoint.SOURCE);
            sliceProgressService.updateTotalTableCount(count);
            csvProcessManagement.startTaskDispatcher();
            feignClient.enableCsvExtractService();
            LogUtils.info(log, "enabled data check csv mode");
            kafkaTopicDeleteProvider.deleteTopicIfTableCheckedCompleted();
            kafkaTopicDeleteProvider.deleteTopicIfAllCheckedCompleted();
            TaskRegisterCenter registerCenter = SpringUtil.getBean(TaskRegisterCenter.class);
            while (!registerCenter.checkCompletedAll(count)
                && kafkaTopicDeleteProvider.waitDeleteTopicsEventCompleted()) {
                ThreadUtil.sleepOneSecond();
            }
            csvProcessManagement.closeTaskDispatcher();
            topicInitialize.drop();
            shutdown(CSV_CHECK_COMPLETED);
        }
    }
}
