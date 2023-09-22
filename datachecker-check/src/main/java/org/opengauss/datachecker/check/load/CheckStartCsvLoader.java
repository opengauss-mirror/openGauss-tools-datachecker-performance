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
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
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

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(CheckMode.CSV, ConfigCache.getCheckMode())) {
            log.info("start data check csv");
            sliceProgressService.startProgressing();
            int count = feignClient.fetchCheckTableCount();
            sliceProgressService.updateTotalTableCount(count);

            feignClient.enableCsvExtractService();
            log.info("enabled data check csv mode");
            kafkaTopicDeleteProvider.deleteTopicIfTableCheckedCompleted();
            kafkaTopicDeleteProvider.deleteTopicIfAllCheckedCompleted();
            kafkaTopicDeleteProvider.waitDeleteTopicsEventCompleted();
            TaskRegisterCenter registerCenter = SpringUtil.getBean(TaskRegisterCenter.class);
            while (!registerCenter.checkCompletedAll(count)) {
                ThreadUtil.sleepOneSecond();
            }
            shutdown(CSV_CHECK_COMPLETED);
        }
    }
}
