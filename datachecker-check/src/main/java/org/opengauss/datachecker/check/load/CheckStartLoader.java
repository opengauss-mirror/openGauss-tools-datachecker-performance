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

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.modules.report.CheckResultManagerService;
import org.opengauss.datachecker.check.modules.report.ProgressService;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.event.KafkaTopicDeleteProvider;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.service.MemoryManagerService;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * CheckStartLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/11/9
 * @since ：11
 */
@Slf4j
@Order(199)
@Service
public class CheckStartLoader extends AbstractCheckLoader {
    private static final String FULL_CHECK_COMPLETED = "full check completed";
    @Value("${spring.memory-monitor-enable}")
    private boolean isEnableMemoryMonitor;
    @Resource
    private CheckService checkService;
    @Resource
    private FeignClientService feignClient;
    @Resource
    private CheckResultManagerService checkResultManagerService;
    @Resource
    private KafkaTopicDeleteProvider kafkaTopicDeleteProvider;
    @Resource
    private ProgressService progressService;
    @Resource
    private MemoryManagerService memoryManagerService;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(CheckMode.FULL, checkEnvironment.getCheckMode())) {
            if (!checkEnvironment.isCheckTableEmpty()) {
                memoryManagerService.startMemoryManager(isEnableMemoryMonitor);
                final LocalDateTime startTime = LocalDateTime.now();
                progressService.progressing(checkEnvironment, startTime);
                checkService.start(CheckMode.FULL);
                final LocalDateTime endTime = LocalDateTime.now();
                log.info("check task execute success ,cost time ={}", Duration.between(startTime, endTime).toSeconds());
                checkResultManagerService.summaryCheckResult();
                kafkaTopicDeleteProvider.deleteTopicIfAllCheckedCompleted();
                kafkaTopicDeleteProvider.waitDeleteTopicsEventCompleted();
            }
            feignClient.shutdown(FULL_CHECK_COMPLETED);
            shutdown(FULL_CHECK_COMPLETED);
        }
    }
}
