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

import org.opengauss.datachecker.check.modules.check.ExportCheckResult;
import org.opengauss.datachecker.check.modules.report.SliceProgressService;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.event.KafkaTopicDeleteProvider;
import org.opengauss.datachecker.check.service.CheckTableStructureService;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.check.CheckTableInfo;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.service.MemoryManagerService;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.common.util.ThreadUtil;
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
@Order(199)
@Service
public class CheckStartLoader extends AbstractCheckLoader {
    private static final String FULL_CHECK_COMPLETED = "full check completed";

    @Value("${spring.memory-monitor-enable}")
    private boolean isEnableMemoryMonitor;
    @Resource
    private CheckService checkService;
    @Resource
    private KafkaTopicDeleteProvider kafkaTopicDeleteProvider;
    @Resource
    private MemoryManagerService memoryManagerService;
    @Resource
    private SliceProgressService sliceProgressService;
    @Resource
    private CheckTableStructureService checkTableStructureService;
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (!Objects.equals(CheckMode.FULL, checkEnvironment.getCheckMode())) {
            return;
        }
        if (!checkEnvironment.isCheckTableEmpty()) {
            ExportCheckResult.backCheckResultDirectory();
            memoryManagerService.startMemoryManager(isEnableMemoryMonitor);
            final LocalDateTime startTime = LocalDateTime.now();
            String processNo = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
            CheckTableInfo checkTableInfo= checkTableStructureService.check(processNo);
            int checkedTableCount = checkTableInfo.fetchCheckedTableCount();
            sliceProgressService.startProgressing();
            sliceProgressService.updateTotalTableCount(checkedTableCount);
            checkService.start(CheckMode.FULL);
            kafkaTopicDeleteProvider.deleteTopicIfAllCheckedCompleted();
            kafkaTopicDeleteProvider.waitDeleteTopicsEventCompleted();
            TaskRegisterCenter registerCenter = SpringUtil.getBean(TaskRegisterCenter.class);
            while (!registerCenter.checkCompletedAll(checkedTableCount)) {
                ThreadUtil.sleepOneSecond();
            }
            log.info("check task execute success ,cost time ={}", Duration.between(startTime, LocalDateTime.now())
                                                                          .toSeconds());
        } else {
            log.info("check task execute success ,cost time =0");
        }
        shutdown(FULL_CHECK_COMPLETED);
    }
}
