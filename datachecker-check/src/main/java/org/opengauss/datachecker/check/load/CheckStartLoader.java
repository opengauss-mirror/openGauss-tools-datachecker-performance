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
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.check.service.IncrementManagerService;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
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
    @Resource
    private CheckService checkService;
    @Resource
    private IncrementManagerService incrementManagerService;
    @Resource
    private FeignClientService feignClient;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(CheckMode.INCREMENT, checkEnvironment.getCheckMode())) {
            log.info("start data check increment");
            incrementManagerService.startIncrementDataLogs();
            log.info("enabled data check increment mode ,at {}", LocalDateTime.now());
            return;
        }
        final LocalDateTime startTime = LocalDateTime.now();
        checkService.start(CheckMode.FULL);
        final LocalDateTime endTime = LocalDateTime.now();
        log.info("check task execute success ,cost time ={}", Duration.between(startTime, endTime).toSeconds());

        feignClient.shutdown(FULL_CHECK_COMPLETED);
        shutdown(FULL_CHECK_COMPLETED);
    }
}
