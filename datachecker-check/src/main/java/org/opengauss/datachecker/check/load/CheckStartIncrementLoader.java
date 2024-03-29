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
import org.opengauss.datachecker.check.modules.report.ProgressService;
import org.opengauss.datachecker.check.service.IncrementManagerService;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
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
@Order(198)
@Service
public class CheckStartIncrementLoader extends AbstractCheckLoader {
    @Resource
    private IncrementManagerService incrementManagerService;
    @Resource
    private ProgressService progressService;

    @Override
    public void load(CheckEnvironment checkEnvironment) {
        if (Objects.equals(CheckMode.INCREMENT, checkEnvironment.getCheckMode())) {
            log.info("start data check increment");
            final LocalDateTime startTime = LocalDateTime.now();
            progressService.progressing(checkEnvironment, startTime);
            incrementManagerService.startIncrementDataLogs();
            log.info("enabled data check increment mode ,at {}", startTime);
        }
    }
}
