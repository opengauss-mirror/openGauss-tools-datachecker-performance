/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.check;

import lombok.Getter;
import lombok.RequiredArgsConstructor;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.config.DataCheckConfig;
import org.opengauss.datachecker.check.modules.report.CheckResultManagerService;
import org.opengauss.datachecker.check.service.StatisticalService;
import org.springframework.stereotype.Service;

/**
 * DataCheckRunnableSupport
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/5
 * @since ：11
 */
@Getter
@Service
@RequiredArgsConstructor
public class DataCheckRunnableSupport {
    private final FeignClientService feignClientService;
    private final TableStatusRegister tableStatusRegister;
    private final DataCheckConfig dataCheckConfig;
    private final StatisticalService statisticalService;
    private final KafkaConsumerService kafkaConsumerService;
    private final CheckResultManagerService checkResultManagerService;
}
