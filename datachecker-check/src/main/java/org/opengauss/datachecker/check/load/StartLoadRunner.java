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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.service.ConfigManagement;
import org.opengauss.datachecker.check.service.KafkaServiceManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.Arrays;

/**
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
@Component
public class StartLoadRunner implements ApplicationRunner {
    private static final Logger log = LogUtils.getLogger(StartLoadRunner.class);

    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private AsyncCheckStarter startRunner;
    @Resource
    private ConfigManagement configManagement;
    @Resource
    private KafkaServiceManager kafkaServiceManager;

    @Override
    public void run(ApplicationArguments args) {
        LogUtils.info(log, "start load runner :{}", Arrays.deepToString(args.getSourceArgs()));
        configManagement.init();
        startRunner.initCheckEnvironment(checkEnvironment);
        startRunner.start();
    }
}
