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

package org.opengauss.datachecker.check;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.cmd.CheckCommandLine;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;
import org.springframework.stereotype.Indexed;

/**
 * DatacheckerCheckApplication
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Indexed
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.check.client"})
@SpringBootApplication
@ComponentScan(value = {"org.opengauss.datachecker.check", "org.opengauss.datachecker.common"})
public class CheckApplication {
    private static final Logger log = LogUtils.getLogger();

    public static void main(String[] args) {
        try {
            CheckCommandLine commandLine = new CheckCommandLine();
            commandLine.parseArgs(args);
            System.setProperty(CmdOption.LOG_NAME, commandLine.getMode());
            SpringApplication application = new SpringApplication(CheckApplication.class);
            if (commandLine.hasHelp()) {
                commandLine.help();
            } else {
                application.run(args);
            }
        } catch (Throwable er) {
            log.error("{}server start has unknown error", ErrorCode.CHECK_START_ERROR, er);
        }
    }
}
