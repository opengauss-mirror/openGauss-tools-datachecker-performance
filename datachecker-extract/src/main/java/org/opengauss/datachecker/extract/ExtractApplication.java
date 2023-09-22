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

package org.opengauss.datachecker.extract;

import org.opengauss.datachecker.common.exception.ExtractBootstrapException;
import org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption;
import org.opengauss.datachecker.extract.cmd.ExtractCommandLine;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.annotation.ComponentScan;

import static org.opengauss.datachecker.extract.constants.ExtConstants.PROFILE_SINK;
import static org.opengauss.datachecker.extract.constants.ExtConstants.PROFILE_SOURCE;

/**
 * ExtractApplication
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@EnableFeignClients(basePackages = {"org.opengauss.datachecker.extract.client"})
@SpringBootApplication
@ComponentScan(value = {"org.opengauss.datachecker.extract", "org.opengauss.datachecker.common"})
public class ExtractApplication {
    public static void main(String[] args) {
        ExtractCommandLine commandLine = new ExtractCommandLine();
        commandLine.parseArgs(args);
        SpringApplication application = new SpringApplication(ExtractApplication.class);
        System.setProperty(CmdOption.LOG_NAME, commandLine.getMode());
        if (commandLine.hasOnlySink()) {
            application.setAdditionalProfiles(PROFILE_SINK);
            application.run(args);
        } else if (commandLine.hasOnlySource()) {
            application.setAdditionalProfiles(PROFILE_SOURCE);
            application.run(args);
        } else if (commandLine.hasHelp()) {
            commandLine.help();
        } else {
            commandLine.help();
            throw new ExtractBootstrapException("extract profile setting error, profile must be 'sink' or 'source'");
        }
    }
}
