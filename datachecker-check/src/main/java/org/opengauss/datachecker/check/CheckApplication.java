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

import org.apache.logging.log4j.LogManager;
import org.opengauss.datachecker.check.cmd.CheckCommandLine;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption;
import org.springframework.boot.SpringApplication;
import org.springframework.boot.autoconfigure.SpringBootApplication;
import org.springframework.boot.autoconfigure.jdbc.DataSourceAutoConfiguration;
import org.springframework.boot.autoconfigure.jdbc.DataSourceTransactionManagerAutoConfiguration;
import org.springframework.boot.autoconfigure.orm.jpa.HibernateJpaAutoConfiguration;
import org.springframework.cloud.openfeign.EnableFeignClients;
import org.springframework.context.ConfigurableApplicationContext;
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
@SpringBootApplication(exclude = {
    DataSourceAutoConfiguration.class, DataSourceTransactionManagerAutoConfiguration.class,
    HibernateJpaAutoConfiguration.class
})
@ComponentScan(value = {"org.opengauss.datachecker.check", "org.opengauss.datachecker.common"})
public class CheckApplication {
    public static void main(String[] args) {
        try {
            CheckCommandLine commandLine = new CheckCommandLine();
            commandLine.parseArgs(args);
            System.setProperty(CmdOption.LOG_NAME, commandLine.getMode());
            SpringApplication application = new SpringApplication(CheckApplication.class);
            if (commandLine.hasHelp()) {
                commandLine.help();
            } else {
                ConfigurableApplicationContext ctx = application.run(args);
                Runtime.getRuntime().addShutdownHook(new Thread(() -> {
                    ctx.close();
                    try {
                        Thread.sleep(3000);
                    } catch (InterruptedException e) {
                        Thread.currentThread().interrupt();
                    }
                    System.setProperty("log4j2.disableShutdownHook", "true");
                    LogManager.shutdown(false); // 禁止关闭时记录日志
                }));
            }
        } catch (Throwable er) {
            throw new CheckingException("server start has unknown error " + er.getMessage());
        }
    }
}
