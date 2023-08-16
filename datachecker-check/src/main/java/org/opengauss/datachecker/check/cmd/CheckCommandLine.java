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

package org.opengauss.datachecker.check.cmd;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Option;
import org.opengauss.datachecker.common.service.CommonCommandLine;

import java.util.Objects;

import static org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption.CHECK;

/**
 * check CheckCommandLine
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/8
 * @since ：11
 */
@Slf4j
public class CheckCommandLine extends CommonCommandLine {

    public CheckCommandLine() {
        super();
        options.addOption(getJvmOption());
        options.addOption(getCheckOption());
    }

    private static Option getJvmOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("some jvm param examples:");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Xmx1G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Xms1G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:MaxMetaspaceSize=512M xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:MetaspaceSize=512M xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:+UseG1GC xxx.jar");
        descBuffer.append(System.lineSeparator());
        return Option.builder().longOpt("jvm").argName("jvm options").hasArgs().desc(descBuffer.toString()).build();
    }

    private static Option getCheckOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("start the check endpoint , args --check can be ignored");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("like: ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" java -jar datachecker-check-0.0.1.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" nohup java -jar datachecker-check-0.0.1.jar >/dev/null 2>&1 & ");
        descBuffer.append(System.lineSeparator());
        return Option.builder().longOpt(CHECK).desc(descBuffer.toString()).build();
    }

    public String getMode() {
        String logName = commandLine.getOptionProperties("D").getProperty(CmdOption.LOG_NAME);
        return Objects.isNull(logName) ? CHECK : logName;
    }
}