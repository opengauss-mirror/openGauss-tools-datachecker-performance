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

package org.opengauss.datachecker.extract.cmd;

import lombok.extern.slf4j.Slf4j;
import org.apache.commons.cli.Option;
import org.opengauss.datachecker.common.service.CommonCommandLine;

import java.util.Objects;

import static org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption.SINK;
import static org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption.SOURCE;

/**
 * extract ExtractCommandLine
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/8
 * @since ：11
 */
@Slf4j
public class ExtractCommandLine extends CommonCommandLine {

    public ExtractCommandLine() {
        super();
        options.addOption(getJvmOption());
        options.addOption(getSinkOption());
        options.addOption(getSourceOption());
    }

    private static Option getSinkOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("start the sink extract endpoint ,  ");
        descBuffer.append("sink and sink cannot be configured at the same java command.  ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("like: ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" java -jar datachecker-extract-0.0.1.jar -sink ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" java -jar datachecker-extract-0.0.1.jar --sink");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" nohup java -jar datachecker-extract-0.0.1.jar --sink >/dev/null 2>&1 & ");
        descBuffer.append(System.lineSeparator());
        return Option.builder(SINK).longOpt(SINK).desc(descBuffer.toString()).build();
    }

    private static Option getSourceOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("start the source extract endpoint ,  ");
        descBuffer.append("sink and source cannot be configured at the same java command.  ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("like: ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" java -jar datachecker-extract-0.0.1.jar -source ");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" java -jar datachecker-extract-0.0.1.jar --source");
        descBuffer.append(System.lineSeparator());
        descBuffer.append(" nohup java -jar datachecker-extract-0.0.1.jar --source >/dev/null 2>&1 & ");
        descBuffer.append(System.lineSeparator());
        return Option.builder(SOURCE).longOpt(SOURCE).desc(descBuffer.toString()).build();
    }

    private static Option getJvmOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("some jvm param examples:");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Xmx3G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Xms3G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:MaxMetaspaceSize=1G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:MetaspaceSize=1G xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -XX:+UseG1GC xxx.jar");
        descBuffer.append(System.lineSeparator());
        return Option.builder().longOpt("jvm").argName("jvm options").hasArgs().desc(descBuffer.toString()).build();
    }

    /**
     * check has only sink
     *
     * @return boolean
     */
    public boolean hasOnlySink() {
        return commandLine.hasOption(SINK) && !commandLine.hasOption(SOURCE);
    }

    /**
     * check has only source
     *
     * @return boolean
     */
    public boolean hasOnlySource() {
        return commandLine.hasOption(SOURCE) && !commandLine.hasOption(SINK);
    }

    public String getMode() {
        String logName = commandLine.getOptionProperties("D").getProperty(CmdOption.LOG_NAME);
        if (hasOnlySink()) {
            return Objects.isNull(logName) ? SINK : logName;
        } else {
            return Objects.isNull(logName) ? SOURCE : logName;
        }
    }
}