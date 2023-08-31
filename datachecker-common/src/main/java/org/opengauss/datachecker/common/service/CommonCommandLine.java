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

package org.opengauss.datachecker.common.service;

import org.apache.commons.cli.CommandLine;
import org.apache.commons.cli.CommandLineParser;
import org.apache.commons.cli.DefaultParser;
import org.apache.commons.cli.HelpFormatter;
import org.apache.commons.cli.Option;
import org.apache.commons.cli.Options;
import org.apache.commons.cli.ParseException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.exception.ExtractBootstrapException;
import org.opengauss.datachecker.common.util.LogUtils;

import static org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption.HELP;
import static org.opengauss.datachecker.common.service.CommonCommandLine.CmdOption.HELP_LONG;

/**
 * CommonCommandLine
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/13
 * @since ：11
 */
public class CommonCommandLine {
    private static final Logger log = LogUtils.getLogger();

    protected static Options options = new Options();
    protected static CommandLineParser parser = new DefaultParser();
    protected static CommandLine commandLine;

    public CommonCommandLine() {
        options.addOption(getHelpOption());
        options.addOption(getDStartOption());
    }

    /**
     * loader.path && spring.config.additional-location start command param
     *
     * @return command option
     */
    protected static Option getDStartOption() {
        StringBuffer descBuffer = new StringBuffer();
        descBuffer.append("server -D param :");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -DlogName=check -jar xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("logName to set log4j2 file-name");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Dloader.path=./lib -jar xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("loader.path to Reassign third-party public jar directory");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("java -Dspring.config.additional-location = config/application.yml -jar xxx.jar");
        descBuffer.append(System.lineSeparator());
        descBuffer.append("additional-location to Config file locations used in addition to the defaults.");
        descBuffer.append(System.lineSeparator());
        return Option.builder("D").argName("config=file").hasArgs().valueSeparator().desc(descBuffer.toString())
                     .build();
    }

    /**
     * help command param
     *
     * @return command option
     */
    protected static Option getHelpOption() {
        return Option.builder(HELP).longOpt(HELP_LONG).argName("gs_datacheck help").hasArg(false)
                     .desc("help to use gs_datacheck").build();
    }

    /**
     * parse args to CommandLine
     *
     * @param args args
     * @return CommandLine
     */
    public void parseArgs(String[] args) {
        try {
            commandLine = parser.parse(options, args);
        } catch (ParseException e) {
            log.error("invalid bootstrap args {}", args);
            throw new ExtractBootstrapException("invalid bootstrap args");
        }
    }

    /**
     * check has help command
     *
     * @return boolean
     */
    public boolean hasHelp() {
        return commandLine.hasOption(HELP);
    }

    /**
     * print help
     */
    public void help() {
        HelpFormatter formatter = new HelpFormatter();
        formatter.printHelp("gs_datacheck help ", options);
        System.out.println();
    }

    public interface CmdOption {
        String CHECK = "check";
        String SOURCE = "source";
        String SINK = "sink";
        String HELP = "h";
        String LOG_NAME = "logName";
        String HELP_LONG = "help";
    }
}
