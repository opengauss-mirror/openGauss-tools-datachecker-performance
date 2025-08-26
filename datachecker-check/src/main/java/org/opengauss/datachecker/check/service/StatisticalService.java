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

package org.opengauss.datachecker.check.service;

import org.opengauss.datachecker.check.annotation.aspect.StatisticalRecord;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import javax.annotation.PostConstruct;
import jakarta.validation.constraints.NotNull;
import java.io.File;
import java.time.LocalDateTime;
import java.time.temporal.ChronoUnit;

/**
 * StatisticalService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/20
 * @since ：11
 */
@Service
public class StatisticalService {
    private static final String STATISTICS_RESULT_FILE = "statistics.txt";
    private static final String STATISTICS_RESULT_DIR = "statistics";

    private String statisticalFileName;

    @Value("${data.check.statistical-enable}")
    private boolean shouldEnableStatistical;

    @Value("${data.check.data-path}")
    private String path;

    /**
     * Start loading statistics save path
     */
    @PostConstruct
    public void loadFilePath() {
        if (shouldEnableStatistical) {
            FileUtils.createDirectories(getStatisticalDir());
            statisticalFileName = getStatisticalFileName();
            FileUtils.deleteFile(statisticalFileName);
            statistics("check service start ......", LocalDateTime.now());
        }
    }

    /**
     * Manage statistical information
     *
     * @param name  point information
     * @param start start time
     */
    public void statistics(String name, @NotNull LocalDateTime start) {
        if (shouldEnableStatistical) {
            StatisticalRecord record = buildStatistical(name, start);
            FileUtils.writeAppendFile(statisticalFileName, JsonObjectUtil.format(record));
        }
    }

    private String getStatisticalDir() {
        return path.concat(File.separator).concat(STATISTICS_RESULT_DIR);
    }

    private String getStatisticalFileName() {
        return path.concat(File.separator).concat(STATISTICS_RESULT_DIR).concat(File.separator)
                   .concat(STATISTICS_RESULT_FILE);
    }

    private StatisticalRecord buildStatistical(String name, LocalDateTime start) {
        LocalDateTime end = LocalDateTime.now();
        return new StatisticalRecord().setStart(JsonObjectUtil.formatTime(start)).setEnd(JsonObjectUtil.formatTime(end))
                                      .setCost(start.until(end, ChronoUnit.SECONDS)).setName(name);
    }
}
