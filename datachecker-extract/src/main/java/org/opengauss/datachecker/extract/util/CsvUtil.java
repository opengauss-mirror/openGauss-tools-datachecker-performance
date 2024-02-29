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

package org.opengauss.datachecker.extract.util;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

import java.nio.file.Files;
import java.nio.file.Path;

/**
 * CsvUtil
 *
 * @author ：wangchao
 * @date ：Created in 2024/2/29
 * @since ：11
 */
public class CsvUtil {
    private static final Logger log = LogUtils.getLogger();

    /**
     * check file exist ,if not exist ,wait and sleep half second to continue check.
     *
     * @param file file
     * @return boolean
     */
    public static boolean checkExistAndWait(Path file) {
        int maxRetryTimes = ConfigCache.getIntValue(ConfigConstants.MAX_RETRY_TIMES);
        while (Files.notExists(file) && maxRetryTimes > 0) {
            log.warn("file {} is not exist, waiting ...", file);
            ThreadUtil.sleepHalfSecond();
            maxRetryTimes--;
        }
        return Files.exists(file);
    }
}
