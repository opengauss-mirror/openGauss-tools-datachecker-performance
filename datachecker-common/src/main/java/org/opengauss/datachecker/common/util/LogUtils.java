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

package org.opengauss.datachecker.common.util;

import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LogUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/6/14
 * @since ：11
 */
public class LogUtils {
    private static final String CHECK_BUSINESS = "check_business";
    private static final String EXTRACT_BUSINESS = "extract_business";
    private static final String KAFKA_BUSINESS = "kafka_business";

    /**
     * get kafka business logger
     *
     * @return logger
     */
    public static Logger geKafkaLogger() {
        return LogManager.getLogger(KAFKA_BUSINESS);
    }

    /**
     * get check business logger
     *
     * @return logger
     */
    public static Logger getCheckLogger() {
        return LogManager.getLogger(CHECK_BUSINESS);
    }

    /**
     * get extrect business logger
     *
     * @return logger
     */
    public static Logger getExtractLogger() {
        return LogManager.getLogger(EXTRACT_BUSINESS);
    }
}
