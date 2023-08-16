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

import java.lang.management.ManagementFactory;
import java.text.SimpleDateFormat;
import java.util.Date;

/**
 * DateUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
public class DateUtils {
    private static final long TO_DAY = 1000 * 24 * 60 * 60;
    private static final long TO_HOUR = 1000 * 60 * 60;
    private static final long TO_MIN = 1000 * 60;
    private static final String YYYY_MM_DD_HH_MM_SS = "yyyy-MM-dd HH:mm:ss";

    /**
     * Obtain the current Date type date
     *
     * @return Date()
     */
    public static Date getNowDate() {
        return new Date();
    }

    /**
     * format date
     *
     * @param date date
     * @return date
     */
    public static String parseDateToStr(final Date date) {
        return new SimpleDateFormat(YYYY_MM_DD_HH_MM_SS).format(date);
    }

    /**
     * Obtain server startup time
     *
     * @return server start date
     */
    public static Date getServerStartDate() {
        long time = ManagementFactory.getRuntimeMXBean().getStartTime();
        return new Date(time);
    }

    /**
     * Calculate two time differences
     *
     * @param endDate endDate
     * @param nowDate nowDate
     * @return date Poor
     */
    public static String getDatePoor(Date endDate, Date nowDate) {
        long diff = endDate.getTime() - nowDate.getTime();
        long day = diff / TO_DAY;
        long hour = diff % TO_DAY / TO_HOUR;
        long min = diff % TO_DAY % TO_HOUR / TO_MIN;
        return day + "day" + hour + "hour" + min + "min";
    }
}
