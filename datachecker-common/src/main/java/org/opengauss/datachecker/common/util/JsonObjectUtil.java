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

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.serializer.SerializerFeature;

import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;

/**
 * JsonObjectUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
public class JsonObjectUtil {
    private static final String TIME_MILLIS_FORMATTER = "yyyy-MM-dd HH:mm:ss.SSS";
    private static final String TIME_SEC_FORMATTER = "yyyy-MM-dd HH:mm:ss";
    private static final DateTimeFormatter FORMATTER = DateTimeFormatter.ofPattern(TIME_MILLIS_FORMATTER);

    /**
     * The object is formatted as a JSON string,
     * and the formatting is automatically wrapped according to the attributes
     * {@code  SerializerFeature.PrettyFormat}<p>
     * {@code  SerializerFeature.WriteMapNullValue} Null pointer formatting<p>
     * {@code  SerializerFeature.WriteDateUseDateFormat} date format<p>
     *
     * @param object Formatting Objects
     * @return formatting string
     */
    public static String format(Object object) {
        return JSONObject.toJSONString(object, SerializerFeature.PrettyFormat, SerializerFeature.WriteMapNullValue,
            SerializerFeature.WriteDateUseDateFormat);
    }

    /**
     * The object is formatted as a JSON string,
     * and the formatting is automatically wrapped according to the attributes
     * {@code  SerializerFeature.PrettyFormat}<p>
     * {@code  SerializerFeature.WriteMapNullValue} Null pointer formatting<p>
     * {@value TIME_SEC_FORMATTER} date format<p>
     *
     * @param object Formatting Objects
     * @return formatting string
     */
    public static String prettyFormatMillis(Object object) {
        return JSONObject.toJSONStringWithDateFormat(object, TIME_MILLIS_FORMATTER, SerializerFeature.PrettyFormat,
            SerializerFeature.WriteMapNullValue);
    }

    public static String formatSimple(Object object) {
        return JSONObject.toJSONStringWithDateFormat(object, TIME_MILLIS_FORMATTER,
            SerializerFeature.WriteMapNullValue);
    }

    /**
     * The object is formatted as a JSON string,and format time to {@value TIME_SEC_FORMATTER}
     *
     * @param object object
     * @return formatting string
     */
    public static String formatSec(Object object) {
        return JSONObject.toJSONStringWithDateFormat(object, TIME_SEC_FORMATTER, SerializerFeature.WriteMapNullValue);
    }

    /**
     * LocalDateTime time is formatted as yyyy-MM-dd HH:mm:ss.SSS
     *
     * @param localDateTime time
     * @return time of string
     */
    public static String formatTime(LocalDateTime localDateTime) {
        return FORMATTER.format(localDateTime);
    }
}
