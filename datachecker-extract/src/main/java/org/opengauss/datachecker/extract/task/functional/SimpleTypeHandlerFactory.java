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

package org.opengauss.datachecker.extract.task.functional;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.DateTimeFormatterMap;
import org.opengauss.datachecker.common.util.HexUtil;
import org.opengauss.datachecker.common.util.LogUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.concurrent.ConcurrentHashMap;

/**
 * 类型处理器工厂
 *
 * @author ：wangchao
 * @date ：Created in 2024/3/4
 * @since ：11
 */
public class SimpleTypeHandlerFactory {
    private static final Logger LOG = LogUtils.getLogger();
    private static final String NULL = null;
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatterMap TIMESTAMP_MAPPER = new DateTimeFormatterMap();
    private static final Map<Integer, String> FLOAT_FORMAT_CACHE = new ConcurrentHashMap<>();
    private static final int O_NUMERIC_SCALE_F84 = -84;
    private static final int O_NUMERIC_SCALE_0 = 0;

    /**
     * 字符串是否是科学计数
     *
     * @param value value
     * @return boolean
     */
    private static boolean isScientificNotation(String value) {
        return value.contains("E") || value.contains("e");
    }

    /**
     * 根据scale参数格式化浮点类型数据
     *
     * @param scale scale
     * @return value
     */
    private static String getFloatFormat(Integer scale) {
        return FLOAT_FORMAT_CACHE.computeIfAbsent(scale, key -> "%." + key + "f");
    }

    /**
     * <pre>
     * 创建 bit 类型处理函数
     * 如果 bit precision ==1 即bit(1),则该类型默认为布尔值，其值仅有0,1两个值。
     * 默认返回字符串结果0,1值
     * 如果precision !=1,则转换bit类型值为16进制字符串
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createBitHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            if (rsmd.getPrecision(columnIdx) == 1) {
                return resultSet.getString(columnIdx);
            }
            return HexUtil.byteToHexTrim(resultSet.getBytes(columnIdx));
        };
    }

    /**
     * <pre>
     * 创建 bit 类型处理函数
     * Csv 模式下，Bit类型直接输出原始字符串值。
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createCsvBitHandler() {
        return ResultSet::getString;
    }

    /**
     * <pre>
     * 创建 浮点 类型处理函数
     * Csv 模式下，浮点类型直接输出原始字符串值。如果存在科学计数则展开科学计数表示
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createCsvFloatHandler() {
        return (resultSet, columnIdx) -> {
            String floatValue = resultSet.getString(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            if (isScientificNotation(floatValue)) {
                return new BigDecimal(floatValue).toPlainString();
            }
            return floatValue;
        };
    }

    /**
     * <pre>
     * 创建 Char 类型处理函数
     * Char 类型直接输出原始字符串值。
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createCharHandler() {
        return ResultSet::getString;
    }

    /**
     * <pre>
     * 创建 Byte 类型处理函数
     * byte类型转换为16进制字符串
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createBytesHandler() {
        return (resultSet, columnLabel) -> HexUtil.byteToHex(resultSet.getBytes(columnLabel));
    }

    /**
     * <pre>
     * 创建 DateTime/timestamp时间 类型处理函数
     * 根据时间类型scale格式化时间毫秒数
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createDateTimeHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            final Timestamp timestamp =
                resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
            DateTimeFormatter dateTimeFormatter = TIMESTAMP_MAPPER.get(rsmd.getScale(columnIdx));
            return Objects.nonNull(timestamp) ? dateTimeFormatter.format(timestamp.toLocalDateTime()) : NULL;
        };
    }

    /**
     * <pre>
     * 创建 Date 日期 类型处理函数
     * 统一格式化日期输出格式 yyyy-MM-dd
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createDateHandler() {
        return (resultSet, columnLabel) -> {
            final Date date = resultSet.getDate(columnLabel);
            return Objects.nonNull(date) ? DATE.format(date.toLocalDate()) : NULL;
        };
    }

    /**
     * 创建 通用对象类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createObjectHandler() {
        return (resultSet, columnLabel) -> {
            Object object = resultSet.getObject(columnLabel);
            return resultSet.wasNull() ? NULL : object.toString();
        };
    }

    /**
     * <pre>
     * 创建 Time 时间 类型处理函数
     * 统一格式化时间输出格式 HH:mm:ss
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createTimeHandler() {
        return (resultSet, columnLabel) -> {
            final Time time = resultSet.getTime(columnLabel);
            return Objects.nonNull(time) ? TIME.format(time.toLocalTime()) : NULL;
        };
    }

    /**
     * <pre>
     * 创建 Year 年 类型处理函数
     * 统一格式化年输出格式 yyyy
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createYearHandler() {
        return (resultSet, columnLabel) -> {
            final Date date = resultSet.getDate(columnLabel);
            return Objects.nonNull(date) ? YEAR.format(date.toLocalDate()) : NULL;
        };
    }

    /**
     * <pre>
     * 创建 openGauss 浮点 类型处理函数 float1,float2
     * 并根据field mod 格式化浮点类型值，不进行科学计数值判断
     * </pre>
     *
     * @return OpgsTypeHandler
     */
    public OpgsTypeHandler createOgSmallFloatHandler() {
        return (resultSet, columnIdx, field) -> {
            float floatValue = resultSet.getFloat(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            String value = String.valueOf(floatValue);
            int scale = field.getMod();
            if (scale > 0) {
                return String.format(getFloatFormat(scale), floatValue);
            }
            return value;
        };
    }

    /**
     * <pre>
     * 创建 浮点 类型处理函数
     * 并根据Scale格式化浮点类型值，并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createFloatHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            float floatValue = resultSet.getFloat(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            String value = String.valueOf(floatValue);
            if (isScientificNotation(value)) {
                return new BigDecimal(value).toPlainString();
            }
            int scale = rsmd.getScale(columnIdx);
            if (scale > 0) {
                return String.format(getFloatFormat(scale), floatValue);
            }
            return value;
        };
    }

    /**
     * <pre>
     * 创建 openGauss 浮点 类型处理函数 float4,float8
     * 并根据field mod 格式化浮点类型值，并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return OpgsTypeHandler
     */
    public OpgsTypeHandler createOgFloatHandler() {
        return (resultSet, columnIdx, filed) -> {
            float floatValue = resultSet.getFloat(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            int scale = filed.getMod();
            String value = String.valueOf(floatValue);
            if (isScientificNotation(value)) {
                return new BigDecimal(value).toPlainString();
            }
            if (scale > 0) {
                return String.format(getFloatFormat(scale), floatValue);
            }
            return value;
        };
    }

    /**
     * <pre>
     * 创建 openGauss 浮点 类型处理函数 double
     * 并根据field mod 格式化浮点类型值，并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return OpgsTypeHandler
     */
    public OpgsTypeHandler createOgDoubleHandler() {
        return (resultSet, columnIdx, filed) -> {
            double doubleValue = resultSet.getDouble(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            int scale = filed.getMod();
            if (scale > 0) {
                return String.format(getFloatFormat(scale), doubleValue);
            } else {
                String value = String.valueOf(doubleValue);
                if (isScientificNotation(value)) {
                    return new BigDecimal(value).toPlainString();
                }
                return value;
            }
        };
    }

    /**
     * <pre>
     * 创建 mysql 浮点 类型处理函数 double
     * 并根据field decimals 格式化浮点类型值，并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return MysqlTypeHandler
     */
    public MysqlTypeHandler createMysqlDoubleHandler() {
        return (resultSet, columnIdx, filed) -> {
            double doubleValue = resultSet.getDouble(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            int scale = filed.getDecimals();
            if (scale > 0) {
                return String.format(getFloatFormat(scale), doubleValue);
            } else {
                String value = String.valueOf(doubleValue);
                if (isScientificNotation(value)) {
                    return new BigDecimal(value).toPlainString();
                }
                return value;
            }
        };
    }

    /**
     * <pre>
     * 创建 整形 int类型处理函数
     * 并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createIntHandler() {
        return (resultSet, columnLabel) -> {
            int intValue = resultSet.getInt(columnLabel);
            if (resultSet.wasNull()) {
                return NULL;
            }
            String value = String.valueOf(intValue);
            if (isScientificNotation(value)) {
                return new BigDecimal(value).toPlainString();
            }
            return value;
        };
    }

    /**
     * <pre>
     * 创建 整形 long类型处理函数
     * 并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createLongHandler() {
        return (resultSet, columnLabel) -> {
            long intValue = resultSet.getLong(columnLabel);
            if (resultSet.wasNull()) {
                return NULL;
            }
            String value = String.valueOf(intValue);
            if (isScientificNotation(value)) {
                return new BigDecimal(value).toPlainString();
            }
            return value;
        };
    }

    /**
     * <pre>
     * 创建 无符号整形 unsigned long类型处理函数
     * 并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createUnsignedLongHandler() {
        return (resultSet, columnLabel) -> {
            BigDecimal value = resultSet.getBigDecimal(columnLabel);
            if (resultSet.wasNull()) {
                return NULL;
            }
            String bigintValue = String.valueOf(value.toBigInteger());
            if (isScientificNotation(bigintValue)) {
                return value.toPlainString();
            }
            return bigintValue;
        };
    }

    /**
     * <pre>
     * 创建 bigDecimal 类型处理函数
     * 并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createBigDecimalHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            BigDecimal decimalValue = resultSet.getBigDecimal(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            int precision = rsmd.getPrecision(columnIdx);
            int scale = rsmd.getScale(columnIdx);
            if (precision > scale && scale > 0) {
                return decimalValue.toPlainString();
            } else {
                String value = String.valueOf(decimalValue.doubleValue());
                if (isScientificNotation(value)) {
                    return new BigDecimal(value).toPlainString();
                }
                return value;
            }
        };
    }

    /**
     * 创建 blob 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createBlobHandler() {
        return ResultSet::getString;
    }

    /**
     * 创建 clob 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createClobHandler() {
        return ResultSet::getString;
    }

    /**
     * 创建 xml 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createXmlHandler() {
        return ResultSet::getString;
    }

    /**
     * <pre>
     * 创建 openGauss bit 类型处理函数
     * bit 类型格式化为16进制字符串
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOgBitHandler() {
        return (rs, columnLabel) -> HexUtil.binaryToHex(rs.getString(columnLabel));
    }

    /**
     * <pre>
     * 创建 openGauss binary 类型处理函数
     * binary 类型格式化为16进制字符串 ,屏蔽16进制字符串前缀标识符 \x
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOgBinaryHandler() {
        return (rs, columnLabel) -> {
            String binary = rs.getString(columnLabel);
            return Objects.isNull(binary) ? NULL : binary.substring(2)
                                                         .toUpperCase(Locale.ENGLISH);
        };
    }

    /**
     * <pre>
     * 创建 boolean 类型处理函数
     * boolean类型格式化为 true false 字符串
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createBooleanHandler() {
        return (rs, columnLabel) -> rs.getBoolean(columnLabel) ? "true" : "false";
    }

    /**
     * 创建 openGauss bp char 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOgBpCharHandler() {
        return ResultSet::getString;
    }

    /**
     * 创建 small int 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createSmallIntHandler() {
        return (resultSet, columnLabel) -> {
            int intValue = resultSet.getInt(columnLabel);
            if (resultSet.wasNull()) {
                return NULL;
            }
            return String.valueOf(intValue);
        };
    }

    /**
     * 创建 mysql 时间类型datetime/timestamp 类型处理函数
     *
     * @return MysqlTypeHandler
     */
    public MysqlTypeHandler createMysqlDateTimeHandler() {
        return (resultSet, columnIdx, field) -> {
            final Timestamp timestamp =
                resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
            if (resultSet.wasNull()) {
                return NULL;
            }
            DateTimeFormatter dateTimeFormatter = TIMESTAMP_MAPPER.get(field.getDecimals());
            return dateTimeFormatter.format(timestamp.toLocalDateTime());
        };
    }

    /**
     * <pre>
     * 创建 oracle 数字 类型处理函数
     * oracle scale >= -84 && scale<=0 则BigDecimal格式化为整数，scale>0 格式化为浮点类型数据
     * 并判断value值是否为科学计数法表示，是则展平科学计数
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createOracleBigDecimalHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            BigDecimal decimalValue = resultSet.getBigDecimal(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            int scale = rsmd.getScale(columnIdx);
            String value;
            if (scale >= O_NUMERIC_SCALE_F84 && scale <= O_NUMERIC_SCALE_0) {
                value = String.valueOf(decimalValue.toBigInteger());
            } else {
                value = String.valueOf(decimalValue.doubleValue());
            }
            if (isScientificNotation(value)) {
                return new BigDecimal(value).toPlainString();
            }
            return value;
        };
    }

    /**
     * 创建 oracle Raw 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleRawHandler() {
        return ResultSet::getString;
    }

    /**
     * 创建 oracle clob 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleClobHandler() {
        return (resultSet, columnLabel) -> {
            if (resultSet.wasNull()) {
                return NULL;
            }
            StringBuilder sb = new StringBuilder();
            BufferedReader bf = null;
            Reader reader = null;
            try {
                reader = resultSet.getCharacterStream(columnLabel);
                if (reader == null) {
                    return NULL;
                }
                bf = new BufferedReader(reader);
                String line;
                while ((line = bf.readLine()) != null) {
                    sb.append(line);
                }
            } catch (IOException io) {
                LOG.error("read clobToString error");
            } finally {
                closeBufferedReader(bf);
                closeReader(reader);
            }
            return sb.toString();
        };
    }

    private void closeBufferedReader(BufferedReader bf) {
        try {
            if (Objects.nonNull(bf)) {
                bf.close();
            }
        } catch (IOException e) {
            LOG.error("close BufferedReader error");
        }
    }

    private void closeReader(Reader reader) {
        try {
            if (Objects.nonNull(reader)) {
                reader.close();
            }
        } catch (IOException e) {
            LOG.error("close Reader error");
        }
    }

    /**
     * 创建 oracle blob 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleBlobHandler() {
        return (resultSet, columnLabel) -> {
            if (resultSet.wasNull()) {
                return NULL;
            }
            return HexUtil.byteToHexTrim(resultSet.getBytes(columnLabel));
        };
    }

    /**
     * 创建 oracle xml 类型处理函数
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleXmlHandler() {
        return ResultSet::getString;
    }
}
