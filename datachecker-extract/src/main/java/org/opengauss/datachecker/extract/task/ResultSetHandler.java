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

package org.opengauss.datachecker.extract.task;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.DateTimeFormatterMap;
import org.opengauss.datachecker.common.util.HexUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.lang.NonNull;

import java.math.BigDecimal;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.text.DecimalFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Result set object processor
 *
 * @author wang chao
 * @date ：Created in 2022/6/13
 * @since 11
 **/
public abstract class ResultSetHandler {
    private static Map<Integer, DecimalFormat> decimalFormatCache = new HashMap<>();
    private static final String DECIMAL_FORMAT_PATTERN_START = "0.";
    private static final String DECIMAL_APPEND_ZERO = "0";

    protected static final Logger LOG = LogUtils.getLogger();
    protected static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    protected static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected static final DateTimeFormatter TIMESTAMP_NANOS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    protected static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");

    /**
     * TIMESTAMP_MAPPER {@link DateTimeFormatterMap}
     */
    protected static final DateTimeFormatterMap TIMESTAMP_MAPPER = new DateTimeFormatterMap();
    protected static final String EMPTY = "";
    protected static final String NULL = null;

    protected static final int NUMERIC_SCALE_F84 = -84;
    protected static final int NUMERIC_SCALE_0 = 0;
    protected static final int NUMERIC_PRECISION_0 = 0;
    protected final boolean supplyZero;

    protected ResultSetHandler() {
        this.supplyZero = false;
    }

    protected ResultSetHandler(Boolean supplyZero) {
        this.supplyZero = supplyZero;
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param tableName JDBC Data query table
     * @param rsmd      JDBC Data query result set
     * @param resultSet JDBC Data query result set
     * @param values    values
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(final String tableName, ResultSetMetaData rsmd, ResultSet resultSet,
        Map<String, String> values) {
        try {
            IntStream.rangeClosed(1, rsmd.getColumnCount())
                     .forEach(columnIdx -> {
                         String columnLabel = null;
                         try {
                             columnLabel = rsmd.getColumnLabel(columnIdx);
                             values.put(columnLabel, convert(resultSet, columnIdx, rsmd));
                         } catch (SQLException ex) {
                             LOG.error(" Convert data [{}:{}] {} error ", tableName, columnLabel, ex.getMessage(), ex);
                         }
                     });
        } catch (SQLException ex) {
            LOG.error(" parse data metadata information exception", ex);
        }
        return values;
    }

    /**
     * fixedLenCharToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String fixedLenCharToString(ResultSet rs, String columnLabel) throws SQLException {
        return rs.getString(columnLabel);
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData rsmd = resultSet.getMetaData();
        String tableName = rsmd.getTableName(1);
        return putOneResultSetToMap(tableName, rsmd, resultSet, new TreeMap<>());
    }

    /**
     * convert
     *
     * @param resultSet resultSet
     * @param columnIdx columnIdx
     * @param rsmd      rsmd
     * @return result
     * @throws SQLException SQLException
     */
    protected abstract String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException;

    /**
     * CSV MODE : Zero padding at the end of floating-point decimals specific to CSV mode
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @param scale       scale
     * @return data of string
     * @throws SQLException SQLException
     */
    protected String floatingPointNumberToString(@NonNull ResultSet resultSet, String columnLabel, Integer scale)
        throws SQLException {
        BigDecimal bigDecimal = resultSet.getBigDecimal(columnLabel);
        if (Objects.isNull(bigDecimal)) {
            return NULL;
        }
        return getDecimalFormat(scale).format(bigDecimal.doubleValue());
    }

    private static DecimalFormat getDecimalFormat(Integer scale) {
        DecimalFormat scaleFormatter;
        if (decimalFormatCache.containsKey(scale)) {
            scaleFormatter = decimalFormatCache.get(scale);
        } else {
            String pattern;
            if (scale == 0) {
                pattern = DECIMAL_APPEND_ZERO;
            } else {
                pattern = DECIMAL_FORMAT_PATTERN_START + DECIMAL_APPEND_ZERO.repeat(Math.max(0, scale));
            }
            scaleFormatter = new DecimalFormat(pattern);
            decimalFormatCache.put(scale, scaleFormatter);
        }
        return scaleFormatter;
    }

    /**
     * float and double data type translate to string,we must be use bigDecimal get the data,
     * and use toString format the data to a string.
     *
     * @param resultSet   rs
     * @param columnLabel columnLabel
     * @return format
     * @throws SQLException SQLException
     */
    protected String floatingPointNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        BigDecimal bigDecimal = resultSet.getBigDecimal(columnLabel);
        if (Objects.isNull(bigDecimal)) {
            return NULL;
        }
        String value = bigDecimal.toString();
        if (isScientificNotation(value)) {
            return new BigDecimal(value).toPlainString();
        }
        return value;
    }

    /**
     * numericFloatNumberToString
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String numericFloatNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        BigDecimal floatValue = resultSet.getBigDecimal(columnLabel);
        if (resultSet.wasNull()) {
            return NULL;
        }
        String value = String.valueOf(floatValue.doubleValue());
        if (isScientificNotation(value)) {
            return new BigDecimal(value).toPlainString();
        }
        return value;
    }

    /**
     * doubleNumberToString
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String doubleNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        double floatValue = resultSet.getDouble(columnLabel);
        if (resultSet.wasNull()) {
            return NULL;
        }
        String value = String.valueOf(floatValue);
        if (isScientificNotation(value)) {
            return new BigDecimal(value).toPlainString();
        }
        return value;
    }

    /**
     * isScientificNotation
     *
     * @param value value
     * @return boolean
     */
    protected boolean isScientificNotation(String value) {
        return value.contains("E") || value.contains("e");
    }

    /**
     * numeric0ToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String numeric0ToString(ResultSet rs, String columnLabel) throws SQLException {
        BigDecimal bigDecimal = rs.getBigDecimal(columnLabel);
        return Objects.isNull(bigDecimal) ? NULL : bigDecimal.toBigInteger()
                                                             .toString();
    }

    /**
     * getDateFormat
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String getDateFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? DATE.format(date.toLocalDate()) : NULL;
    }

    /**
     * getTimeFormat
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String getTimeFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Time time = resultSet.getTime(columnLabel);
        return Objects.nonNull(time) ? TIME.format(time.toLocalTime()) : NULL;
    }

    /**
     * getTimestampFormat
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String getTimestampFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Timestamp timestamp =
            resultSet.getTimestamp(columnLabel, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        return Objects.nonNull(timestamp) ? formatTimestamp(timestamp) : NULL;
    }

    private String formatTimestamp(@NonNull Timestamp timestamp) {
        return timestamp.getNanos() > 0 ? TIMESTAMP_NANOS.format(timestamp.toLocalDateTime()) :
            TIMESTAMP.format(timestamp.toLocalDateTime());
    }

    /**
     * getYearFormat
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String getYearFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? YEAR.format(date.toLocalDate()) : NULL;
    }

    /**
     * bytesToString
     *
     * @param bytes bytes
     * @return result
     */
    protected String bytesToString(byte[] bytes) {
        return HexUtil.byteToHex(bytes);
    }

    /**
     * isNumericFloat
     *
     * @param precision precision
     * @param scale     scale
     * @return boolean
     */
    public static boolean isNumericFloat(int precision, int scale) {
        return precision > NUMERIC_PRECISION_0 && scale > NUMERIC_SCALE_0;
    }

    /**
     * isNumeric0
     *
     * @param precision precision
     * @param scale     scale
     * @return boolean
     */
    public static boolean isNumeric0(int precision, int scale) {
        return precision > NUMERIC_PRECISION_0 && scale == NUMERIC_SCALE_0;
    }

    /**
     * if dataType is numeric ,then check precision and scale is zero.
     *
     * @param precision precision
     * @param scale     scale
     * @return boolean
     */
    public static boolean isNumericDefault(int precision, int scale) {
        return precision == NUMERIC_PRECISION_0 && scale == NUMERIC_SCALE_0;
    }

    @FunctionalInterface
    interface TypeHandler {
        /**
         * result convert to string
         *
         * @param resultSet   resultSet
         * @param columnLabel columnLabel
         * @return result result
         * @throws SQLException SQLException
         */
        String convert(ResultSet resultSet, String columnLabel) throws SQLException;
    }
}