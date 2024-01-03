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
import org.opengauss.datachecker.common.util.HexUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.lang.NonNull;

import java.io.IOException;
import java.math.BigDecimal;
import java.sql.Blob;
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
    private static final String decimal_format_pattern_start = "0.";
    private static final String decimal_append_zero = "0";

    protected static final Logger log = LogUtils.getLogger();
    protected static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    protected static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected static final DateTimeFormatter TIMESTAMP_NANOS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    protected static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected static final String EMPTY = "";
    protected static final String NULL = null;

    protected static final int NUMERIC_SCALE_F84 = -84;
    protected static final int NUMERIC_SCALE_0 = 0;
    protected static final int NUMERIC_PRECISION_0 = 0;
    protected final boolean supplyZero;

    public ResultSetHandler() {
        this.supplyZero = false;
    }

    public ResultSetHandler(Boolean supplyZero) {
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
                             log.error(" Convert data [{}:{}] {} error ", tableName, columnLabel, ex.getMessage(), ex);
                         }
                     });
        } catch (SQLException ex) {
            log.error(" parse data metadata information exception", ex);
        }
        return values;
    }

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

    protected abstract String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException;

    /**
     * CSV MODE : Zero padding at the end of floating-point decimals specific to CSV mode
     *
     * @param resultSet   resultSet
     * @param columnLabel columnLabel
     * @param scale       scale
     * @return data of string
     * @throws SQLException
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
                pattern = decimal_append_zero;
            } else {
                pattern = decimal_format_pattern_start + decimal_append_zero.repeat(Math.max(0, scale));
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
     * @throws SQLException
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

    protected String floatNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        float floatValue = resultSet.getFloat(columnLabel);
        if (resultSet.wasNull()) {
            return NULL;
        }
        String value = String.valueOf(floatValue);
        if (isScientificNotation(value)) {
            return new BigDecimal(value).toPlainString();
        }
        return value;
    }

    private boolean isScientificNotation(String value) {
        return value.contains("E") || value.contains("e");
    }

    protected String numeric0ToString(ResultSet rs, String columnLabel) throws SQLException {
        BigDecimal bigDecimal = rs.getBigDecimal(columnLabel);
        return Objects.isNull(bigDecimal) ? NULL : bigDecimal.toBigInteger()
                                                             .toString();
    }

    protected String getDateFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? DATE.format(date.toLocalDate()) : NULL;
    }

    protected String getTimeFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Time time = resultSet.getTime(columnLabel);
        return Objects.nonNull(time) ? TIME.format(time.toLocalTime()) : NULL;
    }

    protected String getTimestampFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Timestamp timestamp =
            resultSet.getTimestamp(columnLabel, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        return Objects.nonNull(timestamp) ? formatTimestamp(timestamp) : NULL;
    }

    private String formatTimestamp(@NonNull Timestamp timestamp) {
        return timestamp.getNanos() > 0 ? TIMESTAMP_NANOS.format(timestamp.toLocalDateTime()) :
            TIMESTAMP.format(timestamp.toLocalDateTime());
    }

    protected String getYearFormat(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? YEAR.format(date.toLocalDate()) : NULL;
    }

    protected String blobToString(Blob blob) throws SQLException, IOException {
        if (Objects.isNull(blob)) {
            return NULL;
        }
        return new String(blob.getBytes(1, (int) blob.length()));
    }

    protected String bytesToString(byte[] bytes) {
        return HexUtil.byteToHex(bytes);
    }

    protected String trim(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        final String string = resultSet.getString(columnLabel);
        return string == null ? NULL : string.stripTrailing();
    }

    public static boolean isNumericFloat(int precision, int scale) {
        return precision > NUMERIC_PRECISION_0 && scale > NUMERIC_SCALE_0;
    }

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
         * @return result
         * @throws SQLException SQLException
         */
        String convert(ResultSet resultSet, String columnLabel) throws SQLException;
    }
}