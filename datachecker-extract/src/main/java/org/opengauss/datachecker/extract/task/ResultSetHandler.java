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

import java.math.BigDecimal;
import java.sql.Blob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
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
    private static final Logger log = LogUtils.getLogger();
    protected static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    protected static final DateTimeFormatter YEAR = DateTimeFormatter.ofPattern("yyyy");
    protected static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    protected static final DateTimeFormatter TIMESTAMP_NANOS =
        DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSSSSS");
    protected static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
    protected static final String EMPTY = "";
    protected static final String NULL = null;
    protected static final String FLOATING_POINT_NUMBER_ZERO = "0.0";

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param rsmd      JDBC Data query result set
     * @param resultSet JDBC Data query result set
     * @param values    values
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(ResultSetMetaData rsmd, ResultSet resultSet,
        Map<String, String> values) {
        try {
            IntStream.rangeClosed(1, rsmd.getColumnCount())
                    .forEach(columnIdx -> {
                        String columnLabel = null;
                        String tableName = null;
                        try {
                            columnLabel = rsmd.getColumnLabel(columnIdx);
                            tableName = rsmd.getTableName(columnIdx);
                            values.put(columnLabel, convert(resultSet, rsmd.getColumnTypeName(columnIdx), columnLabel,
                                    rsmd.getColumnDisplaySize(columnIdx)));
                        } catch (SQLException ex) {
                            log.error("putOneResultSetToMap Convert data [{}:{}] {} error ", tableName, columnLabel,
                                    ex.getMessage(), ex);
                        }
                    });
        } catch (SQLException ex) {
            log.error("putOneResultSetToMap get data metadata information exception", ex);
        }
        return values;
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData rsmd = resultSet.getMetaData();
        return putOneResultSetToMap(rsmd, resultSet, new TreeMap<>());
    }

    protected abstract String convert(ResultSet resultSet, String columnTypeName, String columnLabel, int displaySize)
        throws SQLException;

    protected String floatingPointNumberToString(@NonNull ResultSet resultSet, String columnLabel) throws SQLException {
        BigDecimal bigDecimal = resultSet.getBigDecimal(columnLabel);
        return resultSet.wasNull() ? FLOATING_POINT_NUMBER_ZERO :
            Objects.isNull(bigDecimal) ? FLOATING_POINT_NUMBER_ZERO : Double.toString(bigDecimal.doubleValue());
    }

    protected String getDateFormat(@NonNull ResultSet resultSet, String columnLabel, int displaySize)
        throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? DATE.format(date.toLocalDate()) : NULL;
    }

    protected String getTimeFormat(@NonNull ResultSet resultSet, String columnLabel, int displaySize)
        throws SQLException {
        final Time time = resultSet.getTime(columnLabel);
        return Objects.nonNull(time) ? TIME.format(time.toLocalTime()) : NULL;
    }

    protected String getTimestampFormat(@NonNull ResultSet resultSet, String columnLabel, int displaySize)
        throws SQLException {
        final Timestamp timestamp =
            resultSet.getTimestamp(columnLabel, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        return Objects.nonNull(timestamp) ? formatTimestamp(timestamp) : NULL;
    }

    private String formatTimestamp(@NonNull Timestamp timestamp) {
        return timestamp.getNanos() > 0 ? TIMESTAMP_NANOS.format(timestamp.toLocalDateTime()) :
            TIMESTAMP.format(timestamp.toLocalDateTime());
    }

    protected String getYearFormat(@NonNull ResultSet resultSet, String columnLabel, int displaySize)
        throws SQLException {
        final Date date = resultSet.getDate(columnLabel);
        return Objects.nonNull(date) ? YEAR.format(date.toLocalDate()) : NULL;
    }

    protected String blobToString(Blob blob) throws SQLException {
        if (Objects.isNull(blob)) {
            return NULL;
        }
        return new String(blob.getBytes(1, (int) blob.length()));
    }

    protected String bytesToString(byte[] bytes) {
        return HexUtil.byteToHex(bytes);
    }

    protected String trim(@NonNull ResultSet resultSet, String columnLabel, int displaySize) throws SQLException {
        final String string = resultSet.getString(columnLabel);
        return string == null ? NULL : string.stripTrailing();
    }

    @FunctionalInterface
    interface TypeHandler {
        /**
         * result convert to string
         *
         * @param resultSet   resultSet
         * @param columnLabel columnLabel
         * @param displaySize displaySize
         * @return result
         * @throws SQLException SQLException
         */
        String convert(ResultSet resultSet, String columnLabel, int displaySize) throws SQLException;
    }
}