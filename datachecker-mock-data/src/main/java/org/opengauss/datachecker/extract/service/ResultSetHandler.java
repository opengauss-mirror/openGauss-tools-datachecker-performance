/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.databind.DeserializationFeature;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.SerializationFeature;
import com.fasterxml.jackson.datatype.jsr310.JavaTimeModule;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.springframework.lang.NonNull;

import java.sql.Date;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Time;
import java.sql.Timestamp;
import java.sql.Types;
import java.text.SimpleDateFormat;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TimeZone;
import java.util.stream.IntStream;

/**
 * Result set object processor
 *
 * @author wang chao
 * @date ：Created in 2022/6/13
 * @since 11
 **/
@Slf4j
public class ResultSetHandler {
    private static final ObjectMapper MAPPER = ObjectMapperWapper.getObjectMapper();
    private static final List<Integer> SQL_TIME_TYPES =
        List.of(Types.DATE, Types.TIME, Types.TIMESTAMP, Types.TIME_WITH_TIMEZONE, Types.TIMESTAMP_WITH_TIMEZONE);
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatter TIME = DateTimeFormatter.ofPattern("HH:mm:ss");
    private static final DateTimeFormatter TIMESTAMP = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss.SSS");

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param resultSet JDBC Data query result set
     * @param rsmd      JDBC ResultSet Metadata
     * @return JDBC Data encapsulation results
     * @throws SQLException Return SQL exception
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet, ResultSetMetaData rsmd) throws SQLException {
        Map<String, String> values = new HashMap<>(InitialCapacity.CAPACITY_16);
        IntStream.range(0, rsmd.getColumnCount()).forEach(idx -> {
            try {
                int columnIdx = idx + 1;
                String columnLabel = rsmd.getColumnLabel(columnIdx);
                Object columnValue;
                final int columnType = rsmd.getColumnType(columnIdx);
                if (SQL_TIME_TYPES.contains(columnType)) {
                    columnValue = timeHandler(resultSet, columnIdx, columnType);
                } else {
                    columnValue = resultSet.getObject(columnLabel);
                }
                values.put(columnLabel, MAPPER.convertValue(columnValue, String.class));
            } catch (SQLException ex) {
                log.error("putOneResultSetToMap Convert data according to result set metadata information."
                    + " Result set exception {}", ex.getMessage());
            }
        });
        return values;
    }

    private String timeHandler(ResultSet resultSet, int columnIdx, int columnType) throws SQLException {
        String format = StringUtils.EMPTY;
        switch (columnType) {
            case Types.DATE:
                format = getDateFormat(resultSet, columnIdx);
                break;
            case Types.TIME:
            case Types.TIME_WITH_TIMEZONE:
                format = getTimeFormat(resultSet, columnIdx);
                break;
            case Types.TIMESTAMP:
            case Types.TIMESTAMP_WITH_TIMEZONE:
                format = getTimestampFormat(resultSet, columnIdx);
                break;
            default:
        }
        return format;
    }

    private String getDateFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Date date = resultSet.getDate(columnIdx);
        if (Objects.nonNull(date)) {
            formatTime = DATE.format(date.toLocalDate());
        }
        return formatTime;
    }

    private String getTimeFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Time time = resultSet.getTime(columnIdx);
        if (Objects.nonNull(time)) {
            formatTime = TIME.format(time.toLocalTime());
        }
        return formatTime;
    }

    private String getTimestampFormat(@NonNull ResultSet resultSet, int columnIdx) throws SQLException {
        String formatTime = StringUtils.EMPTY;
        final Timestamp timestamp =
            resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
        if (Objects.nonNull(timestamp)) {
            formatTime = TIMESTAMP.format(timestamp.toLocalDateTime());
        }
        return formatTime;
    }

    /**
     * The result set object processor converts the result set data into JSON strings
     */
    static class ObjectMapperWapper {
        private static final ObjectMapper MAPPER;

        static {
            MAPPER = new ObjectMapper();
            MAPPER.enable(SerializationFeature.INDENT_OUTPUT);
            MAPPER.setSerializationInclusion(JsonInclude.Include.ALWAYS);
            MAPPER.configure(DeserializationFeature.FAIL_ON_UNKNOWN_PROPERTIES, false);
            MAPPER.configure(SerializationFeature.FAIL_ON_EMPTY_BEANS, false);
            MAPPER.configure(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS, false);
            MAPPER.setDateFormat(new SimpleDateFormat("yyyy-MM-dd HH:mm:ss"));
            MAPPER.disable(SerializationFeature.WRITE_DATES_AS_TIMESTAMPS);
            MAPPER.registerModule(new JavaTimeModule());
        }

        /**
         * ObjectMapper
         *
         * @return ObjectMapper
         */
        public static ObjectMapper getObjectMapper() {
            return MAPPER;
        }
    }
}