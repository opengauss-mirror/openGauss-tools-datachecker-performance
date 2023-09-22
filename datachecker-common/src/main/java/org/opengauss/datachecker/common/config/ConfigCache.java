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

package org.opengauss.datachecker.common.config;

import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.io.File;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;

import static org.opengauss.datachecker.common.constant.ConfigConstants.CHECK_PATH;

/**
 * CsvConfigCache
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/31
 * @since ：11
 */
public class ConfigCache {
    private static final Map<String, Object> CACHE = new HashMap<>();

    /**
     * add config key value
     *
     * @param key   config key
     * @param value config value
     */
    public static void put(String key, Object value) {
        CACHE.put(key, value);
    }

    /**
     * add config of checkMode
     *
     * @param checkMode check Mode
     */
    public static void setCheckMode(CheckMode checkMode) {
        CACHE.put(ConfigConstants.CHECK_MODE, checkMode);
    }

    /**
     * get config CheckMode
     *
     * @return check mode
     */
    public static CheckMode getCheckMode() {
        return getValue(ConfigConstants.CHECK_MODE, CheckMode.class);
    }

    /**
     * get config from cache
     *
     * @param key    config key
     * @param classz config value of type
     * @param <T>    value type
     * @return value
     */
    public static <T> T getValue(String key, Class<T> classz) {
        try {
            Object value = CACHE.get(key);
            if (value == null) {
                return null;
            }
            return (T) value;
        } catch (ClassCastException ex) {
            return null;
        }
    }

    /**
     * get current endpoint
     *
     * @return endpoint
     */
    public static Endpoint getEndPoint() {
        return getValue(ConfigConstants.ENDPOINT, Endpoint.class);
    }

    /**
     * get config key when value type is String
     *
     * @param key config key
     * @return config value
     */
    public static String getValue(String key) {
        return getValue(key, String.class);
    }

    /**
     * get config key when value type is Long
     *
     * @param key config key
     * @return config value
     */
    public static long getLongValue(String key) {
        Long value = getValue(key, Long.class);
        return Objects.isNull(value) ? 0L : value;
    }

    /**
     * get config key when value type is Integer
     *
     * @param key config key
     * @return config value
     */
    public static int getIntValue(String key) {
        Integer value = getValue(key, Integer.class);
        return Objects.isNull(value) ? 0 : value;
    }

    /**
     * get config key when value type is Boolean
     *
     * @param key config key
     * @return config value
     */
    public static Boolean getBooleanValue(String key) {
        return getValue(key, Boolean.class);
    }

    /**
     * get check result path
     *
     * @return check result path
     */
    public static String getCheckResult() {
        return getValue(CHECK_PATH, String.class) + File.separatorChar + "result" + File.separatorChar;
    }

    /**
     * get check database of schema
     *
     * @return check schema
     */
    public static String getSchema() {
        return getValue(ConfigConstants.CSV_SCHEMA);
    }

    /**
     * get check csv writer log path
     *
     * @return check csv writer log path
     */
    public static String getWriter() {
        return getValue(ConfigConstants.CSV_WRITER_PATH);
    }

    /**
     * get check csv reader log path
     *
     * @return reader log path
     */
    public static String getReader() {
        return getValue(ConfigConstants.CSV_READER_PATH);
    }

    /**
     * get check csv data dir path
     *
     * @return csv data dir path
     */
    public static String getCsvData() {
        return getValue(ConfigConstants.CSV_DATA_PATH);
    }

    /**
     * get csv log monitor interval milli seconds
     * default value 100 milli seconds
     *
     * @return milli seconds
     */
    public static Long getCsvLogMonitorInterval() {
        return getLongValue(ConfigConstants.CSV_SLEEP_INTERVAL);
    }

    /**
     * get csv metadata tables file path
     *
     * @return tables file path
     */
    public static Path getCsvMetadataTablesPath() {
        String tablePath = getValue(ConfigConstants.CSV_SCHEMA_TABLES_PATH);
        return Path.of(tablePath);
    }

    /**
     * get csv metadata columns file path
     *
     * @return columns file path
     */
    public static Path getCsvMetadataColumnsPath() {
        String columnsPath = getValue(ConfigConstants.CSV_SCHEMA_COLUMNS_PATH);
        return Path.of(columnsPath);
    }
}
