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

package org.opengauss.datachecker.extract.cache;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.Vector;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.IntStream;

/**
 * <pre>
 * Table data extraction status {@code tableExtractStatusMap}
 * Cache structure definition {@code Map<K,Map<T, V>>} within：
 * K - Name of the table corresponding to data extraction
 * T - Sequence number of the current table data extraction and sharding task
 * V - Completion status of the sharding task for extracting data from the current table.
 *     When the value is 0,it indicates that the task is not completed，
 *    other the value is 1,it indicates that the task is completed. the default value is 0.
 * </pre>
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
public class TableExtractStatusCache {
    private static final Logger log = LogUtils.getLogger();
    /**
     * data extraction task completion status
     */
    private static final Byte STATUS_COMPLATE = 1;
    /**
     * Initialization status of a data extraction task
     */
    private static final Byte STATUS_INIT = 0;
    /**
     * Task sequence number. start sequence number
     */
    private static final int TASK_ORDINAL_START_INDEX = 1;
    /**
     * Table data extraction status cache : {@code Map<String,  Map<Integer, Byte>>}
     */
    private static final Map<String, Map<Integer, Byte>> TABLE_EXTRACT_STATUS_MAP = new ConcurrentHashMap<>();

    private static final Vector<String> ERROR_LIST = new Vector<>();

    /**
     * Table data extraction task status initialization. {code map} is a set of table decomposition tasks.
     *
     * @param map Initial value of the execution status of the table extraction task.
     */
    public static void init(Map<String, Integer> map) {
        Assert.isTrue(Objects.nonNull(map), Message.INIT_STATUS_PARAM_EMPTY);
        map.forEach((table, taskCount) -> {
            Map<Integer, Byte> tableStatus = new ConcurrentHashMap<>(InitialCapacity.CAPACITY_16);
            IntStream.rangeClosed(TASK_ORDINAL_START_INDEX, taskCount).forEach(ordinal -> {
                tableStatus.put(ordinal, STATUS_INIT);
            });
            TABLE_EXTRACT_STATUS_MAP.put(table, tableStatus);
        });
        log.info(Message.INIT_STATUS);
    }

    /**
     * Updates the execution status of a task in a specified table.
     *
     * @param tableName table name
     * @param ordinal   Sequence number of a table data extraction and decomposition task.
     */
    public static synchronized void update(@NonNull String tableName, Integer ordinal) {
        try {
            // the table must exist.
            Assert.isTrue(TABLE_EXTRACT_STATUS_MAP.containsKey(tableName),
                String.format(Message.TABLE_STATUS_NOT_EXIST, tableName));

            // Obtain the status information corresponding to the current table
            // and verify the validity of the task status parameters to be updated.
            Map<Integer, Byte> tableStatus = TABLE_EXTRACT_STATUS_MAP.get(tableName);
            Assert.isTrue(tableStatus.containsKey(ordinal),
                String.format(Message.TABLE_ORDINAL_NOT_EXIST, tableName, ordinal));

            // update status
            tableStatus.put(ordinal, STATUS_COMPLATE);
            log.info("update tableName : {}, ordinal : {} extract completed-status {}", tableName, ordinal,
                STATUS_COMPLATE);
        } catch (Exception exception) {
            log.error(Message.UPDATE_STATUS_EXCEPTION, exception);
        }
    }

    /**
     * Mark current table data extraction exception
     *
     * @param tableName tableName
     */
    public static void addErrorList(String tableName) {
        ERROR_LIST.add(tableName);
    }

    /**
     * Check whether the current table is marked as data extraction exception table
     *
     * @param tableName tableName
     * @return If true is returned, it indicates that the current table has a task occurred an exceptio
     */
    public static boolean hasErrorOccurred(String tableName) {
        return ERROR_LIST.contains(tableName);
    }

    /**
     * data extraction status cache message management
     */
    interface Message {
        /**
         * data extraction status message ：table not exist
         */
        String TABLE_STATUS_NOT_EXIST = "The status information of the current table {%s} does not exist. "
            + "Please initialize it and update it again.";
        /**
         * data extraction status message ：table ordinal not exist
         */
        String TABLE_ORDINAL_NOT_EXIST = "The current table {%s} sequence {%s} task status information does not exist."
            + " Please initialize it and update it again.";
        /**
         * data extraction status message ：update table status exception
         */
        String UPDATE_STATUS_EXCEPTION = "Failed to update the task status of the specified table.";
        /**
         * data extraction status message ：Initializing the data extraction task status
         */
        String INIT_STATUS = "Initializing the data extraction task status.";
        /**
         * data extraction status message ：initialization parameter of extraction task status  cannot be empty
         */
        String INIT_STATUS_PARAM_EMPTY =
            "The initialization parameter of the data extraction task status cannot be empty.";
    }

    /**
     * Check whether the execution status of all tasks corresponding to the current table is complete.
     * if true  is returned ,all task are completed.
     * if false is returned ,at least one task is not completed.
     *
     * @param tableName table name
     */
    public static boolean checkCompleted(@NonNull String tableName) {
        // check whether the table name exists.
        Assert.isTrue(TABLE_EXTRACT_STATUS_MAP.containsKey(tableName),
            String.format(Message.TABLE_STATUS_NOT_EXIST, tableName));
        return !TABLE_EXTRACT_STATUS_MAP.get(tableName).containsValue(STATUS_INIT);
    }

    /**
     * <p>
     * Check whether all tasks whose sequence number is less than {@code ordinal}
     * in the current table {@code tableName} are complete.
     * if true  is returned , all tasks whose sequence number is less than {@code ordinal}
     * in the current table {@code tableName} have been completed.
     * if false is returned ,at least one task whose sequence number is less than {@code ordinal}
     * in the current table {@code tableName} is not completed.
     * </p>
     *
     * @param tableName table name
     * @param ordinal   sequence number of a table splitting task.
     * @return
     */
    public static boolean checkCompleted(@NonNull String tableName, int ordinal) {
        // check whether the table name exists.
        Assert.isTrue(TABLE_EXTRACT_STATUS_MAP.containsKey(tableName),
            String.format(Message.TABLE_STATUS_NOT_EXIST, tableName));
        Map<Integer, Byte> tableStatus = TABLE_EXTRACT_STATUS_MAP.get(tableName);
        long noCompleted = IntStream.range(TASK_ORDINAL_START_INDEX, ordinal)
                                    .filter(idx -> Objects.equals(tableStatus.get(idx), STATUS_INIT)).count();
        log.info("tableName : {}, ordinal : {} check noCompleted=[{}]", tableName, ordinal, noCompleted);
        return noCompleted == 0;
    }

    /**
     * deleting the table extraction status.
     *
     * @param key table name
     */
    public static void remove(@NonNull String key) {
        try {
            TABLE_EXTRACT_STATUS_MAP.remove(key);
        } catch (Exception exception) {
            log.error("failed to delete the cache,", exception);
        }
    }

    /**
     * 删除表抽取状态
     */
    public static void removeAll() {
        try {
            TABLE_EXTRACT_STATUS_MAP.clear();
        } catch (Exception exception) {
            log.error("failed to delete the cache,", exception);
        }
    }

    /**
     * extract table
     *
     * @return extract table
     */
    public static Set<String> getAllKeys() {
        try {
            return TABLE_EXTRACT_STATUS_MAP.keySet();
        } catch (Exception exception) {
            log.error("failed to obtain the cache,", exception);
            return null;
        }
    }
}
