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
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;

/**
 * MetaDataCache
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@Component
public class MetaDataCache {
    private static final Logger log = LogUtils.getLogger();
    private static Map<String, TableMetadata> TABLE_CACHE = new ConcurrentHashMap<>();

    /**
     * Save to the Cache k v
     *
     * @param key   Metadata key
     * @param value Metadata value of table
     */
    public static void put(@NonNull String key, TableMetadata value) {
        try {
            TABLE_CACHE.put(key, value);
        } catch (Exception exception) {
            log.error("put in cache exception ", exception);
        }
    }

    public static Map<String, TableMetadata> getAll() {
        try {
            return TABLE_CACHE;
        } catch (Exception exception) {
            log.error("put in cache exception ", exception);
        }
        return new HashMap<>();
    }

    public static boolean isEmpty() {
        return TABLE_CACHE == null || TABLE_CACHE.size() == 0;
    }

    /**
     * Batch storage  to the cache
     *
     * @param map map of key,value ,that have some table metadata
     */
    public static void putMap(@NonNull Map<String, TableMetadata> map) {
        try {
            TABLE_CACHE.putAll(map);
        } catch (Exception exception) {
            log.error("batch storage cache exception", exception);
        }
    }

    /**
     * get cache
     *
     * @param key table name as cached key
     */
    public static TableMetadata get(String key) {
        try {
            return TABLE_CACHE.get(key);
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
        }
    }

    public static void updateRowCount(String key, long rowCount) {
        try {
            if (TABLE_CACHE.containsKey(key)) {
                TABLE_CACHE.get(key).setTableRows(rowCount);
            }
        } catch (Exception exception) {
            log.error("update cache exception", exception);
        }
    }

    /**
     * Check whether the specified key is in the cache
     *
     * @param key table name as cached key
     * @return result
     */
    public static boolean containsKey(String key) {
        try {
            return TABLE_CACHE.containsKey(key);
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return false;
        }
    }

    /**
     * Obtains all cached key sets
     *
     * @return keys cached key sets
     */
    public static Set<String> getAllKeys() {
        try {
            return TABLE_CACHE.keySet();
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
        }
    }

    /**
     * Clear all cache information
     */
    public static void removeAll() {
        if (Objects.nonNull(TABLE_CACHE) && TABLE_CACHE.size() > 0) {
            log.info("clear cache information");
            TABLE_CACHE.clear();
        }
    }

    /**
     * Specify cache information clearly based on key values
     *
     * @param key key values of the cache to be cleared
     */
    public static void remove(String key) {
        if (Objects.nonNull(TABLE_CACHE) && TABLE_CACHE.size() > 0) {
            log.info("clear cache information");
            TABLE_CACHE.remove(key);
        }
    }
}
