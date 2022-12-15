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

import com.google.common.cache.CacheBuilder;
import com.google.common.cache.CacheLoader;
import com.google.common.cache.LoadingCache;
import com.google.common.cache.RemovalListener;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.SpringUtil;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.Set;

/**
 * MetaDataCache
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@Slf4j
@Component
public class MetaDataCache {
    private static LoadingCache<String, TableMetadata> CACHE = null;

    /**
     * Initializing the Metadata Cache Method
     */
    public static void initCache() {
        if (CACHE == null) {
            CACHE = CacheBuilder.newBuilder()
                                //Set the concurrent read/write level based on the number of CPU cores;
                                .concurrencyLevel(1)
                                // Size of the buffer pool
                                .maximumSize(Integer.MAX_VALUE)
                                // Removing a Listener
                                .removalListener((RemovalListener<String, TableMetadata>) remove -> log
                                    .debug("cache: [{}], removed", remove.getKey())).recordStats().build(
                    // Method of handing a Key that does not exist
                    new CacheLoader<>() {
                        @Override
                        public TableMetadata load(String tableName) {
                            log.info("cache: [{}], does not exist", tableName);
                            MetaDataService metaDataService = SpringUtil.getBean(MetaDataService.class);
                            return metaDataService.queryMetaDataOfSchema(tableName);
                        }
                    });
        }
        log.info("initialize table meta data cache");
    }

    /**
     * Save to the Cache k v
     *
     * @param key   Metadata key
     * @param value Metadata value of table
     */
    public static void put(@NonNull String key, TableMetadata value) {
        try {
            CACHE.put(key, value);
        } catch (Exception exception) {
            log.error("put in cache exception ", exception);
        }
    }

    public static Map<String, TableMetadata> getAll() {
        try {
            if (CACHE == null) {
                initCache();
            }
            return CACHE.asMap();
        } catch (Exception exception) {
            log.error("put in cache exception ", exception);
        }
        return new HashMap<>();
    }

    public static boolean isEmpty() {
        return CACHE == null || CACHE.size() == 0;
    }

    /**
     * Batch storage  to the cache
     *
     * @param map map of key,value ,that have some table metadata
     */
    public static void putMap(@NonNull Map<String, TableMetadata> map) {
        try {
            CACHE.putAll(map);
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
            return CACHE.get(key);
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
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
            return CACHE.asMap().containsKey(key);
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
            return getAll().keySet();
        } catch (Exception exception) {
            log.error("get cache exception", exception);
            return null;
        }
    }

    /**
     * Clear all cache information
     */
    public static void removeAll() {
        if (Objects.nonNull(CACHE) && CACHE.size() > 0) {
            log.info("clear cache information");
            CACHE.cleanUp();
        }
    }

    /**
     * Specify cache information clearly based on key values
     *
     * @param key key values of the cache to be cleared
     */
    public static void remove(String key) {
        if (Objects.nonNull(CACHE) && CACHE.size() > 0) {
            log.info("clear cache information");
            CACHE.asMap().remove(key);
        }
    }
}
