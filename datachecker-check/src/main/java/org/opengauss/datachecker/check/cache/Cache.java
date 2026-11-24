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

package org.opengauss.datachecker.check.cache;

import java.util.Set;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
public interface Cache<K, V> {

    /**
     * Initialize cache and set default values for key values
     *
     * @param keys Cache key value
     */
    void init(Set<K> keys);

    /**
     * Add key value pairs to cache
     *
     * @param key   key
     * @param value value
     */
    void put(K key, V value);

    /**
     * Query cache according to key
     *
     * @param key Cache key
     * @return Cache value
     */
    V get(K key);

    /**
     * Get cache key set
     *
     * @return Key set
     */
    Set<K> getKeys();

    /**
     * Update cached data
     *
     * @param key   key
     * @param value value
     * @return Updated cache value
     */
    V update(K key, V value);

    /**
     * Delete the specified key cache
     *
     * @param key key
     */
    void remove(K key);

    /**
     * Clear all caches
     */
    void removeAll();

    /**
     * The cache persistence interface will persist the cache information locally
     */
    void persistent();

    /**
     * The service starts to recover cached information. Recover historical data based on persistent cached data
     * Scan the cache file at the specified location, parse the JSON string, and deserialize the current cache data
     */
    void recover();
}
