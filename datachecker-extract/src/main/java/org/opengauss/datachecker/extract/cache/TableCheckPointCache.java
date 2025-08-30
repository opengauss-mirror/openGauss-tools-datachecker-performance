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

import org.opengauss.datachecker.common.entry.common.PointPair;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * TableCheckPointCache
 *
 * @author ：lvlintao
 * @date ：Created in 2023/10/28
 * @since ：11
 */
@Component
public class TableCheckPointCache {
    private static final Map<String, List<PointPair>> TABLE_CHECKPOINT_CACHE = new ConcurrentHashMap<>();

    private int tableSize;

    /**
     * Save to the Cache k v
     *
     * @param key Metadata key
     * @param value Metadata value of table
     */
    public void put(@NonNull String key, List<PointPair> value) {
        count(key);
        TABLE_CHECKPOINT_CACHE.put(key, value);
    }

    private void count(String key) {
        if (!TABLE_CHECKPOINT_CACHE.containsKey(key)) {
            tableSize++;
        }
    }

    /**
     * remove table check point cache
     *
     * @param key key
     */
    public void remove(String key) {
        TABLE_CHECKPOINT_CACHE.remove(key);
    }

    /**
     * add point in cache
     *
     * @param key key
     * @param value value
     */
    public void add(@NonNull String key, List<PointPair> value) {
        count(key);
        if (TABLE_CHECKPOINT_CACHE.containsKey(key)) {
            TABLE_CHECKPOINT_CACHE.get(key).addAll(value);
        } else {
            TABLE_CHECKPOINT_CACHE.put(key, value);
        }
    }

    /**
     * contains key
     *
     * @param key key
     * @return boolean
     */
    public boolean contains(String key) {
        return TABLE_CHECKPOINT_CACHE.containsKey(key);
    }

    public int tableCount() {
        return tableSize;
    }

    /**
     * clean table check point cache
     */
    public void clean() {
        tableSize = 0;
        TABLE_CHECKPOINT_CACHE.clear();
    }

    /**
     * get cache
     *
     * @param key table name as cached key
     * @return list the checkPoint list
     */
    public List<PointPair> get(String key) {
        return TABLE_CHECKPOINT_CACHE.get(key);
    }
}
