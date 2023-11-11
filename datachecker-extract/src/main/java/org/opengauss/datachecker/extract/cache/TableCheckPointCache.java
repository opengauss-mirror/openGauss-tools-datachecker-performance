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
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Component;

import java.util.ArrayList;
import java.util.HashMap;
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
    private static final Logger log = LogUtils.getLogger();
    private static final Map<String, List<Object>> TABLE_CHECKPOINT_CACHE = new ConcurrentHashMap<>();

    /**
     * Save to the Cache k v
     *
     * @param key   Metadata key
     * @param value Metadata value of table
     */
    public void put(@NonNull String key, List<Object> value) {
        try {
            TABLE_CHECKPOINT_CACHE.put(key, value);
        } catch (NumberFormatException exception) {
            log.error("put in cache exception ", exception);
        }
    }

    /**
     * Get all table checkPointList relationship
     *
     * @return map table checkPointList map
     */
    public Map<String, List<Object>> getAll() {
        try {
            return TABLE_CHECKPOINT_CACHE;
        } catch (NumberFormatException exception) {
            log.error("put in cache exception ", exception);
        }
        return new HashMap<>();
    }

    /**
     * get cache
     *
     * @param key table name as cached key
     * @return list the checkPoint list
     */
    public List<Object> get(String key) {
        try {
            return TABLE_CHECKPOINT_CACHE.get(key);
        } catch (NumberFormatException exception) {
            log.error("get cache exception", exception);
            return new ArrayList<>();
        }
    }
}
