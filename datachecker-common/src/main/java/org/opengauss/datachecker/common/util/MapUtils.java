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

package org.opengauss.datachecker.common.util;

import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;

/**
 * MapUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
public class MapUtils {

    /**
     * add key,vObject in map.
     *
     * @param map     map
     * @param key     map key
     * @param vObject one of linkedList
     * @param <K>     type of Map key
     * @param <T>     type of Map LinkedList
     */
    public static <K, T> void put(Map<K, List<T>> map, K key, T vObject) {
        map.compute(key, (k, value) -> {
            if (value == null) {
                value = new LinkedList<>();
            }
            value.add(vObject);
            return value;
        });
    }

    /**
     * remove from map.entry.values
     *
     * @param map     map
     * @param key     key
     * @param vObject remove Value
     * @param <K>     type of map key
     * @param <T>     type of map LinkedList
     */
    public static <K, T> void remove(Map<K, List<T>> map, K key, T vObject) {
        if (map.containsKey(key)) {
            List<T> list = map.get(key);
            list.remove(vObject);
            if (list.isEmpty()) {
                map.remove(key);
            }
        }
    }

    /**
     * add value in map.entry.value map
     *
     * @param map    map
     * @param key    key
     * @param vKey   value of HashMap key
     * @param vValue value of HashMap value
     * @param <K>    type of key
     * @param <S>    type of value map Key
     * @param <T>    type of value map value
     */
    public static <K, S, T> void put(Map<K, Map<S, T>> map, K key, S vKey, T vValue) {
        map.compute(key, (valueKey, vMap) -> {
            if (vMap == null) {
                vMap = new HashMap<>();
            }
            vMap.put(vKey, vValue);
            return vMap;
        });
    }

    /**
     * get value from map
     *
     * @param map  map
     * @param key  key
     * @param vKey map value key
     * @param <K>  type of key
     * @param <S>  type of value map Key
     * @param <T>  type of value map value
     * @return map value
     */
    public static <K, S, T> T get(Map<K, Map<S, T>> map, K key, S vKey) {
        if (map.containsKey(key)) {
            return map.get(key).get(vKey);
        } else {
            return null;
        }
    }
}
