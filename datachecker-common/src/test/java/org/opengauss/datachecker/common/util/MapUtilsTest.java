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

package org.opengauss.datachecker.common.util;

import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.SliceVo;

import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

class MapUtilsTest {

    @Test
    void testPut() {
        // Setup
        Map<String, List<SliceVo>> map = new ConcurrentHashMap<>();
        SliceVo vObject = null;

        for (int i = 0; i < 10; i++) {
            vObject = new SliceVo();
            MapUtils.put(map, "key", vObject);
        }
        for (int i = 0; i < 10; i++) {
            vObject = new SliceVo();
            MapUtils.put(map, "key2", vObject);
        }
        for (int i = 0; i < 10; i++) {
            vObject = new SliceVo();
            MapUtils.put(map, "table1", vObject);
        }

        String table1 = "table1";
        if (map.containsKey(table1)) {
            List<SliceVo> unprocessedSlices = map.get(table1);
            List<SliceVo> processedSlices = new LinkedList<>();
            unprocessedSlices.forEach(unprocessed -> {
                processedSlices.add(unprocessed);
            });
            processedSlices.forEach(processed -> {
                MapUtils.remove(map, table1, processed);
            });
        }

        map.forEach((table, unprocessedSlices) -> {
            List<SliceVo> processedSlices = new LinkedList<>();
            unprocessedSlices.forEach(unprocessed -> {
                processedSlices.add(unprocessed);
            });
            processedSlices.forEach(processed -> {
                MapUtils.remove(map, table, processed);
            });
        });
    }
}
