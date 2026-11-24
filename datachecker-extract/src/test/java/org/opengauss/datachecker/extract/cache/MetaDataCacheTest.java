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

package org.opengauss.datachecker.extract.cache;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.HashMap;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;
import static org.opengauss.datachecker.extract.util.TestJsonUtil.KEY_META_DATA_13_TABLE;

class MetaDataCacheTest {
    @BeforeAll
    static void setUp() {
        HashMap<String, TableMetadata> result = TestJsonUtil.parseHashMap(KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(result);
    }

    @Test
    void testGetAll() {
        // Setup
        final HashMap<String, TableMetadata> expectedResult =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        // Run the test
        final Map<String, TableMetadata> result = MetaDataCache.getAll();

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testPutMap() {
        // Setup
        final HashMap<String, TableMetadata> expectedResult =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(expectedResult);
    }

    @Test
    void testGet() {
        String table = "t_data_checker_0033_02";
        final HashMap<String, TableMetadata> map =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        // Run the test
        final TableMetadata result = MetaDataCache.get(table);
        // Verify the results
        assertThat(result).isEqualTo(map.get(table));
    }

    @Test
    void testUpdateRowCount() {
        // Setup
        String table = "t_data_checker_0033_02";
        // Run the test
        MetaDataCache.updateRowCount(table, 10L);
        final TableMetadata metadata = MetaDataCache.get(table);
        // Verify the results
        assertThat(metadata.getTableRows()).isEqualTo(10);
    }

    @Test
    void testContainsKey() {
        String table = "t_data_checker_0033_02";
        assertThat(MetaDataCache.containsKey(table)).isTrue();
    }

    @Test
    void testGetAllKeys() {
        final HashMap<String, TableMetadata> map =
            TestJsonUtil.parseHashMap(TestJsonUtil.KEY_META_DATA_13_TABLE, TableMetadata.class);
        assertThat(MetaDataCache.getAllKeys()).isEqualTo(map.keySet());
    }

}
