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

package org.opengauss.datachecker.extract.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;

import static org.assertj.core.api.Assertions.assertThat;
import static org.opengauss.datachecker.extract.util.TestJsonUtil.KEY_META_DATA_13_TABLE;

class TableRuleAdapterServiceTest {

    private TableRuleAdapterService tableRuleAdapterServiceUnderTest;

    @BeforeEach
    void setUp() {
        tableRuleAdapterServiceUnderTest = new TableRuleAdapterService();
        HashMap<String, TableMetadata> result = TestJsonUtil.parseHashMap(KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(result);
    }
    @DisplayName("one white rules")
    @Test
    void testExecuteTableRule_one_white() {
        // Setup
        final Rule white = new Rule("white", "^[a-zA-Z][a-zA-Z_]+$");
        final List<Rule> rules = List.of(white);
        final List<String> tableList = new ArrayList<>(MetaDataCache.getAllKeys());
        // Run the test
        final List<String> result = tableRuleAdapterServiceUnderTest.executeTableRule(rules, tableList);
        // Verify the results
        assertThat(result).isEqualTo(List.of("t_debug", "t_time", "t_test_table_template", "t_test_mock_function"));
    }

    @DisplayName("multiple white rules")
    @Test
    void testExecuteTableRule_multiple_white() {
        // Setup
        final Rule white1 = new Rule("white", "^[a-zA-Z][a-zA-Z]+$");
        final Rule white2 = new Rule("white", "^[a-zA-Z][a-zA-Z_]+$");
        final List<Rule> rules = List.of(white1, white2);
        final List<String> tableList = new ArrayList<>(MetaDataCache.getAllKeys());
        // Run the test
        final List<String> result = tableRuleAdapterServiceUnderTest.executeTableRule(rules, tableList);

        // Verify the results
        assertThat(result).isEqualTo(List.of("t_debug", "t_time", "t_test_table_template", "t_test_mock_function"));
    }

    @DisplayName("one black rules")
    @Test
    void testExecuteTableRule_one_black() {
        // Setup
        final Rule black = new Rule("black", "^[a-zA-Z][a-zA-Z_]+$");
        final List<Rule> rules = List.of(black);
        final List<String> tableList = new ArrayList<>(MetaDataCache.getAllKeys());
        // Run the test
        final List<String> result = tableRuleAdapterServiceUnderTest.executeTableRule(rules, tableList);
        tableList.removeAll(List.of("t_debug", "t_time", "t_test_table_template", "t_test_mock_function"));
        // Verify the results
        assertThat(result).isEqualTo(tableList);
    }

    @DisplayName("multiple black rules")
    @Test
    void testExecuteTableRule_multiple_black() {
        // Setup
        final Rule black1 = new Rule("black", "^[a-zA-Z][a-zA-Z]+$");
        final Rule black2 = new Rule("black", "^[a-zA-Z][a-zA-Z_]+$");
        final List<Rule> rules = List.of(black1, black2);
        final List<String> tableList = new ArrayList<>(MetaDataCache.getAllKeys());
        // Run the test
        final List<String> result = tableRuleAdapterServiceUnderTest.executeTableRule(rules, tableList);
        tableList.removeAll(List.of("t_debug", "t_time", "t_test_table_template", "t_test_mock_function"));
        // Verify the results
        assertThat(result).isEqualTo(tableList);
    }
}
