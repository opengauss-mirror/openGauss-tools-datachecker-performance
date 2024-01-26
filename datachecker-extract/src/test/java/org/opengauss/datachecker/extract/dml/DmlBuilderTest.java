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

package org.opengauss.datachecker.extract.dml;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.List;
import java.util.Map;

import static org.assertj.core.api.Assertions.assertThat;

class DmlBuilderTest {

    private DmlBuilder dmlBuilderMsTest;
    private DmlBuilder dmlBuilderOgTest;

    @BeforeEach
    void setUp() {
        dmlBuilderMsTest = new DmlBuilder(DataBaseType.MS, false);
        dmlBuilderOgTest = new DmlBuilder(DataBaseType.OG, true);
    }

    @DisplayName("test build columns")
    @Test
    void testBuildColumns() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);

        final List<ColumnsMetaData> columnsMetas = List.of(columnsMetaData);

        // Run the test
        dmlBuilderMsTest.buildColumns(columnsMetas);
        dmlBuilderOgTest.buildColumns(columnsMetas);
        assertThat(dmlBuilderMsTest.columns).isEqualTo("`columnName`");
        assertThat(dmlBuilderOgTest.columns).isEqualTo("`columnName`");
        // Verify the results
    }

    @DisplayName("test build multiple columns")
    @Test
    void testBuildMultipleColumns() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType2");
        columnsMetaData2.setDataType("dataType2");
        columnsMetaData2.setOrdinalPosition(1);
        final List<ColumnsMetaData> columnsMetas = List.of(columnsMetaData, columnsMetaData2);

        // Run the test
        dmlBuilderMsTest.buildColumns(columnsMetas);
        dmlBuilderOgTest.buildColumns(columnsMetas);
        assertThat(dmlBuilderMsTest.columns).isEqualTo("`columnName`,`columnName2`");
        assertThat(dmlBuilderOgTest.columns).isEqualTo("`columnName`,`columnName2`");
        // Verify the results
    }

    @DisplayName("test build database type")
    @Test
    void testBuildDataBaseType() {
        dmlBuilderMsTest.buildDataBaseType(DataBaseType.MS);
        dmlBuilderOgTest.buildDataBaseType(DataBaseType.OG);
        assertThat(dmlBuilderMsTest.dataBaseType).isEqualTo(DataBaseType.MS);
        assertThat(dmlBuilderOgTest.dataBaseType).isEqualTo(DataBaseType.OG);
    }

    @DisplayName("test build database schema")
    @Test
    void testBuildSchema() {
        // Setup
        // Run the test
        dmlBuilderMsTest.buildSchema("schema");
        dmlBuilderOgTest.buildSchema("schema");
        assertThat(dmlBuilderMsTest.schema).isEqualTo("`schema`");
        assertThat(dmlBuilderOgTest.schema).isEqualTo("`schema`");
    }

    @DisplayName("test build database table")
    @Test
    void testBuildTableName() {
        // Setup
        // Run the test
        dmlBuilderMsTest.buildTableName("tableName");
        dmlBuilderOgTest.buildTableName("tableName");
        assertThat(dmlBuilderMsTest.tableName).isEqualTo("`tableName`");
        assertThat(dmlBuilderOgTest.tableName).isEqualTo("`tableName`");
    }

    @DisplayName("test build  condition composite primary   ")
    @Test
    void testBuildConditionCompositePrimary() {
        // Setup
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);

        // Setup
        final ColumnsMetaData columnsMetaData2 = new ColumnsMetaData();
        columnsMetaData2.setTableName("tableName");
        columnsMetaData2.setColumnName("columnName2");
        columnsMetaData2.setColumnType("columnType");
        columnsMetaData2.setDataType("dataType");
        columnsMetaData2.setOrdinalPosition(0);
        columnsMetaData2.setColumnKey(ColumnKey.PRI);

        final List<ColumnsMetaData> primaryMetas = List.of(columnsMetaData, columnsMetaData2);

        // Run the test
        final String result = dmlBuilderMsTest.buildConditionCompositePrimary(primaryMetas);
        final String resultOg = dmlBuilderOgTest.buildConditionCompositePrimary(primaryMetas);

        // Verify the results
        assertThat(result).isEqualTo("(`columnName`,`columnName2`)");
        assertThat(resultOg).isEqualTo("(`columnName`,`columnName2`)");
    }

    @DisplayName("test build  condition value ")
    @Test
    void testColumnsValueList() {
        // Setup
        final Map<String, String> columnsValue = Map.ofEntries(Map.entry("columnName", "value"));
        final ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName("tableName");
        columnsMetaData.setColumnName("columnName");
        columnsMetaData.setColumnType("columnType");
        columnsMetaData.setDataType("dataType");
        columnsMetaData.setOrdinalPosition(0);
        columnsMetaData.setColumnKey(ColumnKey.PRI);
        final List<ColumnsMetaData> columnsMetaList = List.of(columnsMetaData);

        // Run the test
        final List<String> result = dmlBuilderMsTest.columnsValueList(columnsValue, columnsMetaList);
        final List<String> resultOg = dmlBuilderMsTest.columnsValueList(columnsValue, columnsMetaList);

        // Verify the results
        assertThat(result).isEqualTo(List.of("'value'"));
        assertThat(resultOg).isEqualTo(List.of("'value'"));
    }
}
