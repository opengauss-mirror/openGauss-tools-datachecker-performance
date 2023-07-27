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

package org.opengauss.datachecker.extract.service;

import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.exception.ProcessMultipleException;
import org.opengauss.datachecker.common.exception.TaskNotFoundException;
import org.opengauss.datachecker.extract.cache.MetaDataCache;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.task.DataManipulationService;
import org.opengauss.datachecker.extract.task.ExtractTaskBuilder;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;
import static org.opengauss.datachecker.extract.util.TestJsonUtil.KEY_META_DATA_13_TABLE;

/**
 * DataExtractServiceImplTest
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
class DataExtractServiceImplTest {
    @Mock
    private ExtractTaskBuilder mockExtractTaskBuilder;
    @Mock
    private ExtractProperties mockExtractProperties;
    @Mock
    private MetaDataService mockMetaDataService;
    @Mock
    private DataManipulationService mockDataManipulationService;
    @InjectMocks
    private DataExtractServiceImpl dataExtractServiceImplUnderTest;

    @BeforeAll
    static void setUp() {
        HashMap<String, TableMetadata> result = TestJsonUtil.parseHashMap(KEY_META_DATA_13_TABLE, TableMetadata.class);
        MetaDataCache.putMap(result);
    }

    @DisplayName("build extract task")
    @Test
    void testBuildExtractTaskAllTables1() {
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SOURCE);
        // Setup
        final List<ExtractTask> extractTaskList = List.of(new ExtractTask());
        when(mockExtractTaskBuilder.builder(MetaDataCache.getAllKeys())).thenReturn(List.of(new ExtractTask()));
        // Run the test
        final List<ExtractTask> result = dataExtractServiceImplUnderTest.buildExtractTaskAllTables("processNo");
        // Verify the results
        assertThat(result).isEqualTo(extractTaskList);
    }

    @DisplayName("build extract task empty")
    @Test
    void test_ExtractTaskBuilderReturnsNoItems() {
        // Setup
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SOURCE);
        when(mockExtractTaskBuilder.builder(MetaDataCache.getAllKeys())).thenReturn(Collections.emptyList());
        // Run the test
        final List<ExtractTask> result = dataExtractServiceImplUnderTest.buildExtractTaskAllTables("processNo");
        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @DisplayName("build extract task ProcessMultipleException")
    @Test
    void test_ThrowsProcessMultipleException() {
        // Setup
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SOURCE);
        // Configure ExtractTaskBuilder.builder(...).
        when(mockExtractTaskBuilder.builder(MetaDataCache.getAllKeys())).thenReturn(List.of(new ExtractTask()));
        dataExtractServiceImplUnderTest.buildExtractTaskAllTables("processNo");
        // Run the test
        assertThatThrownBy(() -> dataExtractServiceImplUnderTest.buildExtractTaskAllTables("processNo"))
            .isInstanceOf(ProcessMultipleException.class);
    }

    @Test
    void testCleanBuildTask() {
        // Setup
        // Run the test
        dataExtractServiceImplUnderTest.cleanBuildTask();
    }

    @Test
    void testQueryTableInfo() {
        // Setup
        // Verify the results
        assertThatThrownBy(() -> dataExtractServiceImplUnderTest.queryTableInfo("tableName"))
            .isInstanceOf(TaskNotFoundException.class);
    }

    @Test
    void testBuildRepairStatementUpdateDml() {
        // Setup
        String table = "t_test_table_template";
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);
        when(mockDataManipulationService.buildReplace("schema", table, Set.of("value"), tableMetadata))
            .thenReturn(List.of("value"));
        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementUpdateDml("schema", table, Set.of("value"));
        // Verify the results
        assertThat(result).isEqualTo(List.of("value"));
    }

    @Test
    void test_DataManipulationServiceReturnsNoItems() {
        // Setup
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        when(mockDataManipulationService.buildReplace("schema", table, Set.of("value"), tableMetadata))
            .thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementUpdateDml("schema", table, Set.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @Test
    void testBuildRepairStatementInsertDml() {
        // Setup
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        when(mockDataManipulationService.buildInsert("schema", table, Set.of("value"), tableMetadata))
            .thenReturn(List.of("value"));

        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementInsertDml("schema", table, Set.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(List.of("value"));
    }

    @Test
    void testInsertDml_DataManipulationServiceReturnsNoItems() {
        String table = "t_test_table_template";
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        when(mockDataManipulationService.buildInsert("schema", table, Set.of("value"), tableMetadata))
            .thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementInsertDml("schema", table, Set.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @Test
    void testBuildRepairStatementDeleteDml() {
        // Setup
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        when(mockDataManipulationService.buildDelete("schema", table, Set.of("value"), tableMetadata.getPrimaryMetas()))
            .thenReturn(List.of("value"));

        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementDeleteDml("schema", table, Set.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(List.of("value"));
    }

    @Test
    void testDeleteDml_DataManipulationServiceReturnsNoItems() {
        // Setup
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);
        when(mockDataManipulationService.buildDelete("schema", table, Set.of("value"), tableMetadata.getPrimaryMetas()))
            .thenReturn(Collections.emptyList());

        // Run the test
        final List<String> result =
            dataExtractServiceImplUnderTest.buildRepairStatementDeleteDml("schema", table, Set.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @Test
    void testQueryTableColumnValues() {
        // Setup
        final List<Map<String, String>> expectedResult = List.of(Map.ofEntries(Map.entry("value", "value")));

        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        // Configure DataManipulationService.queryColumnValues(...).
        final List<Map<String, String>> maps = List.of(Map.ofEntries(Map.entry("value", "value")));
        when(mockDataManipulationService.queryColumnValues(table, List.of("value"), tableMetadata)).thenReturn(maps);

        // Run the test
        final List<Map<String, String>> result =
            dataExtractServiceImplUnderTest.queryTableColumnValues(table, List.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testQueryTableColumnValues_NoItems() {
        // Setup
        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        String table = "t_test_table_template";
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);
        when(mockDataManipulationService.queryColumnValues(table, List.of("value"), tableMetadata))
            .thenReturn(Collections.emptyList());

        // Run the test
        final List<Map<String, String>> result =
            dataExtractServiceImplUnderTest.queryTableColumnValues(table, List.of("value"));

        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @Test
    void testQueryTableMetadataHash() {
        // Setup
        final TableMetadataHash expectedResult = new TableMetadataHash();
        expectedResult.setTableName("tableName");
        expectedResult.setTableHash(0L);

        // Configure DataManipulationService.queryTableMetadataHash(...).
        final TableMetadataHash tableMetadataHash = new TableMetadataHash();
        tableMetadataHash.setTableName("tableName");
        tableMetadataHash.setTableHash(0L);
        when(mockDataManipulationService.queryTableMetadataHash("tableName")).thenReturn(tableMetadataHash);

        // Run the test
        final TableMetadataHash result = dataExtractServiceImplUnderTest.queryTableMetadataHash("tableName");

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testQuerySecondaryCheckRowData() {
        // Setup
        String table = "t_test_table_template";

        final SourceDataLog dataLog = new SourceDataLog();
        dataLog.setTableName(table);
        dataLog.setBeginOffset(0L);
        dataLog.setCompositePrimarys(List.of("value"));
        dataLog.setCompositePrimaryValues(List.of("value"));

        final RowDataHash rowDataHash = new RowDataHash();
        rowDataHash.setKey("primaryKey");
        rowDataHash.setKHash(0L);
        rowDataHash.setVHash(0L);
        final List<RowDataHash> expectedResult = List.of(rowDataHash);

        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).

        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        // Configure DataManipulationService.queryColumnHashValues(...).
        final RowDataHash rowDataHash1 = new RowDataHash();
        rowDataHash1.setKey("primaryKey");
        rowDataHash1.setKHash(0L);
        rowDataHash1.setVHash(0L);
        final List<RowDataHash> rowDataHashes = List.of(rowDataHash1);
        when(mockDataManipulationService.queryColumnHashValues(table, List.of("value"), tableMetadata))
            .thenReturn(rowDataHashes);

        // Run the test
        final List<RowDataHash> result = dataExtractServiceImplUnderTest.querySecondaryCheckRowData(dataLog);

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }

    @Test
    void testQuerySecondaryCheckRowData_NoItems() {
        String table = "t_test_table_template";

        // Setup
        final SourceDataLog dataLog = new SourceDataLog();
        dataLog.setTableName(table);
        dataLog.setBeginOffset(0L);
        dataLog.setCompositePrimarys(List.of("value"));
        dataLog.setCompositePrimaryValues(List.of("value"));

        // Configure MetaDataService.getMetaDataOfSchemaByCache(...).
        final TableMetadata tableMetadata = MetaDataCache.get(table);
        when(mockMetaDataService.getMetaDataOfSchemaByCache(table)).thenReturn(tableMetadata);

        when(mockDataManipulationService.queryColumnHashValues(table, List.of("value"), tableMetadata))
            .thenReturn(Collections.emptyList());

        // Run the test
        final List<RowDataHash> result = dataExtractServiceImplUnderTest.querySecondaryCheckRowData(dataLog);

        // Verify the results
        assertThat(result).isEqualTo(Collections.emptyList());
    }

    @Test
    void testGetEndpointConfig() {
        // Setup
        final ExtractConfig expectedResult = new ExtractConfig();
        expectedResult.setDebeziumEnable(false);
        final Database database = new Database();
        database.setSchema("schema");
        database.setDatabaseType(DataBaseType.MS);
        database.setEndpoint(Endpoint.SOURCE);
        expectedResult.setDatabase(database);

        when(mockExtractProperties.getDatabaseType()).thenReturn(DataBaseType.MS);
        when(mockExtractProperties.getSchema()).thenReturn("schema");
        when(mockExtractProperties.getEndpoint()).thenReturn(Endpoint.SOURCE);
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(false);

        // Run the test
        final ExtractConfig result = dataExtractServiceImplUnderTest.getEndpointConfig();

        // Verify the results
        assertThat(result).isEqualTo(expectedResult);
    }
}
