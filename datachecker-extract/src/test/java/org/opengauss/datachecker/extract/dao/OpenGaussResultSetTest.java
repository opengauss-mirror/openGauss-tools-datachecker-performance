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

package org.opengauss.datachecker.extract.dao;

import lombok.extern.slf4j.Slf4j;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.opengauss.datachecker.extract.task.OpenGaussResultSetHandler;

/**
 * MysqlResultSetTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
public class OpenGaussResultSetTest extends BaseOpenGaussMapper {
    private static final String schema = "test_schema";
    private final BaseDataResultSetHandlerTest dataResultSetHandler;

    public OpenGaussResultSetTest() {
        super("mapper/OpgsMetaDataMapper.xml");
        initTestDatabaseScript(baseMapperDs, testDatabaseInitScript);
        dataResultSetHandler =
            new BaseDataResultSetHandlerTest(testDataDir, getConnection(), new OpenGaussResultSetHandler(),
                getMapper());
    }

    @DisplayName("test openGauss t_double")
    @Test
    public void testDouble() {
        dataResultSetHandler.testTable(schema, "t_double", "init_t_double.sql");
    }

    @DisplayName("test openGauss t_float")
    @Test
    public void testFloat() {
        dataResultSetHandler.testTable(schema, "t_float", "init_t_float.sql");
    }

    @DisplayName("test openGauss t_decimal")
    @Test
    public void testDecimal() {
        dataResultSetHandler.testTable(schema, "t_decimal", "init_t_decimal.sql");
    }

    @DisplayName("test openGauss t_char")
    @Test
    public void testChar() {
        dataResultSetHandler.testTable(schema, "t_char", "init_t_char.sql");
    }

    @DisplayName("test openGauss t_varchar")
    @Test
    public void testVarchar() {
        dataResultSetHandler.testTable(schema, "t_varchar", "init_t_varchar.sql");
    }

    @DisplayName("test openGauss t_text")
    @Test
    public void testText() {
        dataResultSetHandler.testTable(schema, "t_text", "init_t_text_og.sql");
    }

    @DisplayName("test openGauss t_time")
    @Test
    public void testTime() {
        dataResultSetHandler.testTable(schema, "t_time", "init_t_time.sql");
    }

    @AfterAll
    void dropTestDatabaseT() {
        super.dropTestDb(testDatabaseInitScript);
    }
}
