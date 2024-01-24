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
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;
import org.opengauss.datachecker.extract.task.MysqlResultSetHandler;

/**
 * MysqlResultSetTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
public class MysqlResultSetTest extends BaseDataResultSetHandlerTest<MysqlMetaDataMapper> {
    private static final String schema = "test_schema";

    public MysqlResultSetTest() {
        super("mapper/MysqlMetaDataMapper.xml", new MysqlResultSetHandler());
    }

    @DisplayName("test mysql double t_double")
    @Test
    public void testDouble() {
        testTable(schema, "t_double", "init_t_double.sql");
    }

    @DisplayName("test mysql double t_float")
    @Test
    public void testFloat() {
        testTable(schema, "t_float", "init_t_float.sql");
    }

    @AfterAll
    void dropTestDatabaseT() {
        super.dropTestDatabase();
    }
}
