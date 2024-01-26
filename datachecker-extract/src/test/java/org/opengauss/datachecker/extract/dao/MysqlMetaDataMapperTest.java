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
import org.hamcrest.CoreMatchers;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;

import java.util.List;

import static org.hamcrest.MatcherAssert.assertThat;

/**
 * MysqlMetaDataMapperTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
@TestInstance(Lifecycle.PER_CLASS)
public class MysqlMetaDataMapperTest extends BaseMysqlMapper {

    private String schema = "test_schema";

    public MysqlMetaDataMapperTest() {
        super("mapper/MysqlMetaDataMapper.xml");
        initTestDatabaseScript(baseMapperDs, testDatabaseInitScript);
    }

    @BeforeAll
    void setUp() {
        SqlScriptUtils.execTestSqlScript(getConnection(), testDataDir + "/sql/" + "init_t_double.sql");
    }

    @Test
    public void testQueryTableNameList() {
        List<String> tableNameList = mapper.queryTableNameList(schema);
        assertThat(tableNameList, CoreMatchers.hasItem("t_double"));
    }

    @AfterAll
    void dropTestDatabase() {
        super.dropTestDb(testDatabaseInitScript);
    }
}
