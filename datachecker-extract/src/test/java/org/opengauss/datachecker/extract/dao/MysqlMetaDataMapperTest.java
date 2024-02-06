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
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.LinkedList;
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

    @Test
    public void testCheckDatabaseEmpty() {
        boolean isNotEmpty = mapper.checkDatabaseNotEmpty(schema);
        assertThat(isNotEmpty, CoreMatchers.is(true));
    }

    @Test
    public void testQueryTableMeatdataByJdbc() {
        Connection connection = getConnection();
        String sql = "SELECT info.TABLE_SCHEMA tableSchema,info.table_name tableName,info.table_rows tableRows , "
            + "info.avg_row_length avgRowLength FROM information_schema.tables info WHERE TABLE_SCHEMA='" + schema
            + "'";
        List<TableMetadata> list = new LinkedList<>();
        try (PreparedStatement ps = connection.prepareStatement(sql); ResultSet resultSet = ps.executeQuery()) {
            TableMetadata metadata;
            while (resultSet.next()) {
                metadata = new TableMetadata();
                metadata.setSchema(resultSet.getString("tableSchema"));
                metadata.setTableName(resultSet.getString("tableName"));
                metadata.setTableRows(resultSet.getLong("tableRows"));
                metadata.setAvgRowLength(resultSet.getLong("avgRowLength"));
                list.add(metadata);
            }
        } catch (SQLException esql) {
            log.error("", esql);
        }
    }

    @AfterAll
    void dropTestDatabase() {
        super.dropTestDb(testDatabaseInitScript);
    }
}
