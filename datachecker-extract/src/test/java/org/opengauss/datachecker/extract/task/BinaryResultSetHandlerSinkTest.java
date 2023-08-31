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

package org.opengauss.datachecker.extract.task;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.AfterAll;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.test.context.ActiveProfiles;
import org.springframework.test.context.junit.jupiter.SpringExtension;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertNull;

@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles("sink")
@SpringBootTest
@ExtendWith(SpringExtension.class)
class BinaryResultSetHandlerSinkTest {
    private static final Logger log = LogUtils.getLogger();
    private static final ResultSetHandlerFactory RESULT_SET_FACTORY = new ResultSetHandlerFactory();
    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    private static NamedParameterJdbcTemplate jdbc;
    private static ResultSetHandler resultSetHandler;

    @BeforeAll
    private void createTable() {
        createEnv_0034_02();
        jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        resultSetHandler = RESULT_SET_FACTORY.createHandler(DataBaseType.OG);
    }

    @AfterAll
    private void cleanEnv() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbc = null;
        resultSetHandler = null;
    }

    private void createEnv_0034_02() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbcTemplateOne.execute(CreateTableSql.CREATE);
        jdbcTemplateOne.execute(CreateTableSql.INSERT);
    }

    @SuppressWarnings("all")
    interface CreateTableSql {
        // byte binary type
        String DROP = "DROP table if EXISTS  t_data_checker_binary_003701;";
        String CREATE = "CREATE TABLE t_data_checker_binary_003701 ( id integer NOT NULL PRIMARY KEY,"
            + "c_binary bytea NOT NULL,c_varbinary bytea NULL DEFAULT NULL,"
            + "c_tinyblob BLOB NULL DEFAULT NULL, c_blob BLOB NULL DEFAULT NULL,"
            + "c_mediumblob BLOB NULL DEFAULT NULL,c_longblob BLOB NULL DEFAULT NULL);";
        String INSERT = "INSERT INTO t_data_checker_binary_003701 VALUES"
            + " (1,'\u00013','\u000E','02AA','FF','0E','FF'),(2,'1','10',null,null,null,null),"
            + "(3,'saa','sing','6D657461','636F636F','6170706C65','70656E63696C');";
        String QUERY_ONE = "select * from t_data_checker_binary_003701 where id=1";
        String QUERY_ONE0x310000 = "select * from t_data_checker_binary_003701 where id=2";

    }

    @DisplayName("query binary c1=0x013300")
    @Test
    void testPutOneResultSetToMap() throws Exception {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query binary c1=0x013300 {}", map.toString());
        //Verify the results
        assertEquals("1,51,0", map.get("c_binary"));
        assertEquals("14", map.get("c_varbinary"));
        assertEquals("2,-86", map.get("c_tinyblob"));
        assertEquals("-1", map.get("c_blob"));
        assertEquals("14", map.get("c_mediumblob"));
        assertEquals("-1", map.get("c_longblob"));
    }

    @DisplayName("query sink binary c1=0x310000")
    @Test
    void testPutOneResultSetToMap_0x310000() throws Exception {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE0x310000, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("testPutOneResultSetToMap_0x310000 {}", map.toString());
        //        2022-09-19 18:29:24.107 [main] INFO  [d.e.t.BinaryResultSetHandlerSinkTest] - query binary c1=0x013300
        //        {c_varbinary=14, c_longblob=[B@782e0844, id=1, c_blob=[B@5c13534a, c_mediumblob=[B@4d7f9b33, c_binary=1,51, c_tinyblob=[B@5d08a65c}
        //        {c_varbinary=14, c_longblob=[B@6a6e410c, id=1, c_blob=[B@5a95aaae, c_mediumblob=[B@7402bfe7, c_binary=1,51,0, c_tinyblob=[B@6632eb19}
        //Verify the results
        assertEquals("49,0,0", map.get("c_binary"));
        assertEquals("49,48", map.get("c_varbinary"));
        assertNull(map.get("c_tinyblob"));
        assertNull(map.get("c_blob"));
        assertNull(map.get("c_mediumblob"));
        assertNull(map.get("c_longblob"));
    }

    @DisplayName("query sink binary c1=0x310000")
    @Test
    void test_byte_blob_0x310000() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE0x310000, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("testPutOneResultSetToMap_0x310000 {}", map.toString());

        //Verify the results
        assertEquals("49,0,0", map.get("c_1"));
        assertEquals("49,48", map.get("c_2"));
        assertNull(map.get("c_3"));
        assertNull(map.get("c_4"));
        assertNull(map.get("c_5"));
        assertNull(map.get("c_6"));
    }
}
