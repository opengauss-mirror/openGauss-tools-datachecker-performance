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
@ActiveProfiles("source")
@SpringBootTest
@ExtendWith(SpringExtension.class)
class BinaryResultSetHandlerSourceTest {
    private static final Logger log = LogUtils.getLogger();
    private static final ResultSetHandlerFactory RESULT_SET_FACTORY = new ResultSetHandlerFactory();
    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    private NamedParameterJdbcTemplate jdbc;
    private ResultSetHandler resultSetHandler;

    @BeforeAll
    private void createTable() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbcTemplateOne.execute(CreateTableSql.CREATE);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0034_02);

        jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        resultSetHandler = RESULT_SET_FACTORY.createHandler(DataBaseType.MS);
    }

    @AfterAll
    private void cleanEnv() {
        //        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbc = null;
        resultSetHandler = null;
    }

    @SuppressWarnings("all")
    interface CreateTableSql {
        // byte binary type
        String DROP = "DROP table if EXISTS  t_data_checker_binary_003701;";
        String CREATE = "CREATE TABLE t_data_checker_binary_003701 ( id INT(10) NOT NULL DEFAULT '0',"
            + "c_binary BINARY(3) NOT NULL,c_varbinary VARBINARY(10) NULL DEFAULT NULL,"
            + "c_tinyblob TINYBLOB NULL DEFAULT NULL, c_blob BLOB NULL DEFAULT NULL,"
            + "c_mediumblob MEDIUMBLOB NULL DEFAULT NULL,c_longblob LONGBLOB NULL DEFAULT NULL,"
            + "PRIMARY KEY (id) USING BTREE ) COLLATE='utf8mb4_0900_ai_ci' ENGINE=INNODB;";

        String INSERT_0034_02 = "INSERT INTO t_data_checker_binary_003701 VALUES"
            + "(1,0x013300, 0x0E, 0x02AA,0xFF, 0x0E, 0xFF), (2,0x310000, 0x3130, NULL, NULL, NULL, NULL),"
            + "(3,0x736161, 0x73696E67, 0x6D657461, 0x636F636F, 0x6170706C65,0x70656E63696C);";
        String QUERY_ONE = "select * from t_data_checker_binary_003701  where id=1";

        String binaryFormat = "lower(hex(trim(TRAILING '\\0' from %s)))";
        String QUERY_ONE_FORMAT = "select id," + binaryFormat.replace("%s", "c_binary")
            + "c_binary from t_data_checker_binary_003701  where id=1";
        String QUERY_ONE2 = "select * from t_data_checker_binary_003701 where id=2";

    }

    @DisplayName("query source binary_003701 id=1")
    @Test
    void testQuerySourceBinary_003701_Id_1() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source binary_003701 id=1 {}", map.toString());
        //Verify the results
        assertEquals("1,51,0", map.get("c_binary"));
        assertEquals("14", map.get("c_varbinary"));
        assertEquals("2,-86", map.get("c_tinyblob"));
        assertEquals("-1", map.get("c_blob"));
        assertEquals("14", map.get("c_mediumblob"));
        assertEquals("-1", map.get("c_longblob"));
    }

    @DisplayName("query source binary_003701 id=2")
    @Test
    void testQuerySourceBinary_003701_Id_2() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE2, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source binary_003701 id=2 {}", map.toString());
        //Verify the results
        assertEquals("49,0,0", map.get("c_binary"));
        assertEquals("49,48", map.get("c_varbinary"));
        assertNull(map.get("c_tinyblob"));
        assertNull(map.get("c_blob"));
        assertNull(map.get("c_mediumblob"));
        assertNull(map.get("c_longblob"));
    }

    @DisplayName("query source binary_format_003701 id=1")
    @Test
    void testQuerySourceBinary_format_003701_Id_1() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE_FORMAT, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source binary_003701 id=2 {}", map.toString());
        //Verify the results
        assertEquals("0133", map.get("c_binary"));
    }
}
