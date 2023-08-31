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

@TestInstance(Lifecycle.PER_CLASS)
@ActiveProfiles("source")
@SpringBootTest
@ExtendWith(SpringExtension.class)
class TimeResultSetHandlerSourceTest {
    private static final Logger log = LogUtils.getLogger();
    private static final ResultSetHandlerFactory RESULT_SET_FACTORY = new ResultSetHandlerFactory();
    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    private static NamedParameterJdbcTemplate jdbc;
    private static ResultSetHandler resultSetHandler;

    @BeforeAll
    private void createTable() {
        createEnv_TIME_0018_01();
        jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        resultSetHandler = RESULT_SET_FACTORY.createHandler(DataBaseType.MS);
    }

    @AfterAll
    private void cleanEnv() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbc = null;
        resultSetHandler = null;
    }

    private void createEnv_TIME_0018_01() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbcTemplateOne.execute(CreateTableSql.CREATE);
        jdbcTemplateOne.execute(CreateTableSql.INSERT);
    }

    @SuppressWarnings("all")
    interface CreateTableSql {
        String DROP = "DROP table if EXISTS  t_data_checker_time_0018_01; ";
        String CREATE = " CREATE TABLE t_data_checker_time_0018_01 (id INT(10) NOT NULL,c_date DATE NOT NULL, "
            + " c_time TIME NOT NULL,c_year YEAR NOT NULL,c_date_time DATETIME NOT NULL,"
            + "c_date_time_3 DATETIME(3) NULL DEFAULT NULL,"
            + " c_timestamp TIMESTAMP(3) NOT NULL,PRIMARY KEY (id) USING BTREE )"
            + " COLLATE='utf8mb4_0900_ai_ci' ENGINE=INNODB;";
        String INSERT = "INSERT INTO t_data_checker_time_0018_01 VALUES "
            + " (1, '2022-09-18', '17:39:01', '2022', '2022-09-18 17:39:03', '2022-09-19 10:13:47.123', '2022-09-18 17:39:04');";

        String QUERY_ONE = "select * from  t_data_checker_time_0018_01;";
    }

    @DisplayName("query t_data_checker_time_0018_01 id=1")
    @Test
    void testPutOneResultSetToMap() throws Exception {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query t_data_checker_time_0018_01 id=1 {}", map.toString());
        //        //Verify the results
        assertEquals("1", map.get("id"));
        assertEquals("2022-09-18", map.get("c_date"));
        assertEquals("17:39:01", map.get("c_time"));
        assertEquals("2022", map.get("c_year"));
        assertEquals("2022-09-18 17:39:03", map.get("c_date_time"));
        assertEquals("2022-09-19 10:13:47.123", map.get("c_date_time_3"));
        assertEquals("2022-09-18 17:39:04", map.get("c_timestamp"));
    }
}
