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

import lombok.extern.slf4j.Slf4j;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.BeforeAll;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.TestInstance;
import org.junit.jupiter.api.TestInstance.Lifecycle;
import org.junit.jupiter.api.extension.ExtendWith;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
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
class ResultSetHandlerSinkTest {
    private static final Logger log = LogUtils.getLogger();
    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    private static NamedParameterJdbcTemplate jdbc;
    private static ResultSetHandler resultSetHandler;

    @BeforeAll
    private void createTable() {
        createEnv_0034_02();
        createEnv_0037_01();
        createEnv_0052_05();
        createEnv_0052_10();
        jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        resultSetHandler = new OpenGaussResultSetHandler();
    }

    private void createEnv_0034_02() {
        jdbcTemplateOne.execute(CreateTableSql.DROP);
        jdbcTemplateOne.execute(CreateTableSql.CREATE);
        jdbcTemplateOne.execute(CreateTableSql.INSERT);
    }

    private void createEnv_0052_10() {
        jdbcTemplateOne.execute(CreateTableSql.DROP_0052_10);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0052_10);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0052_10);
    }

    private void createEnv_0052_05() {
        jdbcTemplateOne.execute(CreateTableSql.DROP_0052_05);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0052_05);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0052_05);
    }

    private void createEnv_0037_01() {
        jdbcTemplateOne.execute(CreateTableSql.DROP_0037_01);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0037_01);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0037_01);
    }

    interface CreateTableSql {
        // byte binary type
        String DROP = "DROP TABLE IF EXISTS t_data_checker_0034_02;";
        String CREATE = "CREATE TABLE t_data_checker_0034_02 (c_1 bytea NOT NULL, c_2 bytea,"
            + "c_3 blob,  c_4 blob, c_5 blob, c_6 blob)  WITH (orientation=row, compression=no);"
            + "ALTER TABLE t_data_checker_0034_02 ADD CONSTRAINT pk_t_data_checker_0034_02_1662889877_1 PRIMARY KEY (c_1)";
        String INSERT = "INSERT INTO t_data_checker_0034_02 (c_1,c_2,c_3,c_4,c_5,c_6) VALUES"
            + "('\u00013','\u000E','02AA','FF','0E','FF'),('1','10',null,null,null,null),"
            + "('saa','sing','6D657461','636F636F','6170706C65','70656E63696C');";
        String QUERY_ONE = "select * from t_data_checker_0034_02 where c_1='\u00013'";
        String QUERY_ONE0x310000 = "select * from t_data_checker_0034_02 where c_1='1'";

        // JSON
        String DROP_0052_10 = " drop table if exists t_data_checker_0052_10;";
        String CREATE_0052_10 = "CREATE TABLE t_data_checker_0052_10(c1 JSON, c2 int primary key);";
        String INSERT_0052_10 =
            "insert into t_data_checker_0052_10 values" + "('{\"key1\": \"value1\", \"key2\": \"value2\"}',1),"
                + "('{\"m\": 17, \"n\": \"red\"}',2), ('{\"x\": 17, \"y\": \"red\", \"z\": [3, 5, 7]}',3);";
        String QUERY_0052_10 = "select * from t_data_checker_0052_10 where c2=1";

        // char
        String DROP_0052_05 = "drop table if  exists t_data_checker_0052_05;";
        String CREATE_0052_05 =
            "create table t_data_checker_0052_05(c_1 character(255) NOT NULL,c_2 character varying(255),c_3 text, c_4 text, c_5 text, c_6 text );"
                + " ALTER TABLE t_data_checker_0052_05 ADD CONSTRAINT pk_t_data_checker_0052_05_1663125177_1 PRIMARY KEY (c_1);";
        String INSERT_0052_05 =
            "insert into t_data_checker_0052_05 values ('数据校验工具使用','MySQL数据库','文本信息','滕王阁','王勃','落霞与孤鹜齐飞'),"
                + "('测试工具','openGauss数据库','字符串','将进酒','李白','呼儿将出换美酒'),('102','tom','wangyu@163.com','2022-09-04','sales','Hi')";
        String QUERY_0052_05 = "select * from t_data_checker_0052_05 where c_1='测试工具'";

        String DROP_0037_01 = "drop table if  exists t_data_checker_0037_01;";
        String CREATE_0037_01 =
            "create table t_data_checker_0037_01 (c_1 character(255) , c_2 character varying(255),c_3 text, c_4 text, c_5 text, c_6 text,"
                + " store_id integer NOT NULL)  partition by range (store_id) (partition p0 values less than (3) TABLESPACE pg_default,"
                + " partition p1 values less than (5) TABLESPACE pg_default,partition p2 values less than (7) TABLESPACE pg_default,"
                + " partition p3 values less than (MAXVALUE) TABLESPACE pg_default) ENABLE ROW MOVEMENT;"
                + " ALTER TABLE t_data_checker_0037_01 ADD CONSTRAINT pk_t_data_checker_0037_01_1663125177_0 PRIMARY KEY (store_id);";
        String INSERT_0037_01 = "insert into t_data_checker_0037_01(c_1, c_2,c_3,c_4,c_5,c_6,store_id) values"
            + "('102','tom','wangyu@163.com','2022-09-04','sales','Hi',2),"
            + "('101','jack','chenyu@163.com','2022-09-03','sales','hello',1)";
        String QUERY_0037_01 = "select * from t_data_checker_0037_01 where c_1='101'";

    }

    @DisplayName("query binary c1=0x013300")
    @Test
    void testPutOneResultSetToMap() throws Exception {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("testPutOneResultSetToMap {}", map.toString());
        //Verify the results
        assertEquals("1,51,0", map.get("c_1"));
        assertEquals("14", map.get("c_2"));
        assertEquals("2,-86", map.get("c_3"));
        assertEquals("-1", map.get("c_4"));
        assertEquals("14", map.get("c_5"));
        assertEquals("-1", map.get("c_6"));
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

        //Verify the results
        assertEquals("49,0,0", map.get("c_1"));
        assertEquals("49,48", map.get("c_2"));
        assertNull(map.get("c_3"));
        assertNull(map.get("c_4"));
        assertNull(map.get("c_5"));
        assertNull(map.get("c_6"));
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

    @DisplayName("query sink json 0052_10 c2=1")
    @Test
    void test_query_json_0052_10() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_0052_10, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("test_t_data_checker_0052_10 {}", map.toString());
        //Verify the results
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", map.get("c1"));
        assertEquals("1", map.get("c2"));
    }

    @DisplayName("query source char 0052_05 c_1=测试工具")
    @Test
    void test_query_char_0052_05() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(ResultSetHandlerTest.CreateTableSql.QUERY_0052_05, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("test_t_data_checker_0052_05 {}", map.toString());
        //Verify the results
        assertEquals("测试工具", map.get("c_1"));
        assertEquals("openGauss数据库", map.get("c_2"));
        assertEquals("字符串", map.get("c_3"));
        assertEquals("将进酒", map.get("c_4"));
        assertEquals("李白", map.get("c_5"));
        assertEquals("呼儿将出换美酒", map.get("c_6"));
    }

    @DisplayName("query source char 0037_01")
    @Test
    void test_query_char_0037_01() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(ResultSetHandlerTest.CreateTableSql.QUERY_0037_01, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("test_t_data_checker_0037_01 {}", map.toString());
        //Verify the results
        assertEquals("101", map.get("c_1"));
        assertEquals("jack", map.get("c_2"));
        assertEquals("chenyu@163.com", map.get("c_3"));
        assertEquals("2022-09-03", map.get("c_4"));
        assertEquals("sales", map.get("c_5"));
        assertEquals("hello", map.get("c_6"));
        assertEquals("1", map.get("store_id"));
    }
}
