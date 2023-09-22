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
@ActiveProfiles("source")
@SpringBootTest
@ExtendWith(SpringExtension.class)
class ResultSetHandlerTest {
    private static final Logger log = LogUtils.getLogger();
    @Autowired
    private JdbcTemplate jdbcTemplateOne;
    private NamedParameterJdbcTemplate jdbc;
    private ResultSetHandler resultSetHandler;

    @BeforeAll
    private void createTable() {
        jdbcTemplateOne.execute(CreateTableSql.DROP_0034_02);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0034_02);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0034_02);

        jdbcTemplateOne.execute(CreateTableSql.DROP_0052_10);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0052_10);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0052_10);

        jdbcTemplateOne.execute(CreateTableSql.DROP_0052_05);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0052_05);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0052_05);

        jdbcTemplateOne.execute(CreateTableSql.DROP_0037_01);
        jdbcTemplateOne.execute(CreateTableSql.CREATE_0037_01);
        jdbcTemplateOne.execute(CreateTableSql.INSERT_0037_01);

        jdbc = new NamedParameterJdbcTemplate(jdbcTemplateOne);
        resultSetHandler = new MysqlResultSetHandler();
    }

    interface CreateTableSql {
        // byte binary type
        String DROP_0034_02 = "DROP TABLE IF EXISTS `t_data_checker_0034_02`;";
        String CREATE_0034_02 = "CREATE TABLE IF NOT EXISTS `t_data_checker_0034_02` (`c_1` binary(3) NOT NULL,"
            + "  `c_2` varbinary(10) DEFAULT NULL,  `c_3` tinyblob,  `c_4` blob, `c_5` mediumblob,"
            + "  `c_6` longblob, PRIMARY KEY (`c_1`)) ENGINE=InnoDB";
        String INSERT_0034_02 = "INSERT INTO `t_data_checker_0034_02` (`c_1`, `c_2`, `c_3`, `c_4`, `c_5`, `c_6`) VALUES"
            + "(0x013300, 0x0E, 0x02AA,0xFF, 0x0E, 0xFF), (0x310000, 0x3130, NULL, NULL, NULL, NULL),"
            + "(0x736161, 0x73696E67, 0x6D657461, 0x636F636F, 0x6170706C65,0x70656E63696C);";
        String QUERY_ONE = "select * from t_data_checker_0034_02 where c_1=0x013300";
        String QUERY_ONE0x310000 = "select * from t_data_checker_0034_02 where c_1=0x310000";

        // JSON
        String DROP_0052_10 = " drop table if exists t_data_checker_0052_10;";
        String CREATE_0052_10 = "CREATE TABLE t_data_checker_0052_10(c1 JSON, c2 int primary key);";
        String INSERT_0052_10 =
            "insert into t_data_checker_0052_10 values" + "('{\"key1\": \"value1\", \"key2\": \"value2\"}',1),"
                + "('{\"m\": 17, \"n\": \"red\"}',2), ('{\"x\": 17, \"y\": \"red\", \"z\": [3, 5, 7]}',3);";
        String QUERY_0052_10 = "select * from t_data_checker_0052_10 where c2=1";

        // char
        String DROP_0052_05 = "drop table if  exists t_data_checker_0052_05;";
        String CREATE_0052_05 = "create table t_data_checker_0052_05(c_1 char(255) primary key, c_2 varchar(255),"
            + "c_3 text, c_4 mediumtext, c_5 tinytext, c_6 longtext )default charset=utf8;";
        String INSERT_0052_05 =
            "insert into t_data_checker_0052_05 values ('数据校验工具使用','MySQL数据库','文本信息','滕王阁','王勃','落霞与孤鹜齐飞'),"
                + "('测试工具','openGauss数据库','字符串','将进酒','李白','呼儿将出换美酒'),('102','tom','wangyu@163.com','2022-09-04','sales','Hi')";
        String QUERY_0052_05 = "select * from t_data_checker_0052_05 where c_1='测试工具'";

        String DROP_0037_01 = "drop table if  exists t_data_checker_0037_01;";
        String CREATE_0037_01 =
            "create table t_data_checker_0037_01 (c_1 char(255),c_2 varchar(255),c_3 text, c_4 mediumtext,"
                + " c_5 tinytext,c_6 longtext, store_id int not null primary key)  partition by range (store_id) (partition p0 values less than (3),"
                + " partition p1 values less than (5),partition p2 values less than (7),partition p3 values less than maxvalue);";
        String INSERT_0037_01 = "insert into t_data_checker_0037_01(c_1, c_2,c_3,c_4,c_5,c_6,store_id) values"
            + "('102','tom','wangyu@163.com','2022-09-04','sales','Hi',2),"
            + "('101','jack','chenyu@163.com','2022-09-03','sales','hello',1)";
        String QUERY_0037_01 = "select * from t_data_checker_0037_01 where store_id=1";

    }

    @DisplayName("query source binary 0034_02 c1=0x013300")
    @Test
    void testPutOneResultSetToMap() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source binary 0034_02 c1=0x013300 {}", map.toString());
        //Verify the results
        assertEquals("1,51,0", map.get("c_1"));
        assertEquals("14", map.get("c_2"));
        assertEquals("2,-86", map.get("c_3"));
        assertEquals("-1", map.get("c_4"));
        assertEquals("14", map.get("c_5"));
        assertEquals("-1", map.get("c_6"));
    }

    @DisplayName("query source binary 0034_02 c1=0x310000")
    @Test
    void testPutOneResultSetToMap_0x310000() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_ONE0x310000, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source binary 0034_02 c1=0x310000 {}", map.toString());
        //Verify the results
        assertEquals("49,0,0", map.get("c_1"));
        assertEquals("49,48", map.get("c_2"));
        assertNull(map.get("c_3"));
        assertNull(map.get("c_4"));
        assertNull(map.get("c_5"));
        assertNull(map.get("c_6"));
    }

    @DisplayName("query source json 0052_10 c2=1")
    @Test
    void test_query_json_0052_10() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_0052_10, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source json 0052_10 c2=1 {}", map.toString());
        //Verify the results
        assertEquals("{\"key1\": \"value1\", \"key2\": \"value2\"}", map.get("c1"));
        assertEquals("1", map.get("c2"));
    }

    @DisplayName("query source char 0052_05 c_1=1")
    @Test
    void test_query_char_0052_05() {
        // Setup
        final List<Map<String, String>> result =
            jdbc.query(CreateTableSql.QUERY_0052_05, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source char 0052_05 c_1=1 {}", map.toString());
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
            jdbc.query(CreateTableSql.QUERY_0037_01, new HashMap<>(InitialCapacity.EMPTY),
                (rs, rowNum) -> resultSetHandler.putOneResultSetToMap(rs));
        final Map<String, String> map = result.get(0);
        log.info("query source char 0037_01 {}", map.toString());
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
