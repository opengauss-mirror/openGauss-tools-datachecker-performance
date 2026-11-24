/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.service;

import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.extract.vo.TableStatisticsInfo;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;
import org.springframework.util.CollectionUtils;

import java.util.ArrayList;
import java.util.List;

/**
 * ExtractMockTableService
 *
 * @author wang chao
 * @date 2022/5/8 19:27
 * @since 11
 **/
@Service
@Slf4j
public class ExtractMockTableService {
    private static final String SQL_QUERY_TABLE_STATISTICS = "SELECT TABLE_NAME tableName,SUM(table_rows) count, "
        + "concat(round(sum(data_length/1024/1024),2),'MB') as dataLength  "
        + " from information_schema.tables where table_schema='test' GROUP BY  table_name";

    private static final String SQL_QUERY_TABLE_STATISTICS_SUN = "SELECT 'ALL' tableName ,SUM(table_rows) count, "
        + "concat(round(sum(data_length/1024/1024),2),'MB') as dataLength"
        + " from information_schema.tables where table_schema='test' GROUP BY table_schema";

    @Autowired
    private JdbcTemplate jdbcTemplateMysql;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * createTable
     *
     * @param tableName tableName
     * @return create result
     */
    public String createTable(String tableName) {
        jdbcTemplateMysql.execute(MockMapper.CREATE.replace(":TABLENAME", tableName));
        return tableName;
    }

    /**
     * getAllTableInfo
     *
     * @return TableStatisticsInfo
     */
    public List<TableStatisticsInfo> getAllTableInfo() {
        final List<TableStatisticsInfo> tableNameList = queryTableStatisticsInfos(SQL_QUERY_TABLE_STATISTICS);
        if (CollectionUtils.isEmpty(tableNameList)) {
            return new ArrayList<>();
        }
        tableNameList.addAll(queryTableStatisticsInfos(SQL_QUERY_TABLE_STATISTICS_SUN));
        tableNameList.sort((o1, o2) -> (int) (o1.getCount() - o2.getCount()));
        return tableNameList;
    }

    private List<TableStatisticsInfo> queryTableStatisticsInfos(String querySql) {
        return jdbcTemplateMysql.query(querySql,
            (rs, rowNum) -> new TableStatisticsInfo(rs.getString("tableName"), rs.getLong("count"),
                rs.getString("dataLength")));
    }

    interface MockMapper {
        /**
         * create table sql
         */
        String CREATE = "CREATE TABLE :TABLENAME ( b_number VARCHAR(30) NOT NULL ,"
            + " b_type VARCHAR(20) NULL DEFAULT NULL ,"
            + " b_user VARCHAR(20) NULL DEFAULT NULL ,"
            + " b_int INT(10) NULL DEFAULT NULL, b_bigint BIGINT(19) NULL DEFAULT '0',"
            + " b_text TEXT NULL DEFAULT NULL ,"
            + " b_longtext LONGTEXT NULL DEFAULT NULL ,"
            + " b_date DATE NULL DEFAULT NULL, b_datetime DATETIME NULL DEFAULT NULL,"
            + " b_timestamp TIMESTAMP NULL DEFAULT NULL,"
            + " b_attr1 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr2 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr3 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr4 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr5 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr6 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr7 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr8 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr9 VARCHAR(255) NULL DEFAULT NULL ,"
            + " b_attr10 VARCHAR(255) NULL DEFAULT NULL ,"
            + " PRIMARY KEY (`b_number`) USING BTREE )  COLLATE='utf8mb4_0900_ai_ci'" + " ENGINE=InnoDB ;";

    }
}
