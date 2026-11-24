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

import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * QueryDataWapper
 *
 * @author ：wangchao
 * @date ：Created in 2022/8/2
 * @since ：11
 */
public class QueryDataWapper {
    /**
     * query table data
     *
     * @param jdbcTemplate jdbcTemplate
     * @param sql          sql
     * @param tableName    tableName
     * @return data
     */
    public List<String> queryPrimaryValues(JdbcTemplate jdbcTemplate, String sql, String tableName) {
        Map<String, Object> map = new HashMap<>(InitialCapacity.CAPACITY_1);
        NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
        final String execSql = sql.replace(":table", tableName);
        return jdbc.queryForList(execSql, map, String.class);
    }
}
