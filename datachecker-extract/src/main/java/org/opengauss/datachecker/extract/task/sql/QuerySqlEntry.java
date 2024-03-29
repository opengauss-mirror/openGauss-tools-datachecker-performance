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

package org.opengauss.datachecker.extract.task.sql;

import lombok.Getter;

/**
 * QuerySqlEntry
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/27
 * @since ：11
 */
@Getter
public class QuerySqlEntry {
    private String table;
    private String sql;
    private long start;
    private long offset;
    private String seqStart;
    private String seqEnd;

    /**
     * build query sql entry
     *
     * @param table  table name
     * @param sql    sql
     * @param start  start
     * @param offset offset
     */
    public QuerySqlEntry(String table, String sql, long start, long offset) {
        this.table = table;
        this.sql = sql;
        this.start = start;
        this.offset = offset;
    }

    public QuerySqlEntry(String table, String sql, Object start, Object offset) {
        this.table = table;
        this.sql = sql;
        if (start instanceof Long) {
            this.start = (long) start;
            this.offset = (long) offset;
        } else {
            this.seqStart = (String) start;
            this.seqEnd = (String) offset;
        }
    }

    @Override
    public String toString() {
        if (seqStart != null && !"".equals(seqStart)) {
            return " [" + table + "] - [" + seqStart + ":" + seqEnd + "] ";
        } else {
            return " [" + table + "] - [" + start + "," + offset + "] ";
        }
    }
}
