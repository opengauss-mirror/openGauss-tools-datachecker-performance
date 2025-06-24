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

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.extract.task.CheckPoint;

/**
 * query statement builder factory
 * build FullQueryStatement and SliceQueryStatement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class QueryStatementFactory {
    /**
     * create SliceQueryStatement
     *
     * @param checkPoint checkPoint
     * @param tableName tableName
     * @return A new AutoSliceQueryStatement instance.
     */
    public AutoSliceQueryStatement createSliceQueryStatement(CheckPoint checkPoint, String tableName) {
        return new SinglePrimaryAutoSliceQueryStatement(checkPoint);
    }

    /**
     * create slice query statement of single primary slice
     *
     * @return A new SinglePrimarySliceQueryStatement instance.
     */
    public SliceQueryStatement createSliceQueryStatement() {
        return new SinglePrimarySliceQueryStatement();
    }

    /**
     * create slice query statement of union primary slice
     *
     * @return A new UnionPrimarySliceQueryStatement instance.
     */
    public UnionPrimarySliceQueryStatement createSlicePageQueryStatement() {
        return new UnionPrimarySliceQueryStatement();
    }

    /**
     * create UnionPrimaryAutoSliceQueryStatement
     *
     * @param checkPoint checkPoint
     * @return A new AutoSliceQueryStatement instance.
     */
    public AutoSliceQueryStatement createUnionPrimarySliceQueryStatement(CheckPoint checkPoint) {
        return new UnionPrimaryAutoSliceQueryStatement(checkPoint);
    }

    /**
     * create FullQueryStatement
     *
     * @return FullQueryStatement
     */
    public FullQueryStatement createFullQueryStatement() {
        return tableMetadata -> {
            final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
            String fullSql = sqlBuilder.isDivisions(false).isCsvMode(ConfigCache.isCsvMode()).builder();
            return new QuerySqlEntry(tableMetadata.getTableName(), fullSql, 0, tableMetadata.getTableRows());
        };
    }
}
