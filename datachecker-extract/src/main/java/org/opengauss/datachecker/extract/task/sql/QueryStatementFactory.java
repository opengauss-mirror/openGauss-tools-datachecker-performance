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
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.ExtractPrimaryKeyException;
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
     * @param checkPoint    checkPoint
     * @param tableMetadata tableMetadata
     * @return SliceQueryStatement
     */
    public AutoSliceQueryStatement createSliceQueryStatement(CheckPoint checkPoint, TableMetadata tableMetadata) {
        if (checkPoint.checkInvalidPrimaryKey(tableMetadata)) {
            String dataType = tableMetadata.getPrimaryMetas()
                                           .get(0)
                                           .getDataType();
            throw new ExtractPrimaryKeyException(
                "current not support primary key type  for this table " + tableMetadata.getTableName() + " dataType : "
                    + dataType);
        }
        return new SinglePrimaryAutoSliceQueryStatement(checkPoint);
    }

    /**
     * create slice query statement of single primary slice
     *
     * @return SliceQueryStatement
     */
    public SliceQueryStatement createSliceQueryStatement() {
        return new SinglePrimarySliceQueryStatement();
    }

    /**
     * create FullQueryStatement
     *
     * @return FullQueryStatement
     */
    public FullQueryStatement createFullQueryStatement() {
        return tableMetadata -> {
            final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
            String fullSql = sqlBuilder.isDivisions(false).isCsvMode(ConfigCache.isCsvMode())
                                       .builder();
            return new QuerySqlEntry(tableMetadata.getTableName(), fullSql, 0, tableMetadata.getTableRows());
        };
    }
}
