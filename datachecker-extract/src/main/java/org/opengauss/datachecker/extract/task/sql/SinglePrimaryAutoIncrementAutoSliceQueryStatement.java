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

import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.TaskUtilHelper;
import org.opengauss.datachecker.extract.task.CheckPoint;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * single primary slice query statement ,and the primary key is AutoIncrement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class SinglePrimaryAutoIncrementAutoSliceQueryStatement implements AutoSliceQueryStatement {
    private final CheckPoint checkPoint;

    public SinglePrimaryAutoIncrementAutoSliceQueryStatement(CheckPoint checkPoint) {
        this.checkPoint = checkPoint;
    }

    @Override
    public List<QuerySqlEntry> builderByTaskOffset(TableMetadata tableMetadata, int slice) {
        if (!tableMetadata.isAutoIncrement()) {
            return null;
        }
        updateAutoIncrement(tableMetadata);
        final int[][] taskOffset = new TaskUtilHelper(tableMetadata, slice).calcAutoTaskOffset();

        List<QuerySqlEntry> querySqlList = new LinkedList<>();
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
        sqlBuilder.isDivisions(taskOffset.length > 1);
        IntStream.range(0, taskOffset.length).forEach(idx -> {
            sqlBuilder.offset(taskOffset[idx][0], taskOffset[idx][1]);
            querySqlList.add(new QuerySqlEntry(tableMetadata.getTableName(), sqlBuilder.builder(), taskOffset[idx][0],
                taskOffset[idx][1]));
        });
        return querySqlList;
    }

    @Override
    public Object[][] builderSlice(TableMetadata tableMetadata, int slice) {
        return new Object[0][];
    }

    @Override
    public List<PointPair> getCheckPoint(TableMetadata tableMetadata, int slice) {
        return null;
    }

    private void updateAutoIncrement(TableMetadata tableMetadata) {
        long rowCount = checkPoint.queryRowsOfAutoIncrementTable(tableMetadata);
        long maxId = checkPoint.queryMaxIdOfAutoIncrementTable(tableMetadata);
        tableMetadata.setRealRows(true);
        tableMetadata.setMaxTableId(maxId);
        tableMetadata.setTableRows(rowCount);
    }
}
