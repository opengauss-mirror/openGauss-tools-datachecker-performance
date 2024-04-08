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
import org.opengauss.datachecker.extract.task.CheckPoint;

import java.util.LinkedList;
import java.util.List;
import java.util.stream.IntStream;

/**
 * single primary slice query statement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class SinglePrimaryAutoSliceQueryStatement implements AutoSliceQueryStatement {
    private final CheckPoint singlePrimaryCheckPoint;

    /**
     * create SinglePrimarySliceQueryStatement
     *
     * @param checkPoint checkPoint
     */
    public SinglePrimaryAutoSliceQueryStatement(CheckPoint checkPoint) {
        this.singlePrimaryCheckPoint = checkPoint;
    }

    @Override
    public List<QuerySqlEntry> builderByTaskOffset(TableMetadata tableMetadata, int slice) {
        List<QuerySqlEntry> querySqlList = new LinkedList<>();
        List<Object> checkPointList = singlePrimaryCheckPoint.initCheckPointList(tableMetadata, slice);
        final Object[][] taskOffset;
        if (singlePrimaryCheckPoint.checkPkNumber(tableMetadata)) {
            taskOffset = singlePrimaryCheckPoint.translateBetween(checkPointList);
        } else {
            taskOffset = singlePrimaryCheckPoint.translateBetweenString(checkPointList);
        }
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
        sqlBuilder.isDivisions(taskOffset.length > 1)
                .isCsvMode(ConfigCache.isCsvMode());
        String tableName = tableMetadata.getTableName();
        IntStream.range(0, taskOffset.length).forEach(idx -> {
            sqlBuilder.isFullCondition(idx == taskOffset.length - 1).offset(taskOffset[idx][0], taskOffset[idx][1]);
            QuerySqlEntry entry =
                    new QuerySqlEntry(tableName, sqlBuilder.builder(), taskOffset[idx][0], taskOffset[idx][1]);
            querySqlList.add(entry);
        });
        return querySqlList;
    }

    @Override
    public Object[][] builderSlice(TableMetadata tableMetadata, int slice) {
        List<Object> checkPointList = singlePrimaryCheckPoint.initCheckPointList(tableMetadata, slice);
        final Object[][] taskOffset;
        if (singlePrimaryCheckPoint.checkPkNumber(tableMetadata)) {
            taskOffset = singlePrimaryCheckPoint.translateBetween(checkPointList);
        } else {
            taskOffset = singlePrimaryCheckPoint.translateBetweenString(checkPointList);
        }
        return taskOffset;
    }

    @Override
    public List<Object> getCheckPoint(TableMetadata tableMetadata, int slice) {
        return singlePrimaryCheckPoint.initCheckPointList(tableMetadata, slice);
    }
}
