/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
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
import org.opengauss.datachecker.extract.task.CheckPoint;

import java.util.List;

/**
 * Union primary slice query statement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class UnionPrimaryAutoSliceQueryStatement implements AutoSliceQueryStatement {
    private final CheckPoint primaryCheckPoint;

    /**
     * create SinglePrimarySliceQueryStatement
     *
     * @param checkPoint checkPoint
     */
    public UnionPrimaryAutoSliceQueryStatement(CheckPoint checkPoint) {
        this.primaryCheckPoint = checkPoint;
    }

    @Override
    public List<QuerySqlEntry> builderByTaskOffset(TableMetadata tableMetadata, int slice) {
        return null;
    }

    @Override
    public Object[][] builderSlice(TableMetadata tableMetadata, int slice) {
        return null;
    }

    @Override
    public List<PointPair> getCheckPoint(String tableName, int slice) {
        return primaryCheckPoint.initUnionPrimaryCheckPointList(tableName);
    }
}
