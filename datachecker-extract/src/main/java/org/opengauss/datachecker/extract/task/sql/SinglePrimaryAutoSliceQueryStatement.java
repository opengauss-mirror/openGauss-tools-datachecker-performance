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
import org.opengauss.datachecker.extract.task.CheckPoint;

import java.util.List;

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
        return null;
    }

    @Override
    public Object[][] builderSlice(TableMetadata tableMetadata, int slice) {
        return null;
    }

    @Override
    public List<PointPair> getCheckPoint(String tableName, int slice) {
        return singlePrimaryCheckPoint.initCheckPointList(tableName, slice);
    }
}
