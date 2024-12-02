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
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.util.MetaDataUtil;

import java.util.ArrayList;
import java.util.List;
import java.util.Objects;

/**
 * single primary slice query statement
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class UnionPrimarySliceQueryStatement implements SliceQueryStatement {
    private final boolean isHalfOpenHalfClosed;

    /**
     * create SinglePrimarySliceQueryStatement
     */
    public UnionPrimarySliceQueryStatement() {
        // csv mode, slice data scope is full closed , but jdbc mode ,slice data scope is half open and half closed
        this.isHalfOpenHalfClosed = !Objects.equals(ConfigCache.getCheckMode(), CheckMode.CSV);
    }

    /**
     * build slice count sql entry
     *
     * @param tableMetadata tableMetadata
     * @param slice slice
     * @return sql entry
     */
    public QuerySqlEntry buildSliceCount(TableMetadata tableMetadata, SliceVo slice) {
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
        sqlBuilder.isDivisions(slice.getTotal() > 1);
        sqlBuilder.isFirstCondition(slice.getNo() == 1);
        sqlBuilder.isEndCondition(slice.getNo() == slice.getTotal());
        sqlBuilder.isHalfOpenHalfClosed(isHalfOpenHalfClosed);
        sqlBuilder.isCsvMode(ConfigCache.isCsvMode());
        ColumnsMetaData primaryKey = tableMetadata.getSinglePrimary();
        boolean isDigit = MetaDataUtil.isDigitPrimaryKey(primaryKey);
        Object offset = translateOffset(isDigit, slice.getBeginIdx());
        Object endOffset = translateOffset(isDigit, slice.getEndIdx());
        sqlBuilder.offset(offset, endOffset);
        return new QuerySqlEntry(slice.getTable(), sqlBuilder.countBuilder(), offset, endOffset);
    }

    @Override
    public QuerySqlEntry buildSlice(TableMetadata tableMetadata, SliceVo slice) {
        final SelectSqlBuilder sqlBuilder = new SelectSqlBuilder(tableMetadata);
        sqlBuilder.isDivisions(slice.getTotal() > 1);
        sqlBuilder.isFirstCondition(slice.getNo() == 1);
        sqlBuilder.isEndCondition(slice.getNo() == slice.getTotal());
        sqlBuilder.isHalfOpenHalfClosed(isHalfOpenHalfClosed);
        sqlBuilder.isCsvMode(ConfigCache.isCsvMode());
        ColumnsMetaData primaryKey = tableMetadata.getSinglePrimary();
        boolean isDigit = MetaDataUtil.isDigitPrimaryKey(primaryKey);
        Object offset = translateOffset(isDigit, slice.getBeginIdx());
        Object endOffset = translateOffset(isDigit, slice.getEndIdx());
        sqlBuilder.offset(offset, endOffset);
        return new QuerySqlEntry(slice.getTable(), sqlBuilder.builder(), offset, endOffset);
    }

    private Object translateOffset(boolean isDigit, String beginIdx) {
        return Objects.isNull(beginIdx) ? null : isDigit ? Long.valueOf(beginIdx) : beginIdx;
    }

    /**
     * build  slice select sql, if select count bigger than a large number,so we will select it by page select.
     * page select for example select * from where ... limit xxx offset xxx
     *
     * @param baseSliceSql slice sql entry
     * @param sliceCount slice total count
     * @param fetchSize page select fetch size
     * @return page select sql
     */
    public List<String> buildPageStatement(QuerySqlEntry baseSliceSql, int sliceCount, int fetchSize) {
        int totalPage = sliceCount / fetchSize + (sliceCount % fetchSize == 0 ? 0 : 1);
        List<String> statements = new ArrayList<>(totalPage);
        for (int i = 0; i < totalPage; i++) {
            StringBuilder sqlBuilder = new StringBuilder(baseSliceSql.getSql());
            sqlBuilder.append(" limit ").append(fetchSize).append(" offset ").append(i * fetchSize);
            statements.add(sqlBuilder.toString());
        }
        return statements;
    }
}
