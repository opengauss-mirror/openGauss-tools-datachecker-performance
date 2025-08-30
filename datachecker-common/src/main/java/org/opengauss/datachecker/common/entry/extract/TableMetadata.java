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

package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.util.CollectionUtils;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Objects;

/**
 * Table metadata information
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Data
@Accessors(chain = true)
@ToString
public class TableMetadata {
    private Endpoint endpoint;
    private DataBaseType dataBaseType;
    private String schema;
    private boolean isOgCompatibilityB = false;
    /**
     * tableName
     */
    private String tableName;
    private String tableCollation;

    /**
     * Total table data
     */
    private long tableRows;

    /**
     * If config count_row this will be true.
     */
    private boolean realRows;
    private boolean isExistTableRows;

    private long avgRowLength;

    private long maxTableId = 0;
    private long tableHash = 0;

    /**
     * Primary key column properties
     */
    private List<ColumnsMetaData> primaryMetas;
    private ColumnsMetaData sliceColumn;

    /**
     * Table column properties
     */
    private List<ColumnsMetaData> columnsMetas;

    private ConditionLimit conditionLimit;

    /**
     * Judge if this table is auto increment, if this true and no conditionLimit configured,
     * you can use id between start and end.
     *
     * @return true if primary is auto increment.
     */
    public boolean isAutoIncrement() {
        if (primaryMetas == null || primaryMetas.size() != 1) {
            return false;
        }
        return primaryMetas.get(0).isAutoIncrementColumn();
    }

    /**
     * 增加联合主键选择机制
     *
     * @return ColumnsMetaData
     */
    public ColumnsMetaData getSliceColumn() {
        if (isSinglePrimary()) {
            sliceColumn = primaryMetas.get(0);
        }
        return sliceColumn;
    }

    /**
     * 当前是否是单一主键表
     *
     * @return
     */
    public boolean isSinglePrimary() {
        return !CollectionUtils.isEmpty(primaryMetas) && primaryMetas.size() == 1;
    }

    /**
     * judge if this table is union primary key table.
     *
     * @return true if primary is union primary key
     */
    public boolean isUnionPrimary() {
        return !CollectionUtils.isEmpty(primaryMetas) && primaryMetas.size() > 1;
    }

    /**
     * judge if this table is single col primary key table.
     *
     * @return true if primary is union primary key
     */
    public ColumnsMetaData getSinglePrimary() {
        if (hasPrimary()) {
            return primaryMetas.get(0);
        }
        return null;
    }

    public boolean hasPrimary() {
        return Objects.nonNull(primaryMetas) && !primaryMetas.isEmpty();
    }

    public boolean hasSinglePrimary() {
        return Objects.nonNull(primaryMetas) && primaryMetas.size() == 1;
    }

    /**
     * If primary key can use between to iteractor resultset.
     *
     * @return true if enable.
     */
    public boolean canUseBetween() {
        if (getConditionLimit() != null) {
            return false;
        }
        return isAutoIncrement();
    }

    public static TableMetadata parse(ResultSet rs) throws SQLException {
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setTableName(rs.getString(1));
        tableMetadata.setTableRows(rs.getLong(2));
        tableMetadata.setAvgRowLength(rs.getLong(3));
        return tableMetadata;
    }

    public static TableMetadata parse(ResultSet rs, String schema, Endpoint endpoint, DataBaseType databaseType)
        throws SQLException {
        return parse(rs).setSchema(schema).setEndpoint(endpoint).setDataBaseType(databaseType);
    }
}
