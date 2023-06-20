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

import io.swagger.v3.oas.annotations.media.Schema;
import lombok.Builder;
import lombok.Data;
import lombok.ToString;
import lombok.experimental.Accessors;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;

/**
 * Table metadata information
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/14
 * @since ：11
 */
@Schema(name = "Table metadata information")
@Data
@Accessors(chain = true)
@ToString
public class TableMetadata {
    /**
     * tableName
     */
    private String tableName;

    /**
     * Total table data
     */
    private long tableRows;
    
    /**
     *  If config count_row this will be true.
     */
    private boolean realRows;

    private long avgRowLength;
    
    private long maxTableId = 0;

    /**
     * Primary key column properties
     */
    private List<ColumnsMetaData> primaryMetas;

    /**
     * Table column properties
     */
    private List<ColumnsMetaData> columnsMetas;

    private ConditionLimit conditionLimit;
    
    /**
     * Judge if this table is auto increment, if this true and no conditionLimit configured,
     * you can use id between start and end.
     * @return true if primary is auto increment.
     */
    public boolean isAutoIncrement() {
        if (primaryMetas == null || primaryMetas.size() != 1) {
            return false;
        }
        return primaryMetas.get(0).isAutoIncrementColumn();
    }
    
    /**
     * If primary key can use between to iteractor resultset.
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
}
