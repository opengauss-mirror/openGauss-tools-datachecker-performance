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
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.exception.CommonException;
import org.opengauss.datachecker.common.util.EnumUtil;

import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.Locale;
import java.util.Objects;

/**
 * Table metadata information
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Data
@Accessors(chain = true)
@ToString
public class ColumnsMetaData implements Comparable<ColumnsMetaData> {
    public static final String AUTO_INCREMENT = "auto_increment";
    /**
     * Table
     */
    private String tableName;
    /**
     * Primary key column name
     */
    private String columnName;
    /**
     * Primary key column data type
     */
    private String columnType;
    /**
     * Primary key column data type
     */
    private String dataType;
    /**
     * Table field sequence number
     */
    private int ordinalPosition;
    /**
     * {@value ColumnKey#API_DESCRIPTION}
     */
    private ColumnKey columnKey;

    private String extra;

    public String getColumnMsg() {
        return " [" + columnName + " : " + columnType + "]";
    }

    public boolean isAutoIncrementColumn() {
        if (columnKey == null || columnKey != ColumnKey.PRI) {
            return false;
        }
        return !StringUtils.isEmpty(getExtra()) && getExtra().toLowerCase(Locale.ENGLISH).contains(AUTO_INCREMENT);
    }

    public static ColumnsMetaData parse(ResultSet rs) throws SQLException {
        ColumnsMetaData columnsMetaData = new ColumnsMetaData();
        columnsMetaData.setTableName(rs.getString(1)).setColumnName(rs.getString(2)).setOrdinalPosition(rs.getInt(3))
                       .setDataType(rs.getString(4)).setColumnType(rs.getString(5))
                       .setColumnKey(EnumUtil.valueOf(ColumnKey.class, rs.getString(6))).setExtra(rs.getString(7));
        return columnsMetaData;
    }

    @Override
    public int compareTo(ColumnsMetaData obj) {
        if (Objects.isNull(obj)) {
            return 1;
        }
        return this.ordinalPosition - obj.ordinalPosition;
    }
}