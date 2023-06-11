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
import org.opengauss.datachecker.common.entry.enums.ColumnKey;

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
public class ColumnsMetaData {
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

    public String getColumnMsg() {
        return " [" + columnName + " : " + columnType + "]";
    }
}

