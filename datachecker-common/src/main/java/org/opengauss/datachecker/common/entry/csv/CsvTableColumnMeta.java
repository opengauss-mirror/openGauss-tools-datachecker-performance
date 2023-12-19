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

package org.opengauss.datachecker.common.entry.csv;

import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;

import java.util.Locale;
import java.util.Objects;

/**
 * CsvTableColumnMeta
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/11
 * @since ：11
 */
@Data
public class CsvTableColumnMeta {
    private String schema;
    private String table;
    private String column_data_type;
    private String column_type;
    private String column_name;
    private String column_key;
    private int column_index;

    /**
     * convert csv file table column meta ColumnsMetaData
     *
     * @return ColumnsMetaData
     */
    public ColumnsMetaData toColumnsMetaData() {
        ColumnsMetaData column = new ColumnsMetaData();
        column.setTableName(table)
              .setColumnName(Objects.nonNull(column_name) ? column_name.toLowerCase(Locale.ENGLISH) : "")
              .setColumnType(column_data_type)
              .setDataType(Objects.nonNull(column_type) ? column_type : column_data_type)
              .setOrdinalPosition(column_index)
              .setColumnKey(Objects.equals(column_key, ColumnKey.PRI.getCode()) ? ColumnKey.PRI : null);
        return column;
    }
}
