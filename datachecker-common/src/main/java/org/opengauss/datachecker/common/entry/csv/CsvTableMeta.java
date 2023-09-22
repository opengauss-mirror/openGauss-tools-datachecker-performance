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
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

/**
 * CsvTableMeta
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/11
 * @since ：11
 */
@Data
public class CsvTableMeta {
    private String schema;
    private String table;
    private int count;
    private boolean contain_primary_key;

    /**
     * convert csv file table column meta TableMetadata
     *
     * @return TableMetadata
     */
    public TableMetadata toTableMetadata() {
        TableMetadata tableMetadata = new TableMetadata();
        tableMetadata.setSchema(schema).setTableName(table).setTableRows(count);
        return tableMetadata;
    }
}
