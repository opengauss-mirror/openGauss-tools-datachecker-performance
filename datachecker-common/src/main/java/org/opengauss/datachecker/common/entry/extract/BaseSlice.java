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
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.SliceLogType;

/**
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@Data
public class BaseSlice {
    private String schema;
    private String table;
    private String name;
    private SliceLogType type;
    private boolean isWholeTable = false;
    private Endpoint endpoint;
    /**
     * slice no
     */
    private int no;
    /**
     * slice count total
     */
    private int total;
    /**
     * slice of index begin value
     */
    private String beginIdx;
    /**
     * slice of index end value
     */
    private String endIdx;

    /**
     * number of records in the current slice obtaining table
     */
    private int fetchSize;

    /**
     * slice name build by schema,table name , total of slice , slice no <br>
     * schema_table_total_no
     *
     * @return name
     */
    public String getName() {
        if (name == null || name.isBlank()) {
            return table + "_" + total + "_" + no;
        } else {
            return name;
        }
    }

    public boolean isEmptyTable() {
        return total == 1 && fetchSize == 0;
    }

    public boolean isSlice() {
        return total > 1;
    }

    @Override
    public String toString() {
        return "schema=" + schema + ", table=" + table + ", name=" + name + ", type=" + type + ", no=" + no + ", total="
            + total + ", beginIdx=" + beginIdx + ", endIdx=" + endIdx + ", fetchSize=" + fetchSize;
    }
}
