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

package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.extract.util.HashHandler;

import java.util.List;
import java.util.Map;

/**
 * ResultSetHashHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/5
 * @since ：11
 */
public class ResultSetHashHandler {
    private final HashHandler hashHandler = new HashHandler();

    /**
     * <pre>
     * Obtain the primary key information in the ResultSet according to the primary key name of the table.
     * Obtain all field information in the ResultSet according to the set of table field names.
     * And hash the primary key value and the record value.
     * The calculation result is encapsulated as a RowDataHash object
     * </pre>
     *
     * @param primary primary list
     * @param columns column list
     * @param rowData Query data set
     * @return Returns the hash calculation result of extracted data
     */
    public RowDataHash handler(List<String> primary, List<String> columns, Map<String, String> rowData) {
        long rowHash = hashHandler.xx3Hash(rowData, columns);
        String primaryValue = hashHandler.value(rowData, primary);
        long primaryHash = hashHandler.xx3Hash(rowData, primary);
        RowDataHash hashData = new RowDataHash();
        hashData.setKey(primaryValue).setKHash(primaryHash).setVHash(rowHash);
        return hashData;
    }
}
