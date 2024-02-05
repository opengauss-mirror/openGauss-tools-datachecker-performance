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

package org.opengauss.datachecker.extract.dao;

import org.junit.jupiter.api.Test;

/**
 * QuerySqlTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/2/4
 * @since ：11
 */
public class QuerySqlTest {
    @Test
    public void testQueryTableNameList() {
        String query =
            "SELECT info.table_name FROM  information_schema.tables info WHERE  table_schema='%s' limit %d,%d";
        String test = String.format(query, "test", 0, 10);
        System.out.println(test);
    }

    @Test
    public void testQueryTableMeateList() {
        String query =
            " SELECT info.TABLE_SCHEMA tableSchema,info.table_name tableName,info.table_rows tableRows , info.avg_row_length avgRowLength "
                + " FROM information_schema.tables info WHERE info.TABLE_SCHEMA='%s' limit %d ,%d";
        String test = String.format(query, "test", 0, 10);
        System.out.println(test);
    }
}