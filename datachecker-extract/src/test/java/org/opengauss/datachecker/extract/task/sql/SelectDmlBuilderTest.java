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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.dml.SelectDmlBuilder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * SelectDmlBuilderTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
public class SelectDmlBuilderTest extends MockTableMeta {
    private TableMetadata mockTableMetadata;
    private SelectDmlBuilder selectDmlBuilder;

    @BeforeEach
    void setUp() {
        mockTableMetadata = mockSingleTablePrimaryMetadata();
        selectDmlBuilder = new SelectDmlBuilder(DataBaseType.OG, true);
    }

    @DisplayName("openGauss no divisions single primary select SQL build")
    @Test
    void testSelectNoDivisionsSqlBuilder() {
        String result = selectDmlBuilder.columns(mockTableMetadata.getColumnsMetas())

                                        .conditionPrimary(mockTableMetadata.getPrimaryMetas().get(0))
                                        .schema(getSchema()).dataBaseType(DataBaseType.OG)
                                        .tableName(mockTableMetadata.getTableName()).build();
        // Verify the results
        assertThat(result).isEqualTo(
            "select `id`,`c_date_time`,`c_date_time_3`,`c_timestamp`,`c_date`,`c_time`,`c_year` from `test`.`t_data_checker_time_0018_01` where `id` in ( :primaryKeys )");
    }

}
