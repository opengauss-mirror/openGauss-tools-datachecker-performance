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
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.dml.UpdateDmlBuilder;

import static org.assertj.core.api.AssertionsForClassTypes.assertThat;

/**
 * SelectDmlBuilderTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
public class UpdateDmlBuilderTest extends MockTableMeta {
    private TableMetadata mockTableMetadata;
    private UpdateDmlBuilder updateDmlBuilder;

    @BeforeEach
    void setUp() {
        mockTableMetadata = mockSingleTablePrimaryMetadata();
        updateDmlBuilder = new UpdateDmlBuilder(DataBaseType.OG, true);
    }

    @DisplayName("openGauss update SQL build")
    @Test
    void testSelectNoDivisionsSqlBuilder() {
        String result =
            updateDmlBuilder.metadata(mockTableMetadata).columnsValues(getValues()).dataBaseType(DataBaseType.OG)
                            .schema(getSchema()).tableName(mockTableMetadata.getTableName()).build();
        // Verify the results
        assertThat(result).isEqualTo(
            "update \"test\".\"t_data_checker_time_0018_01\" set c_date_time='2022-09-18 17:39:51' , c_date_time_3='2022-09-19 10:13:37.741' , c_timestamp='2022-09-18 17:39:51' , c_date='2022-09-18' , c_time='17:39:49' , c_year=2022  where id=15 ;");
    }

}
