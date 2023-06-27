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

import static org.assertj.core.api.AssertionsForInterfaceTypes.assertThat;

/**
 * SelectSqlBuilderTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/2
 * @since ：11
 */
@ExtendWith(MockitoExtension.class)
class SelectSqlBuilderTest extends MockTableMeta {
    private TableMetadata mockTableMetadata;
    private SelectSqlBuilder selectSqlBuilder;

    @BeforeEach
    void setUp() {
        mockTableMetadata = mockSingleTablePrimaryMetadata();
        selectSqlBuilder = new SelectSqlBuilder(mockTableMetadata);
    }

    /**
     * testBuilder
     */
    @DisplayName("openGauss no divisions single primary select SQL build")
    @Test
    void testSelectNoDivisionsSqlBuilder() {
        String result = selectSqlBuilder.isDivisions(false).builder();
        // Verify the results
        assertThat(result).isEqualTo(
            "SELECT \"id\",\"c_date_time\",\"c_date_time_3\",\"c_timestamp\",\"c_date\",\"c_time\",\"c_year\" FROM \"test\".\"t_data_checker_time_0018_01\"");
    }
    @DisplayName("mysql no divisions single primary select SQL build")
    @Test
    void testMysqlSelectNoDivisionsSqlBuilder() {
        String result = selectSqlBuilder.isDivisions(false).builder();
        // Verify the results
        assertThat(result).isEqualTo(
            "SELECT `id`,`c_date_time`,`c_date_time_3`,`c_timestamp`,`c_date`,`c_time`,`c_year` FROM `test`.`t_data_checker_time_0018_01`");
    }
    @DisplayName("openGauss divisions single primary select SQL build")
    @Test
    void testSelectDivisionsSqlBuilder() {
        String result = selectSqlBuilder.isDivisions(false)
                                        .buildSelectSqlOffset(mockTableMetadata, 0, 12);
        // Verify the results
        assertThat(result).isEqualTo(
            "SELECT \"id\",\"c_date_time\",\"c_date_time_3\",\"c_timestamp\",\"c_date\",\"c_time\",\"c_year\" FROM \"test\".\"t_data_checker_time_0018_01\" order by \"id\" asc LIMIT 0,12");
    }
    @DisplayName("mysql divisions single primary select SQL build")
    @Test
    void testMysqlSelectDivisionsSqlBuilder() {
        String result = selectSqlBuilder.isDivisions(false)
                                        .buildSelectSqlOffset(mockTableMetadata, 0, 12);
        // Verify the results
        assertThat(result).isEqualTo(
            "SELECT `id`,`c_date_time`,`c_date_time_3`,`c_timestamp`,`c_date`,`c_time`,`c_year` FROM `test`.`t_data_checker_time_0018_01` order by `id` asc LIMIT 0,12");
    }
}
