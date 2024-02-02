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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * DatabaseTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
class DatabaseTest {
    private Database databaseUnderTest;

    @BeforeEach
    void setUp() {
        databaseUnderTest = new Database();
        databaseUnderTest.databaseType = DataBaseType.MS;
        databaseUnderTest.endpoint = Endpoint.SOURCE;
    }

    @Test
    void testToString() {
        // Setup
        databaseUnderTest.schema = "schema";
        // Run the test
        final String result = databaseUnderTest.toString();

        // Verify the results
        assertThat(result).isEqualTo("Database(schema=schema, databaseType=MS, endpoint=SOURCE)");
    }

    @Test
    void test2ToString() {
        // Setup
        databaseUnderTest.schema = "_schema_tmp";
        // Run the test
        final String result = databaseUnderTest.toString();

        // Verify the results
        assertThat(result).isEqualTo("Database(schema=schema, databaseType=MS, endpoint=SOURCE)");
    }
}
