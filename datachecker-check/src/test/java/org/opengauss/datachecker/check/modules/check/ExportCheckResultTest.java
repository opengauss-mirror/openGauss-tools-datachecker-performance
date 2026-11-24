/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.check;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.client.FeignClientService;

import static org.assertj.core.api.Assertions.assertThat;

class ExportCheckResultTest {
    private static final String init_path = "path";

    @BeforeEach
    void setUp() {
        ExportCheckResult.initEnvironment(init_path);
    }

    @Test
    void testExport() {
        // Setup
        final CheckDiffResult result =
            new CheckDiffResult(AbstractCheckDiffResultBuilder.builder());
        // Run the test
        ExportCheckResult.export(result);
        // Verify the results
    }

    @Test
    void testBackCheckResultDirectory() {
        // Setup
        // Run the test
        ExportCheckResult.backCheckResultDirectory();
        // Verify the results
    }

    @Test
    void testGetResultPath() {
        assertThat(ExportCheckResult.getResultPath()).isEqualTo(init_path + "\\result\\");
    }
}
