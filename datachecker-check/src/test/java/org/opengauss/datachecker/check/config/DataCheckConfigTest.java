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

package org.opengauss.datachecker.check.config;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.check.BaseTest;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Autowired;

/**
 * DataCheckConfigTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
class DataCheckConfigTest extends BaseTest {
    private static final Logger log = LogUtils.getLogger();
    @Autowired
    private DataCheckConfig dataCheckConfig;

    @Test
    void testGetCheckResultPaht() {
        final String checkResultPaht = dataCheckConfig.getCheckResultPath();
        log.info(checkResultPaht);
    }
}
