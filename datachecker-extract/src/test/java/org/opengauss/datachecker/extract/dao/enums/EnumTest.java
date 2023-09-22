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

package org.opengauss.datachecker.extract.dao.enums;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.enums.DataSourceType;
import org.opengauss.datachecker.common.util.EnumUtil;
import org.opengauss.datachecker.common.util.LogUtils;

/**
 * EnumTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
public class EnumTest {
    private static final Logger log = LogUtils.getLogger();
    @Test
    void testEnum() {
        DataSourceType type = DataSourceType.Sink;
        log.info("" + type);
        log.info("" + type.equals(DataSourceType.valueOf("Sink")));
        log.info("" + EnumUtil.valueOfIgnoreCase(DataSourceType.class, "Sink"));
        log.info("" + EnumUtil.valueOf(DataSourceType.class, "Sink"));
        log.info("" + EnumUtil.valueOf(DataSourceType.class, "Sink"));
        log.info("" + EnumUtil.valueOf(DataSourceType.class, "sink"));
        log.info("" + EnumUtil.valueOfIgnoreCase(DataSourceType.class, "sink"));
    }
}
