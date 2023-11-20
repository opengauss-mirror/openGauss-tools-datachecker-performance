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

package org.opengauss.datachecker.extract.util;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;

/**
 * DruidDataSourceUtil
 *
 * @author ：wangchao
 * @date ：Created in 2023/9/5
 * @since ：11
 */
public class DruidDataSourceUtil {
    private static final Logger log = LogUtils.getDebugLogger();

    public static void print(DruidDataSource dataSource) {
        log.debug("datasource getUrl : {}", dataSource.getUrl());
        log.debug("datasource Username : {} , DbType : {} -> {}  InitialSize : {} ,MinIdle : {}, MaxActive : {} ",
            dataSource.getUsername(), dataSource.getDbType(), dataSource.getDriverClassName(),
            dataSource.getInitialSize(), dataSource.getMinIdle(), dataSource.getMaxActive());
    }
}
