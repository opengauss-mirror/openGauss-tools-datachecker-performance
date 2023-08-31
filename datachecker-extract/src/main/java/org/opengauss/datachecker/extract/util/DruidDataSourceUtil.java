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
        log.debug("datasource getUsername : {}", dataSource.getUsername());
        log.debug("datasource getDbType : {}", dataSource.getDbType());
        log.debug("datasource getDriverClassName : {}", dataSource.getDriverClassName());
        log.debug("datasource getInitialSize : {}", dataSource.getInitialSize());
        log.debug("datasource getMinIdle : {}", dataSource.getMinIdle());
        log.debug("datasource getMaxActive : {}", dataSource.getMaxActive());
        log.debug("datasource getMaxWait : {}", dataSource.getMaxWait());
        log.debug("datasource getTimeBetweenEvictionRunsMillis : {}", dataSource.getTimeBetweenEvictionRunsMillis());
        log.debug("datasource getMinEvictableIdleTimeMillis : {}", dataSource.getMinEvictableIdleTimeMillis());
        log.debug("datasource getValidationQuery : {}", dataSource.getValidationQuery());
        log.debug("datasource isTestWhileIdle : {}", dataSource.isTestWhileIdle());
        log.debug("datasource isTestOnBorrow : {}", dataSource.isTestOnBorrow());
        log.debug("datasource isTestOnReturn : {}", dataSource.isTestOnReturn());
        log.debug("datasource isUseGlobalDataSourceStat : {}", dataSource.isUseGlobalDataSourceStat());
        log.debug("datasource isPoolPreparedStatements : {}", dataSource.isPoolPreparedStatements());
    }
}
