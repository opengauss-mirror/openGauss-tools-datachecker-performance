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

package org.opengauss.datachecker.extract.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.ExtractApplication;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.context.ApplicationContext;
import org.springframework.jdbc.core.JdbcTemplate;

import java.sql.SQLException;

/**
 * DataSourceTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@SpringBootTest(classes = ExtractApplication.class)
public class DataSourceTest {
    private static final Logger log = LogUtils.getLogger();
    @Autowired
    private ApplicationContext applicationContext;

    @Test
    public void contextLoadTest() throws SQLException {
        DruidDataSource dataSourceOne = null;
        final Object dataSourceObject = applicationContext.getBean("dataSourceOne");
        if (dataSourceObject instanceof DruidDataSource) {
            dataSourceOne = (DruidDataSource) dataSourceObject;
        }
        log.info("dataSourceOne " + dataSourceOne.getClass());
        log.info("dataSourceOne " + dataSourceOne.getConnection());
        log.info("druid dataSourceOne getMaxActive " + dataSourceOne.getMaxActive());
        log.info("druid dataSourceOne getInitialSize " + dataSourceOne.getInitialSize());
        dataSourceOne.close();
    }

    @Test
    public void JdbcTemplateTest() {
        JdbcTemplate jdbcTemplateOne = null;
        final Object jdbcTemplateObject = applicationContext.getBean("JdbcTemplateOne");
        if (jdbcTemplateObject instanceof JdbcTemplate) {
            jdbcTemplateOne = (JdbcTemplate) jdbcTemplateObject;
        }
    }
}
