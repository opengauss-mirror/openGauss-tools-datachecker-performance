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

package org.opengauss.datachecker.extract.config;

import com.alibaba.druid.pool.DruidDataSource;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Primary;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * DruidDataSourceConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Configuration
public class DruidDataSourceConfig {
    /**
     * build mysql DruidDataSource
     *
     * @return druidDataSourceMysql
     */
    @Primary
    @Bean("dataSourceMysql")
    @ConfigurationProperties(prefix = "spring.datasource.druid.mysql")
    public DataSource druidDataSourceMysql() {
        return new DruidDataSource();
    }

    /**
     * build mysql JdbcTemplate
     *
     * @param dataSourceMysql Mysql dataSource
     * @return JdbcTemplate
     */
    @Bean("jdbcTemplateMysql")
    public JdbcTemplate jdbcTemplateMysql(@Qualifier("dataSourceMysql") DataSource dataSourceMysql) {
        return new JdbcTemplate(dataSourceMysql);
    }

    /**
     * build OpenGauss DruidDataSource
     *
     * @return DruidDataSource
     */
    @Bean("dataSourceOpenGauss")
    @ConfigurationProperties(prefix = "spring.datasource.druid.opengauss")
    public DataSource druidDataSourceOpenGauss() {
        return new DruidDataSource();
    }

    /**
     * build OpenGauss JdbcTemplate
     *
     * @param dataSourceOpenGauss dataSourceOpenGauss
     * @return JdbcTemplate
     */
    @Bean("jdbcTemplateOpenGauss")
    public JdbcTemplate jdbcTemplateOpenGauss(@Qualifier("dataSourceOpenGauss") DataSource dataSourceOpenGauss) {
        return new JdbcTemplate(dataSourceOpenGauss);
    }
}
