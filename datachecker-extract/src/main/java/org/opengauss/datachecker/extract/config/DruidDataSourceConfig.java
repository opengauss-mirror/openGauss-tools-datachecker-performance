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
import com.alibaba.druid.spring.boot.autoconfigure.DruidDataSourceBuilder;

import cn.hutool.core.util.StrUtil;
import lombok.extern.slf4j.Slf4j;

import org.apache.ibatis.session.SqlSessionFactory;
import org.mybatis.spring.SqlSessionFactoryBean;
import org.mybatis.spring.SqlSessionTemplate;
import org.mybatis.spring.annotation.MapperScan;
import org.opengauss.datachecker.extract.constants.ExtConstants;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.io.support.PathMatchingResourcePatternResolver;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;

/**
 * DruidDataSourceConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Slf4j
@Configuration
@ConditionalOnProperty(prefix = "spring.extract", name = "dataLoadMode", havingValue = "jdbc")
@ConditionalOnBean({ExtractProperties.class})
@MapperScan(basePackages = "org.opengauss.datachecker.extract.data.mapper", sqlSessionFactoryRef = "sqlSessionFactory")
public class DruidDataSourceConfig implements DataSourceConfig {
    /**
     * build extract DruidDataSource
     *
     * @return DruidDataSource
     */
    @Bean(name = "dataSource")
    @ConfigurationProperties(prefix = "spring.datasource.druid")
    public DataSource druidDataSource() {
        DruidDataSource druidDataSource = DruidDataSourceBuilder.create().build();
        druidDataSource.setMaxPoolPreparedStatementPerConnectionSize(20);
        druidDataSource.setMaxActive(20);
        druidDataSource.setMinIdle(10);
        String isEnableStdinPassword = System.getenv(ExtConstants.ENABLE_ENV_PASSWORD);
        if (StrUtil.equalsIgnoreCase(isEnableStdinPassword, ExtConstants.ENABLE_TRUE)) {
            druidDataSource.setPassword(System.getenv(ExtConstants.SPRING_DATASOURCE_PASSWORD));
            log.info("enable system env password true");
        }
        return druidDataSource;
    }

    /**
     * build extract SqlSessionFactory
     *
     * @param dataSource extract dataSource
     * @return SqlSessionFactory
     * @throws Exception build sqlSessionFactory exception
     */
    @Bean(name = "sqlSessionFactory")
    public SqlSessionFactory primarySqlSessionFactory(@Qualifier("dataSource") DataSource dataSource) throws Exception {
        SqlSessionFactoryBean bean = new SqlSessionFactoryBean();
        bean.setDataSource(dataSource);
        bean.setMapperLocations(new PathMatchingResourcePatternResolver().getResources("classpath*:mapper/*.xml"));
        return bean.getObject();
    }

    /**
     * build extract SqlSessionTemplate
     *
     * @param sessionFactory SqlSessionFactory
     * @return SqlSessionTemplate
     */
    @Bean("sqlSessionTemplate")
    public SqlSessionTemplate primarySqlSessionTemplate(
        @Qualifier("sqlSessionFactory") SqlSessionFactory sessionFactory) {
        return new SqlSessionTemplate(sessionFactory);
    }

    /**
     * build extract JdbcTemplate
     *
     * @param dataSource DataSource
     * @return JdbcTemplate
     */
    @Bean("jdbcTemplate")
    public JdbcTemplate jdbcTemplateOne(@Qualifier("dataSource") DataSource dataSource) {
        JdbcTemplate jdbcTemplate = new JdbcTemplate(dataSource);
        jdbcTemplate.setFetchSize(20000);
        return jdbcTemplate;
    }
}
