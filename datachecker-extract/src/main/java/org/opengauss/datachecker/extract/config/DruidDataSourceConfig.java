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
import com.alibaba.druid.support.http.StatViewServlet;
import com.alibaba.druid.support.http.WebStatFilter;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.web.servlet.FilterRegistrationBean;
import org.springframework.boot.web.servlet.ServletRegistrationBean;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.jdbc.core.JdbcTemplate;

import javax.sql.DataSource;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;

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
     * build extract DruidDataSource
     *
     * @return DruidDataSource
     */
    @Bean("dataSource")
    @ConfigurationProperties(prefix = "spring.datasource.druid.datasourceone")
    public DataSource druidDataSource() {
        return new DruidDataSource();
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

    /**
     * Background monitoring
     * Configure the servlet of the Druid monitoring management background.
     * There is no web.xml file when the servlet container is built in. Therefore ,the servlet registration mode of
     * Spring Boot is used.
     *
     * @return return ServletRegistrationBean
     */
    @Bean
    public ServletRegistrationBean<StatViewServlet> initServletRegistrationBean() {
        ServletRegistrationBean<StatViewServlet> bean =
            new ServletRegistrationBean<>(new StatViewServlet(), "/druid/*");
        HashMap<String, String> initParameters = new HashMap<>(InitialCapacity.CAPACITY_1);
        initParameters.put("allow", "");
        bean.setInitParameters(initParameters);
        return bean;
    }

    /**
     * Configuring the filter for druid monitoring - web monitoring
     * WebStatFilter: used to configure management association monitoring statistice between web and druid data sources.
     *
     * @return return FilterRegistrationBean
     */
    @Bean
    public FilterRegistrationBean webStatFilter() {
        FilterRegistrationBean bean = new FilterRegistrationBean();
        bean.setFilter(new WebStatFilter());

        // exclusions: sets the requests to be filtered out so that statistics are not collected.
        Map<String, String> initParams = new HashMap<>(InitialCapacity.CAPACITY_1);
        // this things don't count.
        initParams.put("exclusions", "*.js,*.css,/druid/*,/jdbc/*");
        bean.setInitParameters(initParams);

        // "/*" indicates that all requests are filtered.
        bean.setUrlPatterns(Arrays.asList("/*"));
        return bean;
    }
}
