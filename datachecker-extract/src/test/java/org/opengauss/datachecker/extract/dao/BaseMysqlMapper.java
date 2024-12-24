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

package org.opengauss.datachecker.extract.dao;

import lombok.extern.slf4j.Slf4j;

import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.jdbc.JdbcTransaction;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.extract.data.mapper.MysqlMetaDataMapper;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.support.ResourcePropertySource;

/**
 * BaseMapperTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
public class BaseMysqlMapper extends BaseMapperTest<MysqlMetaDataMapper> {
    /**
     * 执行
     */
    private static Executor executor;
    /**
     * 配置
     */
    private static Configuration configuration;
    protected static String testDatabaseInitScript;
    protected static String testDataDir;
    protected static String baseMapperDs;

    static {
        try {
            testDatabaseInitScript = "mysql";
            testDataDir = "mysql_opgs";
            baseMapperDs = "test-" + testDatabaseInitScript + ".properties";
            System.out.println(baseMapperDs);
            // 定义一个配置
            configuration = new Configuration();
            configuration.setCacheEnabled(false);
            configuration.setLazyLoadingEnabled(false);
            configuration.setAggressiveLazyLoading(true);
            configuration.setDefaultStatementTimeout(20);
            // 读取ces数据库 数据源配置并解析
            PropertySource propertySource = new ResourcePropertySource(
                new ClassPathResource("init/" + testDatabaseInitScript + "/" + baseMapperDs));

            // 设置数据库链接
            UnpooledDataSource dataSource = new UnpooledDataSource();
            dataSource.setDriver(getProperty(propertySource, "driverClassName"));
            dataSource.setUrl(getProperty(propertySource, "url"));
            dataSource.setUsername(getProperty(propertySource, "username"));
            dataSource.setPassword(getProperty(propertySource, "password"));
            // 设置是我（测试设置事务不提交）
            Transaction transaction =
                new JdbcTransaction(dataSource, TransactionIsolationLevel.READ_UNCOMMITTED, false);
            // 设置执行
            executor = configuration.newExecutor(transaction);

        } catch (Exception exception) {
            log.error("load mybatis configuration error:", exception);
        }
    }

    public BaseMysqlMapper(String mapperName) {
        super(mapperName, configuration, executor, testDataDir);
    }

    @Override
    public MysqlMetaDataMapper getMapper() {
        return super.getMapper();
    }
}