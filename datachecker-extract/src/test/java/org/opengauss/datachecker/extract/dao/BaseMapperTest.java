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

import com.alibaba.fastjson.JSONObject;
import lombok.extern.slf4j.Slf4j;
import org.apache.commons.io.IOUtils;
import org.apache.ibatis.binding.MapperProxyFactory;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.TransactionIsolationLevel;
import org.apache.ibatis.session.defaults.DefaultSqlSession;
import org.apache.ibatis.transaction.Transaction;
import org.apache.ibatis.transaction.jdbc.JdbcTransaction;
import org.opengauss.datachecker.common.exception.ExpectTableDataNotFountException;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

import java.io.IOException;
import java.io.InputStream;
import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.nio.charset.StandardCharsets;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * BaseMapperTest
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/22
 * @since ：11
 */
@Slf4j
public class BaseMapperTest<T> {
    private static final String mysqlUrlPrefix = "jdbc:mysql://";
    private static final String openGaussUrlPrefix = "jdbc:opengauss://";
    private static final String oracleUrlPrefix = "jdbc:oracle:thin:@";
    /**
     * mapper接口类（持久层接口）
     */
    protected T mapper;
    protected MapperProxyFactory<T> mapperProxyFactory;
    /**
     * 数据库连接
     */
    private SqlSession sqlSession;
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

    static {
        try {
            PropertySource rootProperty = new ResourcePropertySource(new ClassPathResource("test.properties"));
            testDatabaseInitScript = getProperty(rootProperty, "test-database-init-script");
            testDataDir = getProperty(rootProperty, "test-data-dir");
            String baseMapperDs = "test-" + testDatabaseInitScript + ".properties";
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

            initTestDatabaseScript(propertySource);
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

    private static String getProperty(PropertySource rootProperty, String propertyKey) {
        return rootProperty.getProperty(propertyKey)
                           .toString();
    }

    private static void initTestDatabaseScript(PropertySource propertySource) {
        try {
            UnpooledDataSource dataSource = new UnpooledDataSource();
            String url = getProperty(propertySource, "url");
            dataSource.setUrl(Objects.equals(testDatabaseInitScript, "mysql") ? getInitScriptUrl(url) : url);
            dataSource.setDriver(getProperty(propertySource, "driverClassName"));
            dataSource.setUsername(getProperty(propertySource, "username"));
            dataSource.setPassword(getProperty(propertySource, "password"));
            Connection connection = dataSource.getConnection();
            ScriptRunner sc = new ScriptRunner(connection);
            String databaseScriptPath = "init/" + testDatabaseInitScript + "/sql/create_db.sql";
            Reader reader = new InputStreamReader(new ClassPathResource(databaseScriptPath).getInputStream());
            sc.runScript(reader);
            connection.close();
        } catch (Exception exc) {
            log.error("load test database init script error:", exc);
        }
    }

    public BaseMapperTest(String mapperName) {
        try {
            // 解析mapper文件
            Resource mapperResource = new ClassPathResource(mapperName);
            XMLMapperBuilder xmlMapperBuilder =
                new XMLMapperBuilder(mapperResource.getInputStream(), configuration, mapperResource.toString(),
                    configuration.getSqlFragments());
            xmlMapperBuilder.parse();
            sqlSession = new DefaultSqlSession(configuration, executor, false);
            ParameterizedType pt = (ParameterizedType) this.getClass()
                                                           .getGenericSuperclass();
            mapperProxyFactory = new MapperProxyFactory<>((Class<T>) (pt.getActualTypeArguments()[0]));
            mapper = mapperProxyFactory.newInstance(sqlSession);
        } catch (Exception ex) {
            log.error("build base mapper error", ex);
        }
    }

    /**
     * 加载数据库待测试表结构以及表数据
     *
     * @param scriptPath 指定表测试脚本路径
     */
    public void loadTestSqlScript(String scriptPath) {
        try {
            scriptPath = testDataDir + "/sql/" + scriptPath;
            execTestSqlScript(scriptPath);
        } catch (Exception exc) {
            log.error("load test sql script error:", exc);
        }
    }

    /**
     * execTestSqlScript
     *
     * @param scriptPath scriptPath
     */
    public void execTestSqlScript(String scriptPath) {
        try {
            ScriptRunner sc = new ScriptRunner(sqlSession.getConnection());
            Reader reader = new InputStreamReader(new ClassPathResource(scriptPath).getInputStream());
            sc.runScript(reader);
        } catch (Exception exc) {
            log.error("load test sql script error:", exc);
        }
    }

    private static String getInitScriptUrl(String defaultUrl) {
        String scriptUrl;
        if (defaultUrl.contains(mysqlUrlPrefix)) {
            String[] split1 = defaultUrl.replace(mysqlUrlPrefix, "")
                                        .split("/");
            scriptUrl = mysqlUrlPrefix + split1[0];
        } else if (defaultUrl.contains(openGaussUrlPrefix)) {
            String[] split1 = defaultUrl.replace(openGaussUrlPrefix, "")
                                        .split("/");
            scriptUrl = openGaussUrlPrefix + split1[0] + "/postgres";
        } else if (defaultUrl.contains(oracleUrlPrefix)) {
            String[] split1 = defaultUrl.replace(oracleUrlPrefix, "")
                                        .split("/");
            scriptUrl = oracleUrlPrefix + split1[0];
        } else {
            throw new ExtractDataAccessException("暂不支持该类型JDBC URL 格式 : " + defaultUrl);
        }
        System.out.println(scriptUrl);
        log.info("init database url scriptUrl=[{}] ", scriptUrl);
        return scriptUrl;
    }

    /**
     * 返回Mybatis Mapper接口实例
     *
     * @return
     */
    public T getMapper() {
        return mapper;
    }

    /**
     * 加载表预期结果对象
     *
     * @param tableName tableName
     * @return
     */
    public List<Map<String, String>> expect(String tableName) {
        try (InputStream inputStream = new ClassPathResource(
            testDataDir + "/expect/" + tableName + ".json").getInputStream();) {
            return JSONObject.parseObject(IOUtils.toString(inputStream, String.valueOf(StandardCharsets.UTF_8)),
                List.class);
        } catch (IOException ex) {
            throw new ExpectTableDataNotFountException(tableName);
        }
    }

    /**
     * 删除测试数据库
     */
    public void dropTestDb() {
        execTestSqlScript("init/" + testDatabaseInitScript + "/sql/drop.sql");
    }

    /**
     * 关闭测试结果集
     *
     * @param resultSet resultSet
     */
    public void close(ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException sql) {
            }
        }
    }

    /**
     * 关闭测试 preparedStatement
     *
     * @param preparedStatement preparedStatement
     */
    public void close(PreparedStatement preparedStatement) {
        if (preparedStatement != null) {
            try {
                preparedStatement.close();
            } catch (SQLException sql) {
            }
        }
    }

    /**
     * 获取测试连接
     *
     * @return
     */
    public Connection getConnection() {
        return sqlSession.getConnection();
    }
}