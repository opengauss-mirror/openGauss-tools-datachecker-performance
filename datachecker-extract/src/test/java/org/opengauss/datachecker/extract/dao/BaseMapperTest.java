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
import org.apache.ibatis.binding.MapperProxyFactory;
import org.apache.ibatis.builder.xml.XMLMapperBuilder;
import org.apache.ibatis.datasource.unpooled.UnpooledDataSource;
import org.apache.ibatis.executor.Executor;
import org.apache.ibatis.jdbc.ScriptRunner;
import org.apache.ibatis.session.Configuration;
import org.apache.ibatis.session.SqlSession;
import org.apache.ibatis.session.defaults.DefaultSqlSession;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractJuintTestException;
import org.springframework.core.env.PropertySource;
import org.springframework.core.io.ClassPathResource;
import org.springframework.core.io.Resource;
import org.springframework.core.io.support.ResourcePropertySource;

import java.io.InputStreamReader;
import java.io.Reader;
import java.lang.reflect.ParameterizedType;
import java.sql.Connection;
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
    private final SqlSession sqlSession;

    protected String testDatabaseInitScript;
    protected String testDataDir;

    protected static String getProperty(PropertySource rootProperty, String propertyKey) {
        return Objects.requireNonNull(rootProperty.getProperty(propertyKey))
                      .toString();
    }

    protected static void initTestDatabaseScript(String baseMapperDs,String testDatabaseInitScript) {
        try {
            PropertySource propertySource = new ResourcePropertySource(
                new ClassPathResource("init/" + testDatabaseInitScript + "/" + baseMapperDs));
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

    public BaseMapperTest(String mapperName, Configuration configuration, Executor executor, String testDataDir) {
        try {
            this.testDataDir = testDataDir;
            // 解析mapper文件
            Resource mapperResource = new ClassPathResource(mapperName);
            XMLMapperBuilder xmlMapperBuilder =
                new XMLMapperBuilder(mapperResource.getInputStream(), configuration, mapperResource.toString(),
                    configuration.getSqlFragments());
            xmlMapperBuilder.parse();
            sqlSession = new DefaultSqlSession(configuration, executor, false);
            ParameterizedType pt = (ParameterizedType) ((Class) this.getClass()
                                                                    .getGenericSuperclass()).getGenericSuperclass();
            mapperProxyFactory = new MapperProxyFactory<>((Class<T>) (pt.getActualTypeArguments()[0]));
            mapper = mapperProxyFactory.newInstance(sqlSession);
        } catch (Exception ex) {
            log.error("build base mapper error", ex);
            throw new ExtractJuintTestException("build base mapper error : ");
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
     * @return T
     */
    public T getMapper() {
        return mapper;
    }

    /**
     * 删除测试数据库
     */
    public void dropTestDb(String testDatabaseInitScript) {
        SqlScriptUtils.execTestSqlScript(getConnection(), "init/" + testDatabaseInitScript + "/sql/drop.sql");
    }

    /**
     * 获取测试连接
     *
     * @return Connection
     */
    public Connection getConnection() {
        return sqlSession.getConnection();
    }
}