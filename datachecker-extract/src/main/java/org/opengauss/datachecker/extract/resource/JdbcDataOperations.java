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

package org.opengauss.datachecker.extract.resource;

import com.alibaba.druid.pool.DruidDataSource;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.opengauss.datachecker.extract.util.DruidDataSourceUtil;
import org.springframework.jdbc.datasource.DataSourceUtils;

import javax.sql.DataSource;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.util.Objects;

/**
 * JdbcDataOperations
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/9
 * @since ：11
 */
public class JdbcDataOperations {
    private static final Logger log = LogUtils.getBusinessLogger();
    private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";
    private static final int LOG_WAIT_TIMES = 600;

    private final DataSource jdbcDataSource;
    private final ResourceManager resourceManager;

    /**
     * constructor
     *
     * @param jdbcDataSource  datasource
     * @param resourceManager resourceManager
     */
    public JdbcDataOperations(DataSource jdbcDataSource, ResourceManager resourceManager) {
        this.jdbcDataSource = jdbcDataSource;
        this.resourceManager = resourceManager;
    }

    /**
     * try to get a jdbc connection and close auto commit.
     *
     * @param allocMemory allocMemory
     * @return Connection
     * @throws SQLException SQLException
     */
    public synchronized Connection tryConnectionAndClosedAutoCommit(long allocMemory) throws SQLException {
        DruidDataSourceUtil.print((DruidDataSource) jdbcDataSource);
        takeConnection(allocMemory);
        return getConnectionAndClosedAutoCommit();
    }

    /**
     * try to get a jdbc connection and close auto commit.
     *
     * @return Connection
     * @throws SQLException SQLException
     */
    public synchronized Connection tryConnectionAndClosedAutoCommit() throws SQLException {
        takeConnection(0);
        return getConnectionAndClosedAutoCommit();
    }

    private Connection getConnectionAndClosedAutoCommit() throws SQLException {
        if (isShutdown()) {
            String message = "extract service is shutdown ,task of table is canceled!";
            throw new ExtractDataAccessException(message);
        }
        Connection connection = jdbcDataSource.getConnection();
        if (connection.getAutoCommit()) {
            connection.setAutoCommit(false);
        }
        return connection;
    }

    /**
     * release connection
     *
     * @param connection connection
     */
    public synchronized void releaseConnection(Connection connection) {
        resourceManager.release();
        DataSourceUtils.releaseConnection(connection, jdbcDataSource);
    }

    /**
     * start openGauss query dop
     *
     * @param queryDop queryDop
     * @throws SQLException SQLException
     */
    public void enableDatabaseParallelQuery(int queryDop) throws SQLException {
        if (Objects.equals(DataBaseType.OG, ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class))) {
            Connection connection = getConnectionAndClosedAutoCommit();
            try (PreparedStatement ps = connection
                .prepareStatement(String.format(OPEN_GAUSS_PARALLEL_QUERY, queryDop))) {
                ps.execute();
            }
            DataSourceUtils.doReleaseConnection(connection, jdbcDataSource);
        }
    }

    /**
     * get parallel query dop
     *
     * @return query dop
     */
    public int getParallelQueryDop() {
        return resourceManager.getParallelQueryDop();
    }

    private void takeConnection(long free) {
        int waitTimes = 0;
        while (!canExecQuery(free)) {
            if (isShutdown()) {
                break;
            }
            ThreadUtil.sleepOneSecond();
            waitTimes++;
            if (waitTimes >= LOG_WAIT_TIMES) {
                log.warn("wait times , try to take connection");
                waitTimes = 0;
            }
        }
    }

    private boolean canExecQuery(long free) {
        return free > 0 ? resourceManager.canExecQuery(free) : resourceManager.canExecQuery();
    }

    private boolean isShutdown() {
        return resourceManager.isShutdown();
    }
}
