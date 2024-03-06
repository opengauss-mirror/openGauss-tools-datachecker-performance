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

import org.apache.commons.lang3.StringUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

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
    private static final Logger log = LogUtils.getLogger(JdbcDataOperations.class);
    private static final String OPEN_GAUSS_PARALLEL_QUERY = "set query_dop to %s;";
    private static final String OPEN_GAUSS_EXTRA_FLOAT_DIGITS = "set extra_float_digits to 0;";
    private static final int LOG_WAIT_TIMES = 600;
    private static final String DOLPHIN_B_COMPATIBILITY_MODE_ON = "set dolphin.b_compatibility_mode to on;";

    private final boolean isOpenGauss;
    private final boolean isOgCompatibilityB;
    private final ResourceManager resourceManager;
    private final boolean isForceRefreshConnectionSqlMode;

    private String sqlModeRefreshStatement = "";

    /**
     * constructor
     *
     * @param resourceManager resourceManager
     */
    public JdbcDataOperations(ResourceManager resourceManager) {
        this.resourceManager = resourceManager;
        this.isForceRefreshConnectionSqlMode = ConfigCache.getBooleanValue(ConfigConstants.SQL_MODE_FORCE_REFRESH);
        this.isOpenGauss = checkDatabaseIsOpenGauss();
        this.isOgCompatibilityB = isOpenGauss ? ConfigCache.getBooleanValue(ConfigConstants.OG_COMPATIBILITY_B) : false;
        initSqlModeRefreshStatement();
    }

    private boolean checkDatabaseIsOpenGauss() {
        return Objects.equals(DataBaseType.OG,
            ConfigCache.getValue(ConfigConstants.DATA_BASE_TYPE, DataBaseType.class));
    }

    private void initSqlModeRefreshStatement() {
        if (isForceRefreshConnectionSqlMode) {
            String sqlMode = ConfigCache.getValue(ConfigConstants.SQL_MODE_VALUE_CACHE);
            if (isOgCompatibilityB) {
                // openGauss compatibility B set database sql mode must be set dolphin.sql_mode
                sqlModeRefreshStatement = "set dolphin.sql_mode ='" + sqlMode + "'";
            } else {
                // mysql and openGauss compatibility A set database sql mode grammar is same
                sqlModeRefreshStatement = "set sql_mode ='" + sqlMode + "'";
            }
        }
    }

    /**
     * try to get a jdbc connection and close auto commit.
     *
     * @param allocMemory allocMemory
     * @return Connection
     */
    public synchronized Connection tryConnectionAndClosedAutoCommit(long allocMemory) {
        takeConnection(allocMemory);
        return getConnectionAndClosedAutoCommit();
    }

    /**
     * try to get a jdbc connection and close auto commit.
     *
     * @return Connection
     */
    public synchronized Connection tryConnectionAndClosedAutoCommit() {
        takeConnection(0);
        return getConnectionAndClosedAutoCommit();
    }

    private Connection getConnectionAndClosedAutoCommit() {
        if (isShutdown()) {
            String message = "extract service is shutdown ,task of table is canceled!";
            throw new ExtractDataAccessException(message);
        }
        Connection connection = ConnectionMgr.getConnection();
        int tryTime = 0;
        while (Objects.isNull(connection) && tryTime < 30) {
            ThreadUtil.sleepMaxHalfSecond();
            connection = ConnectionMgr.getConnection();
            tryTime++;
            LogUtils.debug(log, "try to get jdbc connection! {}", tryTime);
        }
        if (Objects.isNull(connection)) {
            throw new ExtractDataAccessException("can not get jdbc connection!");
        }
        initJdbcConnectionEnvParameter(connection);
        return connection;
    }

    private void initJdbcConnectionEnvParameter(Connection connection) {
        if (isOpenGauss) {
            execute(connection, OPEN_GAUSS_EXTRA_FLOAT_DIGITS);
            if (isOgCompatibilityB) {
                execute(connection, DOLPHIN_B_COMPATIBILITY_MODE_ON);
            }
        }
        if (isForceRefreshConnectionSqlMode && StringUtils.isNotEmpty(sqlModeRefreshStatement)) {
            execute(connection, sqlModeRefreshStatement);
        }
    }

    private void execute(Connection connection, String statementSql) {
        try {
            PreparedStatement ps = connection.prepareStatement(statementSql);
            ps.execute();
        } catch (SQLException sql) {
            LogUtils.error(log, "exectue sql[{}] exception ", statementSql, sql);
        }
    }

    /**
     * release connection
     *
     * @param connection connection
     */
    public synchronized void releaseConnection(Connection connection) {
        resourceManager.release();
        ConnectionMgr.close(connection, null, null);
    }

    /**
     * start openGauss query dop
     *
     * @param queryDop queryDop
     * @throws SQLException SQLException
     */
    public void enableDatabaseParallelQuery(Connection connection, int queryDop) throws SQLException {
        if (isOpenGauss) {
            try (PreparedStatement ps = connection.prepareStatement(
                String.format(OPEN_GAUSS_PARALLEL_QUERY, queryDop))) {
                ps.execute();
            }
        }
    }

    private void takeConnection(long free) {
        int waitTimes = 0;
        while (!canExecQuery(free)) {
            if (isShutdown()) {
                break;
            }
            ThreadUtil.sleepMaxHalfSecond();
            waitTimes++;
            if (waitTimes >= LOG_WAIT_TIMES) {
                LogUtils.debug(log, "wait times , try to take connection");
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
