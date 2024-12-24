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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;

import javax.sql.PooledConnection;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.locks.Lock;
import java.util.concurrent.locks.ReentrantLock;

/**
 * ConnectionMgr
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/17
 * @since ：11
 */
public class ConnectionMgr {
    private static final Logger log = LogUtils.getLogger(ConnectionMgr.class);
    private static final Lock LOCK = new ReentrantLock();
    private static final int MAX_RETRY_TIMES = 60;

    private static String driverClassName = "";
    private static String url = "";
    private static String username = "";
    private static String databasePassport = "";
    private static AtomicBoolean isFirstLoad = new AtomicBoolean(true);

    /**
     * 获取JDBC链接
     *
     * @return jdbc Connection
     */
    public static synchronized Connection getConnection() {
        if (isFirstLoad.get()) {
            driverClassName = getPropertyValue(ConfigConstants.DRIVER_CLASS_NAME);
            url = getPropertyValue(ConfigConstants.DS_URL);
            username = getPropertyValue(ConfigConstants.DS_USER_NAME);
            databasePassport = getPropertyValue(ConfigConstants.DS_PASSWORD);
            try {
                LogUtils.debug(log, "connection class loader ,[{}],[{}]", driverClassName, url);
                Class.forName(driverClassName);
                isFirstLoad.set(false);
            } catch (ClassNotFoundException e) {
                LogUtils.error(log, "load driverClassName {} ", driverClassName, e);
            }
        }
        Connection conn = null;
        LOCK.lock();
        try {
            int retry = 0;
            conn = retryToGetConnection(retry);
        } catch (Exception ignore) {
            LogUtils.error(log, "create connection failed , [{},{}]:[{}][{}]", username, databasePassport, url,
                ignore.getMessage());
        } finally {
            LOCK.unlock();
        }
        return conn;
    }

    private static Connection retryToGetConnection(int retry) throws SQLException {
        Connection conn;
        try {
            conn = DriverManager.getConnection(url, username, databasePassport);
            conn.setAutoCommit(false);
        } catch (Exception exp) {
            if (retry <= MAX_RETRY_TIMES) {
                LogUtils.error(log, "retry to get connection cause by {}", exp.getMessage());
                ThreadUtil.sleepCircle(retry);
                conn = retryToGetConnection(++retry);
            } else {
                throw new ExtractDataAccessException("get connection failed");
            }
        }
        return conn;
    }

    private static String getPropertyValue(String key) {
        return ConfigCache.getValue(key);
    }

    /**
     * 关闭数据库链接及 PreparedStatement、ResultSet结果集
     *
     * @param connection connection
     * @param ps PreparedStatement
     * @param resultSet resultSet
     */
    public static void close(Connection connection, PreparedStatement ps, ResultSet resultSet) {
        if (resultSet != null) {
            try {
                resultSet.close();
            } catch (SQLException sql) {
                sql.printStackTrace();
            }
        }
        if (ps != null) {
            try {
                ps.close();
            } catch (SQLException sql) {
                sql.printStackTrace();
            }
        }
        if (connection != null) {
            try {
                connection.close();
            } catch (SQLException sql) {
                sql.printStackTrace();
            }
        }
    }

    /**
     * 关闭数据库链接（连接池连接返回连接池）
     *
     * @param connection connection
     */
    public static void close(Connection connection) {
        if (connection instanceof PooledConnection) {
            try {
                ((PooledConnection) connection).close();
            } catch (SQLException e) {
                throw new ExtractDataAccessException("close pooled connection error");
            }
        } else {
            try {
                connection.close();
            } catch (SQLException e) {
                throw new ExtractDataAccessException("close connection error");
            }
        }
    }
}
