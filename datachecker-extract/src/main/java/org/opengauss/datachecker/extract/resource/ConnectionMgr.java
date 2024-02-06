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
import org.opengauss.datachecker.common.util.LogUtils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.concurrent.atomic.AtomicBoolean;

/**
 * ConnectionMgr
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/17
 * @since ：11
 */
public class ConnectionMgr {
    private static final Logger log = LogUtils.getLogger();
    private static String driverClassName = "";
    private static String url = "";
    private static String username = "";
    private static String databasePassport = "";
    private static AtomicBoolean isFirstLoad = new AtomicBoolean(true);

    /**
     * 获取JDBC链接
     *
     * @return Connection
     */
    public static synchronized Connection getConnection() {
        if (isFirstLoad.get()) {
            driverClassName = getPropertyValue(ConfigConstants.DRIVER_CLASS_NAME);
            url = getPropertyValue(ConfigConstants.DS_URL);
            username = getPropertyValue(ConfigConstants.DS_USER_NAME);
            databasePassport = getPropertyValue(ConfigConstants.DS_PASSWORD);
            try {
                log.debug("connection class loader ,[{}],[{}]", driverClassName, url);
                Class.forName(driverClassName);
                isFirstLoad.set(false);
            } catch (ClassNotFoundException e) {
                log.error("load driverClassName {} ", driverClassName, e);
            }
        }
        Connection conn = null;
        try {
            conn = DriverManager.getConnection(url, username, databasePassport);
            conn.setAutoCommit(false);
            log.info("Connection succeed!");
        } catch (SQLException exp) {
            log.error("create connection [{},{}]:[{}]", username, databasePassport, url, exp);
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
     * @param ps         PreparedStatement
     * @param resultSet  resultSet
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
}
