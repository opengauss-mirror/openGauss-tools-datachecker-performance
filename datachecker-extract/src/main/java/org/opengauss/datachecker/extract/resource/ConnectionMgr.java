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
import com.alibaba.druid.pool.DruidPooledConnection;

import org.opengauss.datachecker.common.exception.ExtractDataAccessException;

import javax.sql.PooledConnection;

import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;

/**
 * ConnectionMgr
 *
 * @author ：wangchao
 * @date ：Created in 2024/1/17
 * @since ：11
 */
public class ConnectionMgr {
    private static DruidDataSource druidDataSource;

    /**
     * init DruidDataSource
     *
     * @param dataSource DruidDataSource
     */
    public static void initDruidDataSource(DruidDataSource dataSource) {
        druidDataSource = dataSource;
    }

    /**
     * get jdbc Connection
     *
     * @return jdbc Connection
     */
    public static synchronized Connection getConnection() {
        try {
            DruidPooledConnection connection = druidDataSource.getConnection();
            connection.setAutoCommit(false);
            return connection;
        } catch (SQLException ex) {
            throw new ExtractDataAccessException("get connection failed");
        }
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
