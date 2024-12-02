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

package org.opengauss.datachecker.extract.task;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandler;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandlerFactory;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.TreeMap;
import java.util.stream.IntStream;

/**
 * Result set object processor
 *
 * @author wang chao
 * @date ：Created in 2022/6/13
 * @since 11
 **/
public abstract class ResultSetHandler {
    /**
     * log
     */
    protected static final Logger LOG = LogUtils.getLogger();

    /**
     * typeHandlerFactory
     */
    protected final SimpleTypeHandlerFactory typeHandlerFactory = new SimpleTypeHandlerFactory();

    /**
     * defaultObjectHandler
     */
    protected final SimpleTypeHandler defaultObjectHandler = typeHandlerFactory.createObjectHandler();

    /**
     * supplyZero
     */
    protected final boolean supplyZero;

    /**
     * ResultSetHandler
     */
    protected ResultSetHandler() {
        this.supplyZero = false;
    }

    /**
     * ResultSetHandler  supplyZero
     *
     * @param supplyZero supplyZero
     */
    protected ResultSetHandler(Boolean supplyZero) {
        this.supplyZero = supplyZero;
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param tableName JDBC Data query table
     * @param rsmd JDBC Data query result set
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(final String tableName, ResultSetMetaData rsmd,
        ResultSet resultSet) {
        Map<String, String> result = new TreeMap<>();
        try {
            IntStream.rangeClosed(1, rsmd.getColumnCount()).forEach(columnIdx -> {
                String columnLabel = null;
                try {
                    columnLabel = rsmd.getColumnLabel(columnIdx);
                    result.put(columnLabel, convert(resultSet, columnIdx, rsmd));
                } catch (SQLException ex) {
                    LOG.error(" Convert data [{}:{}] {} error ", tableName, columnLabel, ex.getMessage(), ex);
                }
            });
        } catch (SQLException ex) {
            LOG.error(" parse data metadata information exception", ex);
        }
        return result;
    }

    /**
     * Convert the current query result set into map according to the metadata information of the result set
     *
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    public Map<String, String> putOneResultSetToMap(ResultSet resultSet) throws SQLException {
        final ResultSetMetaData rsmd = resultSet.getMetaData();
        String tableName = rsmd.getTableName(1);
        return putOneResultSetToMap(tableName, rsmd, resultSet);
    }

    /**
     * convert
     *
     * @param resultSet resultSet
     * @param columnIdx columnIdx
     * @param rsmd rsmd
     * @return result
     * @throws SQLException SQLException
     */
    protected abstract String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException;
}