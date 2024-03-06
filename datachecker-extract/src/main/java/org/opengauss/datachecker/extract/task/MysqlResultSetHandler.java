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

import com.mysql.cj.MysqlType;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

import com.mysql.cj.result.Field;
import org.opengauss.datachecker.extract.task.functional.CommonTypeHandler;
import org.opengauss.datachecker.extract.task.functional.MysqlTypeHandler;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandler;

/**
 * MysqlResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class MysqlResultSetHandler extends ResultSetHandler {
    private final Map<MysqlType, CommonTypeHandler> commonTypeHandlers = new ConcurrentHashMap<>();
    private final Map<MysqlType, SimpleTypeHandler> simpleTypeHandlers = new ConcurrentHashMap<>();
    private final Map<MysqlType, MysqlTypeHandler> mysqlTypeHandlers = new ConcurrentHashMap<>();

    {
        simpleTypeHandlers.put(MysqlType.CHAR, typeHandlerFactory.createCharHandler());
        simpleTypeHandlers.put(MysqlType.INT, typeHandlerFactory.createIntHandler());
        simpleTypeHandlers.put(MysqlType.INT_UNSIGNED, typeHandlerFactory.createLongHandler());
        simpleTypeHandlers.put(MysqlType.BIGINT, typeHandlerFactory.createLongHandler());
        simpleTypeHandlers.put(MysqlType.BIGINT_UNSIGNED, typeHandlerFactory.createUnsignedLongHandler());

        // byte binary blob
        simpleTypeHandlers.put(MysqlType.BINARY, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(MysqlType.VARBINARY, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(MysqlType.BLOB, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(MysqlType.LONGBLOB, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(MysqlType.MEDIUMBLOB, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(MysqlType.TINYBLOB, typeHandlerFactory.createBytesHandler());

        // date time timestamp
        simpleTypeHandlers.put(MysqlType.DATE, typeHandlerFactory.createDateHandler());
        simpleTypeHandlers.put(MysqlType.TIME, typeHandlerFactory.createTimeHandler());
        simpleTypeHandlers.put(MysqlType.YEAR, typeHandlerFactory.createYearHandler());

        commonTypeHandlers.put(MysqlType.BIT, typeHandlerFactory.createBitHandler());
        commonTypeHandlers.put(MysqlType.FLOAT, typeHandlerFactory.createFloatHandler());
        commonTypeHandlers.put(MysqlType.FLOAT_UNSIGNED, typeHandlerFactory.createFloatHandler());
        commonTypeHandlers.put(MysqlType.DECIMAL, typeHandlerFactory.createBigDecimalHandler());
        commonTypeHandlers.put(MysqlType.DECIMAL_UNSIGNED, typeHandlerFactory.createBigDecimalHandler());

        mysqlTypeHandlers.put(MysqlType.DOUBLE, typeHandlerFactory.createMysqlDoubleHandler());
        mysqlTypeHandlers.put(MysqlType.DOUBLE_UNSIGNED, typeHandlerFactory.createMysqlDoubleHandler());
        mysqlTypeHandlers.put(MysqlType.TIMESTAMP, typeHandlerFactory.createMysqlDateTimeHandler());
        mysqlTypeHandlers.put(MysqlType.DATETIME, typeHandlerFactory.createMysqlDateTimeHandler());
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        final MysqlType mysqlType = MysqlType.getByName(columnTypeName);
        Field[] fields = getResultSetFields(rsmd);
        if (simpleTypeHandlers.containsKey(mysqlType)) {
            return simpleTypeHandlers.get(mysqlType)
                                     .convert(resultSet, columnLabel);
        } else if (commonTypeHandlers.containsKey(mysqlType)) {
            return commonTypeHandlers.get(mysqlType)
                                     .convert(resultSet, columnIdx, rsmd);
        } else if (mysqlTypeHandlers.containsKey(mysqlType)) {
            return mysqlTypeHandlers.get(mysqlType)
                                    .convert(resultSet, columnIdx, fields[columnIdx - 1]);
        } else {
            return defaultObjectHandler.convert(resultSet, columnLabel);
        }
    }

    private Field[] getResultSetFields(ResultSetMetaData rsmd) {
        if (rsmd instanceof com.mysql.cj.jdbc.result.ResultSetMetaData) {
            return ((com.mysql.cj.jdbc.result.ResultSetMetaData) rsmd).getFields();
        }
        return new Field[0];
    }
}