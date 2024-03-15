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

import org.opengauss.datachecker.extract.task.functional.CommonTypeHandler;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OracleResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OracleResultSetHandler extends ResultSetHandler {
    private final Map<String, CommonTypeHandler> commonTypeHandlers = new ConcurrentHashMap<>();
    private final Map<String, SimpleTypeHandler> simpleTypeHandlers = new ConcurrentHashMap<>();

    {
        // float4 - float real
        commonTypeHandlers.put(OracleType.NUMBER, typeHandlerFactory.createOracleBigDecimalHandler());
        // date time timestamp
        commonTypeHandlers.put(OracleType.DATE, typeHandlerFactory.createDateTimeHandler());
        commonTypeHandlers.put(OracleType.TIMESTAMP, typeHandlerFactory.createDateTimeHandler());
        commonTypeHandlers.put(OracleType.TIMESTAMPTZ, typeHandlerFactory.createDateTimeHandler());

        // byte binary blob
        simpleTypeHandlers.put(OracleType.RAW, typeHandlerFactory.createOracleRawHandler());
        simpleTypeHandlers.put(OracleType.NCLOB, typeHandlerFactory.createOracleClobHandler());
        simpleTypeHandlers.put(OracleType.CLOB, typeHandlerFactory.createOracleClobHandler());
        simpleTypeHandlers.put(OracleType.BLOB, typeHandlerFactory.createOracleBlobHandler());
        simpleTypeHandlers.put(OracleType.XML, typeHandlerFactory.createOracleXmlHandler());
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        if (simpleTypeHandlers.containsKey(columnTypeName)) {
            return simpleTypeHandlers.get(columnTypeName)
                                     .convert(resultSet, columnLabel);
        } else if (commonTypeHandlers.containsKey(columnTypeName)) {
            return commonTypeHandlers.get(columnTypeName)
                                     .convert(resultSet, columnIdx, rsmd);
        } else {
            return defaultObjectHandler.convert(resultSet, columnLabel);
        }
    }

    @SuppressWarnings("all")
    interface OracleType {
        String RAW = "RAW";
        String BLOB = "BLOB";
        String NCLOB = "NCLOB";
        String CLOB = "CLOB";
        String BFILE = "BFILE";
        String LONG = "LONG";

        String NUMBER = "NUMBER";
        String NUMBER0 = "NUMBER0";
        String BINARY_FLOAT = "BINARY_FLOAT";
        String BINARY_DOUBLE = "BINARY_DOUBLE";
        String INTEGER = "INTEGER";
        String FLOAT = "FLOAT";
        String DOUBLE = "DOUBLE";

        String CHAR = "CHAR";
        String NCHAR = "NCHAR";
        String VARCHAR2 = "VARCHAR2";
        String NVARCHAR2 = "NVARCHAR2";

        String DATE = "DATE";
        String TIMESTAMP = "TIMESTAMP";
        String TIMESTAMPTZ = "TIMESTAMP_WITH_TIMEZONE";
        String XML = "XMLTYPE";

        List<String> digit = List.of(NUMBER, BINARY_FLOAT, BINARY_DOUBLE, INTEGER, FLOAT, DOUBLE);

        public static boolean isDigit(String typeName) {
            return digit.contains(typeName);
        }
    }
}
