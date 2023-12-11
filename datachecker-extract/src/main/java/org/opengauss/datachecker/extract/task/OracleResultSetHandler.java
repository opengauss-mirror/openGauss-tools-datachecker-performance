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

import org.opengauss.datachecker.common.util.HexUtil;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.Reader;
import java.sql.Clob;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OracleResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OracleResultSetHandler extends ResultSetHandler {
    private final Map<String, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler blobToString = (rs, columnLabel) -> HexUtil.byteToHexTrim(rs.getBytes(columnLabel));
        TypeHandler clobToString = (rs, columnLabel) -> clobToString(rs.getClob(columnLabel));
        TypeHandler xmlToString = (rs, columnLabel) -> rs.getString(columnLabel);
        TypeHandler numberToString = (rs, columnLabel) -> floatingPointNumberToString(rs, columnLabel);
        TypeHandler number0ToString = (rs, columnLabel) -> numeric0ToString(rs, columnLabel);

        // float4 - float real
        typeHandlers.put(OracleType.NUMBER, numberToString);
        typeHandlers.put(OracleType.NUMBER0, number0ToString);
        // byte binary blob
        typeHandlers.put(OracleType.NCLOB, clobToString);
        typeHandlers.put(OracleType.BLOB, blobToString);
        typeHandlers.put(OracleType.CLOB, clobToString);
        typeHandlers.put(OracleType.XML, xmlToString);

        // date time timestamp
        typeHandlers.put(OracleType.DATE, this::getTimestampFormat);
        typeHandlers.put(OracleType.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(OracleType.TIMESTAMPTZ, this::getTimestampFormat);
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        if (OracleType.isNumeric0(columnTypeName, rsmd.getScale(columnIdx))) {
            return typeHandlers.get(OracleType.NUMBER0)
                               .convert(resultSet, columnLabel);
        }
        if (typeHandlers.containsKey(columnTypeName)) {
            return typeHandlers.get(columnTypeName)
                               .convert(resultSet, columnLabel);
        } else {
            Object object = resultSet.getObject(columnLabel);
            return Objects.isNull(object) ? NULL : object.toString();
        }
    }

    protected String clobToString(Clob clob) throws SQLException {
        if (Objects.isNull(clob)) {
            return NULL;
        }
        StringBuffer sb = new StringBuffer();
        try (Reader reader = clob.getCharacterStream(); BufferedReader bf = new BufferedReader(reader)) {
            String line;
            while ((line = bf.readLine()) != null) {
                sb.append(line);
            }
        } catch (IOException io) {
            log.error("read blob error");
        }
        return sb.toString();
    }

    @SuppressWarnings("all")
    interface OracleType {
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

        public static boolean isNumeric0(String typeName, int scale) {
            return NUMBER.equalsIgnoreCase(typeName) && scale >= NUMERIC_SCALE_F84 && scale <= NUMERIC_SCALE_0;
        }
    }
}
