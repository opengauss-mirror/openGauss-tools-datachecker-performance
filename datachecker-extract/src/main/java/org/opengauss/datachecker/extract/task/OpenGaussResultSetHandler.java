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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenGaussResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OpenGaussResultSetHandler extends ResultSetHandler {
    private final Map<String, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        TypeHandler byteaToString = (rs, columnLabel) -> bytesToString(rs.getBytes(columnLabel));
        TypeHandler blobToString = (rs, columnLabel) -> rs.getString(columnLabel);
        TypeHandler clobToString = (rs, columnLabel) -> rs.getString(columnLabel);
        TypeHandler xmlToString = (rs, columnLabel) -> rs.getString(columnLabel);
        TypeHandler bitToString = (rs, columnLabel) -> "B'" + rs.getString(columnLabel) + "'";
        TypeHandler booleanToString = (rs, columnLabel) -> booleanToString(rs, columnLabel);
        TypeHandler numericToString = (rs, columnLabel) -> floatingPointNumberToString(rs, columnLabel);
        TypeHandler numeric0ToString = (rs, columnLabel) -> numeric0ToString(rs, columnLabel);
        TypeHandler float4ToString = (rs, columnLabel) -> float4ToString(rs, columnLabel);
        TypeHandler intToString = (rs, columnLabel) -> intToString(rs, columnLabel);

        // float4 - float real
        typeHandlers.put(OpenGaussType.INT4, numeric0ToString);
        typeHandlers.put(OpenGaussType.INTEGER, numeric0ToString);
        typeHandlers.put(OpenGaussType.FLOAT4, float4ToString);
        typeHandlers.put(OpenGaussType.NUMERIC, numericToString);
        typeHandlers.put(OpenGaussType.NUMERIC0, numeric0ToString);

        // byte binary blob
        typeHandlers.put(OpenGaussType.BYTEA, byteaToString);
        typeHandlers.put(OpenGaussType.BLOB, blobToString);
        typeHandlers.put(OpenGaussType.BOOLEAN, booleanToString);
        typeHandlers.put(OpenGaussType.CLOB, clobToString);
        typeHandlers.put(OpenGaussType.XML, xmlToString);
        typeHandlers.put(OpenGaussType.BIT, bitToString);

        // The openGauss jdbc driver obtains the character,character varying  type as varchar
        typeHandlers.put(OpenGaussType.BPCHAR, this::trim);

        // date time timestamp
        typeHandlers.put(OpenGaussType.DATE, this::getDateFormat);
        typeHandlers.put(OpenGaussType.TIME, this::getTimeFormat);
        typeHandlers.put(OpenGaussType.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(OpenGaussType.TIMESTAMPTZ, this::getTimestampFormat);
    }

    public OpenGaussResultSetHandler() {
        super();
    }

    public OpenGaussResultSetHandler(Boolean supplyZero) {
        super(supplyZero);
    }

    private String intToString(ResultSet rs, String columnLabel) throws SQLException {
        return rs.getString(columnLabel);
    }

    private String float4ToString(ResultSet rs, String columnLabel) throws SQLException {
        return Float.toString(rs.getFloat(columnLabel));
    }

    @Override
    public String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = getPgColumnTypeName(rsmd, columnIdx);
        if (OpenGaussType.isNumeric(columnTypeName)) {
            return convertNumericToString(rsmd, resultSet, columnIdx);
        } else if (OpenGaussType.isFloat(columnTypeName)) {
            return float4ToString(resultSet, columnLabel);
        } else if (OpenGaussType.isBigInteger(columnTypeName)) {
            return numeric0ToString(resultSet, columnLabel);
        } else if (OpenGaussType.isInteger(columnTypeName)) {
            return intToString(resultSet, columnLabel);
        } else if (typeHandlers.containsKey(columnTypeName)) {
            return typeHandlers.get(columnTypeName)
                               .convert(resultSet, columnLabel);
        } else {
            Object object = resultSet.getObject(columnLabel);
            return Objects.isNull(object) ? NULL : object.toString();
        }
    }

    private String convertNumericToString(ResultSetMetaData rsmd, ResultSet resultSet, int columnIdx)
        throws SQLException {
        int precision = rsmd.getPrecision(columnIdx);
        int scale = rsmd.getScale(columnIdx);
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        if (OpenGaussType.isNumericDefault(precision, scale)) {
            return floatingPointNumberToString(resultSet, columnLabel);
        } else if (OpenGaussType.isNumericFloat(precision, scale)) {
            if (supplyZero) {
                return floatingPointNumberToString(resultSet, columnLabel, scale);
            } else {
                return floatingPointNumberToString(resultSet, columnLabel);
            }
        } else {
            return numeric0ToString(resultSet, columnLabel);
        }
    }

    private String getPgColumnTypeName(ResultSetMetaData rsmd, int columnIdx) throws SQLException {
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        if (columnTypeName.contains(OpenGaussType.pg_catalog)) {
            columnTypeName = rsmd.getColumnTypeName(columnIdx)
                                 .replaceAll(OpenGaussType.pg_catalog_type_quotation, OpenGaussType.empty)
                                 .replace(OpenGaussType.pg_catalog_type_split, OpenGaussType.empty);
        }
        return columnTypeName;
    }

    protected String booleanToString(ResultSet rs, String columnLabel) throws SQLException {
        final int booleanVal = rs.getInt(columnLabel);
        return booleanVal == 1 ? "true" : "false";
    }

    @SuppressWarnings("all")
    interface OpenGaussType {
        String pg_catalog = "pg_catalog";
        String pg_catalog_type_quotation = "\"";
        String pg_catalog_type_split = "pg_catalog.";
        String empty = "";
        String BYTEA = "bytea";
        String BOOLEAN = "bool";
        String BLOB = "blob";
        String NUMERIC = "numeric";
        String NUMERIC0 = "numeric0";
        String FLOAT1 = "float4";
        String FLOAT2 = "float4";
        String FLOAT4 = "float4";
        String FLOAT8 = "float4";
        String INTEGER = "Integer";
        String INT1 = "int1";
        String INT2 = "int2";
        String INT4 = "int4";
        String INT8 = "int8";
        String UINT1 = "uint1";
        String UINT2 = "uint2";
        String UINT4 = "uint4";
        String UINT8 = "uint8";
        String VARCHAR = "varchar";
        String BPCHAR = "bpchar";
        String DATE = "date";
        String TIME = "time";
        String TIMESTAMP = "timestamp";
        String TIMESTAMPTZ = "timestamptz";
        String CLOB = "clob";
        String XML = "xml";
        String BIT = "bit";
        List<String> digit =
            List.of(NUMERIC, INT1, INT2, INT4, INT8, UINT1, UINT2, UINT4, UINT8, FLOAT1, FLOAT2, FLOAT4, FLOAT8,
                INTEGER);
        List<String> integerList = List.of(INT1, INT2, INT4, INT8, UINT1, UINT2, UINT4, UINT8, INTEGER);
        List<String> bigintegerList = List.of(INT8, UINT8);
        List<String> floatList = List.of(FLOAT1, FLOAT2, FLOAT4, FLOAT8);

        public static boolean isDigit(String typeName) {
            return digit.contains(typeName);
        }

        public static boolean isNumeric0(String typeName, int precision, int scale) {
            return isNumeric(typeName) && precision > NUMERIC_PRECISION_0 && scale == NUMERIC_SCALE_0;
        }

        public static boolean isFloat(String typeName) {
            return floatList.contains(typeName);
        }

        public static boolean isNumericFloat(int precision, int scale) {
            return precision > NUMERIC_PRECISION_0 && scale > NUMERIC_SCALE_0;
        }

        public static boolean isNumericDefault(int precision, int scale) {
            return precision == NUMERIC_PRECISION_0 && scale == NUMERIC_SCALE_0;
        }

        public static boolean isInteger(String typeName) {
            return integerList.contains(typeName);
        }

        public static boolean isBigInteger(String typeName) {
            return bigintegerList.contains(typeName);
        }

        public static boolean isNumeric(String typeName) {
            return NUMERIC.equalsIgnoreCase(typeName);
        }
    }
}
