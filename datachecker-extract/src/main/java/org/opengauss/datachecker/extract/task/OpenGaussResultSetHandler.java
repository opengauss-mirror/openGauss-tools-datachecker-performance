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

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Locale;
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
    protected final Map<String, TypeHandler> typeHandlers = new ConcurrentHashMap<>();

    {
        // byte binary blob
        TypeHandler byteaToString = (rs, columnLabel) -> bytesToString(rs.getBytes(columnLabel));
        typeHandlers.put(OpenGaussType.BYTEA, byteaToString);

        TypeHandler blobToString = (rs, columnLabel) -> rs.getString(columnLabel);
        typeHandlers.put(OpenGaussType.BLOB, blobToString);
        typeHandlers.put(OpenGaussType.TINYBLOB, blobToString);
        typeHandlers.put(OpenGaussType.MEDIUMBLOB, blobToString);
        typeHandlers.put(OpenGaussType.LONGBLOB, blobToString);

        TypeHandler clobToString = (rs, columnLabel) -> rs.getString(columnLabel);
        typeHandlers.put(OpenGaussType.CLOB, clobToString);

        TypeHandler xmlToString = (rs, columnLabel) -> rs.getString(columnLabel);
        typeHandlers.put(OpenGaussType.XML, xmlToString);

        TypeHandler bitToString = (rs, columnLabel) -> bitToString(rs, columnLabel);
        typeHandlers.put(OpenGaussType.BIT, bitToString);

        TypeHandler binaryToString = (rs, columnLabel) -> binaryToString(rs, columnLabel);
        typeHandlers.put(OpenGaussType.binary, binaryToString);
        typeHandlers.put(OpenGaussType.varbinary, binaryToString);

        TypeHandler booleanToString = (rs, columnLabel) -> booleanToString(rs, columnLabel);
        typeHandlers.put(OpenGaussType.BOOLEAN, booleanToString);

        TypeHandler bpCharToString = (rs, columnLabel) -> fixedLenCharToString(rs, columnLabel);
        typeHandlers.put(OpenGaussType.BPCHAR, bpCharToString);

        // float4 - float real
        TypeHandler numeric0ToString = (rs, columnLabel) -> numeric0ToString(rs, columnLabel);
        typeHandlers.put(OpenGaussType.INT4, numeric0ToString);
        typeHandlers.put(OpenGaussType.INTEGER, numeric0ToString);
        typeHandlers.put(OpenGaussType.NUMERIC0, numeric0ToString);

        // date time timestamp
        typeHandlers.put(OpenGaussType.DATE, this::getDateFormat);
        typeHandlers.put(OpenGaussType.TIME, this::getTimeFormat);
        typeHandlers.put(OpenGaussType.TIMESTAMP, this::getTimestampFormat);
        typeHandlers.put(OpenGaussType.TIMESTAMPTZ, this::getTimestampFormat);
    }

    /**
     * OpenGaussResultSetHandler
     */
    public OpenGaussResultSetHandler() {
        super();
    }

    /**
     * OpenGaussResultSetHandler
     *
     * @param supplyZero supplyZero
     */
    public OpenGaussResultSetHandler(Boolean supplyZero) {
        super(supplyZero);
    }

    /**
     * binaryToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String binaryToString(ResultSet rs, String columnLabel) throws SQLException {
        String binary = rs.getString(columnLabel);
        return rs.wasNull() ? NULL : Objects.isNull(binary) ? NULL : binary.substring(2)
                                                                           .toUpperCase(Locale.ENGLISH);
    }

    /**
     * bitToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String bitToString(ResultSet rs, String columnLabel) throws SQLException {
        return HexUtil.binaryToHex(rs.getString(columnLabel));
    }

    /**
     * intToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String intToString(ResultSet rs, String columnLabel) throws SQLException {
        return rs.getString(columnLabel);
    }

    /**
     * intToString
     *
     * @param rs          rs
     * @param columnLabel columnLabel
     * @return result
     * @throws SQLException SQLException
     */
    protected String booleanToString(ResultSet rs, String columnLabel) throws SQLException {
        final int booleanVal = rs.getInt(columnLabel);
        return booleanVal == 1 ? "true" : "false";
    }

    @Override
    public String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = getPgColumnTypeName(rsmd, columnIdx);
        if (OpenGaussType.isNumeric(columnTypeName)) {
            return convertNumericToString(rsmd, resultSet, columnIdx);
        } else if (OpenGaussType.isFloat(columnTypeName)) {
            int precision = rsmd.getPrecision(columnIdx);
            int scale = rsmd.getScale(columnIdx);
            if (precision > scale && scale > 0) {
                return floatingPointNumberToString(resultSet, columnLabel, scale);
            } else {
                return floatNumberToString(resultSet, columnLabel);
            }
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
        if (isNumericDefault(precision, scale)) {
            return floatNumberToString(resultSet, columnLabel);
        } else if (isNumeric0(precision, scale)) {
            return numeric0ToString(resultSet, columnLabel);
        } else if (isNumericFloat(precision, scale)) {
            return floatingPointNumberToString(resultSet, columnLabel, scale);
        } else {
            return floatNumberToString(resultSet, columnLabel);
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

    @SuppressWarnings("all")
    private interface OpenGaussType {
        /**
         * opengauss data type : pg_catalog
         */
        String pg_catalog = "pg_catalog";

        /**
         * opengauss data type : pg_catalog type quotation
         */
        String pg_catalog_type_quotation = "\"";

        /**
         * opengauss data type : pg_catalog type split prefex
         */
        String pg_catalog_type_split = "pg_catalog.";

        /**
         * empty string constants
         */
        String empty = "";

        /**
         * opengauss data type : bytea
         */
        String BYTEA = "bytea";

        /**
         * opengauss data type : bool
         */
        String BOOLEAN = "bool";

        /**
         * opengauss data type : blob
         */
        String BLOB = "blob";

        /**
         * opengauss data type : tinyblob
         */
        String TINYBLOB = "tinyblob";

        /**
         * opengauss data type : mediumblob
         */
        String MEDIUMBLOB = "mediumblob";

        /**
         * opengauss data type : longblob
         */
        String LONGBLOB = "longblob";

        /**
         * opengauss data type : numeric
         */
        String NUMERIC = "numeric";

        /**
         * opengauss data type : numeric0
         */
        String NUMERIC0 = "numeric0";

        /**
         * opengauss data type : float2
         */
        String FLOAT1 = "float1";

        /**
         * opengauss data type : float2
         */
        String FLOAT2 = "float2";

        /**
         * opengauss data type : float4
         */
        String FLOAT4 = "float4";

        /**
         * opengauss data type : float8
         */
        String FLOAT8 = "float8";

        /**
         * opengauss data type : integer
         */
        String INTEGER = "integer";

        /**
         * opengauss data type : int1
         */
        String INT1 = "int1";

        /**
         * opengauss data type : int2
         */
        String INT2 = "int2";

        /**
         * opengauss data type : int4
         */
        String INT4 = "int4";

        /**
         * opengauss data type : int8
         */
        String INT8 = "int8";

        /**
         * opengauss data type : uint1
         */
        String UINT1 = "uint1";

        /**
         * opengauss data type : uint2
         */
        String UINT2 = "uint2";

        /**
         * opengauss data type : uint4
         */
        String UINT4 = "uint4";

        /**
         * opengauss data type : uint8
         */
        String UINT8 = "uint8";

        /**
         * opengauss data type : varchar
         */
        String VARCHAR = "varchar";

        /**
         * opengauss data type : bpchar
         */
        String BPCHAR = "bpchar";

        /**
         * opengauss data type : date
         */
        String DATE = "date";

        /**
         * opengauss data type : time
         */
        String TIME = "time";

        /**
         * opengauss data type : timestamp
         */
        String TIMESTAMP = "timestamp";

        /**
         * opengauss data type : timestamptz
         */
        String TIMESTAMPTZ = "timestamptz";

        /**
         * opengauss data type : clob
         */
        String CLOB = "clob";

        /**
         * opengauss data type : xml
         */
        String XML = "xml";

        /**
         * opengauss data type : bit
         */
        String BIT = "bit";

        /**
         * opengauss data type : binary
         */
        String binary = "binary";

        /**
         * opengauss data type : varbinary
         */
        String varbinary = "varbinary";

        /**
         * opengauss data type : all digit type
         */
        List<String> digit =
            List.of(NUMERIC, INT1, INT2, INT4, INT8, UINT1, UINT2, UINT4, UINT8, FLOAT1, FLOAT2, FLOAT4, FLOAT8,
                INTEGER);

        /**
         * opengauss data type : all integer type
         */
        List<String> integerList = List.of(INT1, INT2, INT4, INT8, UINT1, UINT2, UINT4, UINT8, INTEGER);

        /**
         * opengauss data type : bigint and unsigned bigint
         */
        List<String> bigintegerList = List.of(INT8, UINT8);

        /**
         * opengauss data type : all float
         */
        List<String> floatList = List.of(FLOAT1, FLOAT2, FLOAT4, FLOAT8);

        /**
         * typeName is {@value digit}
         *
         * @param typeName typeName
         * @return
         */
        public static boolean isDigit(String typeName) {
            return digit.contains(typeName);
        }

        /**
         * typeName is List.of(FLOAT1, FLOAT2, FLOAT4, FLOAT8)
         *
         * @param typeName typeName
         * @return
         */
        public static boolean isFloat(String typeName) {
            return floatList.contains(typeName);
        }

        /**
         * typeName is  List.of(INT1, INT2, INT4, INT8, UINT1, UINT2, UINT4, UINT8, INTEGER)
         *
         * @param typeName typeName
         * @return
         */
        public static boolean isInteger(String typeName) {
            return integerList.contains(typeName);
        }

        /**
         * typeName is List.of(INT8, UINT8)
         *
         * @param typeName typeName
         * @return
         */
        public static boolean isBigInteger(String typeName) {
            return bigintegerList.contains(typeName);
        }

        /**
         * typeName is numeric
         *
         * @param typeName typeName
         * @return
         */
        public static boolean isNumeric(String typeName) {
            return NUMERIC.equalsIgnoreCase(typeName);
        }
    }
}
