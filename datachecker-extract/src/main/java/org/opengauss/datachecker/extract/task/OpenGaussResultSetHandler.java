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

import org.opengauss.core.Field;
import org.opengauss.datachecker.extract.task.functional.CommonTypeHandler;
import org.opengauss.datachecker.extract.task.functional.OpgsTypeHandler;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandler;
import org.opengauss.jdbc.PgResultSetMetaData;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * OpenGaussResultSetHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/19
 * @since ：11
 */
public class OpenGaussResultSetHandler extends ResultSetHandler {
    /**
     * common type handler
     */
    protected final Map<String, CommonTypeHandler> commonTypeHandlers = new ConcurrentHashMap<>();

    /**
     * openGauss type handler
     */
    protected final Map<String, OpgsTypeHandler> openGaussTypeHandlers = new ConcurrentHashMap<>();

    /**
     * simple type handler
     */
    protected final Map<String, SimpleTypeHandler> simpleTypeHandlers = new ConcurrentHashMap<>();

    {
        simpleTypeHandlers.put(OpenGaussType.BYTEA, typeHandlerFactory.createBytesHandler());
        simpleTypeHandlers.put(OpenGaussType.BLOB, typeHandlerFactory.createBlobHandler());
        simpleTypeHandlers.put(OpenGaussType.MEDIUMBLOB, typeHandlerFactory.createBlobHandler());
        simpleTypeHandlers.put(OpenGaussType.TINYBLOB, typeHandlerFactory.createBlobHandler());
        simpleTypeHandlers.put(OpenGaussType.LONGBLOB, typeHandlerFactory.createBlobHandler());
        simpleTypeHandlers.put(OpenGaussType.CLOB, typeHandlerFactory.createClobHandler());
        simpleTypeHandlers.put(OpenGaussType.XML, typeHandlerFactory.createXmlHandler());
        simpleTypeHandlers.put(OpenGaussType.BIT, typeHandlerFactory.createOgBitHandler());
        simpleTypeHandlers.put(OpenGaussType.binary, typeHandlerFactory.createOgBinaryHandler());
        simpleTypeHandlers.put(OpenGaussType.varbinary, typeHandlerFactory.createOgBinaryHandler());
        simpleTypeHandlers.put(OpenGaussType.BOOLEAN, typeHandlerFactory.createBooleanHandler());
        simpleTypeHandlers.put(OpenGaussType.BPCHAR, typeHandlerFactory.createOgBpCharHandler());
        simpleTypeHandlers.put(OpenGaussType.INT1, typeHandlerFactory.createSmallIntHandler());
        simpleTypeHandlers.put(OpenGaussType.UINT1, typeHandlerFactory.createSmallIntHandler());
        simpleTypeHandlers.put(OpenGaussType.INT2, typeHandlerFactory.createSmallIntHandler());
        simpleTypeHandlers.put(OpenGaussType.UINT2, typeHandlerFactory.createSmallIntHandler());
        simpleTypeHandlers.put(OpenGaussType.INT4, typeHandlerFactory.createIntHandler());
        simpleTypeHandlers.put(OpenGaussType.UINT4, typeHandlerFactory.createUnsignedLongHandler());
        simpleTypeHandlers.put(OpenGaussType.INT8, typeHandlerFactory.createLongHandler());
        simpleTypeHandlers.put(OpenGaussType.UINT8, typeHandlerFactory.createUnsignedLongHandler());
        simpleTypeHandlers.put(OpenGaussType.INTEGER, typeHandlerFactory.createIntHandler());
        simpleTypeHandlers.put(OpenGaussType.DATE, typeHandlerFactory.createDateHandler());
        simpleTypeHandlers.put(OpenGaussType.TIME, typeHandlerFactory.createTimeHandler());

        commonTypeHandlers.put(OpenGaussType.TIMESTAMP, typeHandlerFactory.createDateTimeHandler());
        commonTypeHandlers.put(OpenGaussType.TIMESTAMPTZ, typeHandlerFactory.createDateTimeHandler());
        commonTypeHandlers.put(OpenGaussType.NUMERIC, typeHandlerFactory.createBigDecimalHandler());

        // openGauss-jdbc bug 获取real类型(precision，scale) 默认为(8，8)
        openGaussTypeHandlers.put(OpenGaussType.FLOAT1, typeHandlerFactory.createOgSmallFloatHandler());
        openGaussTypeHandlers.put(OpenGaussType.FLOAT2, typeHandlerFactory.createOgSmallFloatHandler());
        openGaussTypeHandlers.put(OpenGaussType.FLOAT4, typeHandlerFactory.createOgFloatHandler());
        openGaussTypeHandlers.put(OpenGaussType.FLOAT8, typeHandlerFactory.createOgDoubleHandler());
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

    @Override
    public String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = getPgColumnTypeName(rsmd, columnIdx);
        Field resultSetField = getResultSetField(columnIdx, rsmd);
        if (simpleTypeHandlers.containsKey(columnTypeName)) {
            return simpleTypeHandlers.get(columnTypeName)
                                     .convert(resultSet, columnLabel);
        } else if (commonTypeHandlers.containsKey(columnTypeName)) {
            return commonTypeHandlers.get(columnTypeName)
                                     .convert(resultSet, columnIdx, rsmd);
        } else if (openGaussTypeHandlers.containsKey(columnTypeName)) {
            return openGaussTypeHandlers.get(columnTypeName)
                                        .convert(resultSet, columnIdx, resultSetField);
        } else {
            return defaultObjectHandler.convert(resultSet, columnLabel);
        }
    }

    private Field getResultSetField(int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        if (rsmd instanceof PgResultSetMetaData) {
            return ((PgResultSetMetaData) rsmd).getField(columnIdx);
        } else {
            return null;
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

    /**
     * openGauss data type
     */
    @SuppressWarnings("all")
    protected interface OpenGaussType {
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
         * year
         */
        String YEAR = "year";

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
