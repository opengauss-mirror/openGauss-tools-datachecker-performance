/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.extract.task;

import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.enums.PrecisionMode;
import org.opengauss.datachecker.extract.task.functional.CommonTypeHandler;
import org.opengauss.datachecker.extract.task.functional.OgracTypeHandlerFactory;
import org.opengauss.datachecker.extract.task.functional.SimpleTypeHandler;

import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Oracle-side result set handler: converts JDBC row data to a string Map column by column type.
 * Compared with the base implementation, it caches column metadata, handler selection and the
 * LONG/LONG RAW-first read order per ResultSetMetaData, so the per-row loop only reads values and
 * writes the Map; type name variants returned by Oracle JDBC are normalized for routing.
 *
 * @author : xujintao
 * @date : Created in 2026/8/31
 * @since : 11
 */
public class OracleResultSetHandler extends ResultSetHandler {
    private static final int MAX_CACHE_SIZE = 1024;

    private final OgracTypeHandlerFactory ogracFactory = new OgracTypeHandlerFactory();
    private final Map<String, CommonTypeHandler> commonTypeHandlers = new ConcurrentHashMap<>();
    private final Map<String, SimpleTypeHandler> simpleTypeHandlers = new ConcurrentHashMap<>();
    private final SimpleTypeHandler defaultObjectHandler = ogracFactory.createObjectHandlerSafe();
    private final PrecisionMode precisionMode;

    private final Map<ResultSetMetaData, ColumnDescriptors> metadataCache = new ConcurrentHashMap<>();

    /**
     * Default constructor using COMPATIBLE (kernel-compatible) mode, matching historical behavior.
     */
    public OracleResultSetHandler() {
        this(PrecisionMode.COMPATIBLE);
    }

    /**
     * Construct with a precision mode, dispatching to the register method for that mode.
     * The Oracle and oGRAC sides must register handlers of the same mode so their
     * output strings stay symmetric.
     *
     * @param precisionMode precision mode
     */
    public OracleResultSetHandler(PrecisionMode precisionMode) {
        super();
        this.precisionMode = precisionMode;
        registerHandlers();
    }

    /**
     * Register the "type -> handler" mapping for the precision mode.
     * Mode-agnostic types (char, interval, LOB, binary, DATE) are registered once in their own
     * register methods; number/float/datetime types pick different handlers per STRICT/COMPATIBLE
     * in their respective register methods.
     */
    private void registerHandlers() {
        registerCharHandlers();
        registerLobAndBinaryHandlers();
        boolean isStrict = precisionMode == PrecisionMode.STRICT;
        registerIntervalHandlers(isStrict);
        registerNumberHandlers(isStrict);
        registerDateTimeHandlers(isStrict);
    }

    /**
     * Register character type handlers (mode-agnostic, identical normalization on both sides).
     */
    private void registerCharHandlers() {
        simpleTypeHandlers.put(OracleType.CHAR, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OracleType.VARCHAR2, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OracleType.NCHAR, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OracleType.NVARCHAR2, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OracleType.LONG, ogracFactory.createCharHandler());
    }

    /**
     * Register LOB and binary type handlers (mode-agnostic).
     */
    private void registerLobAndBinaryHandlers() {
        // LOB: CLOB family (mode-agnostic)
        simpleTypeHandlers.put(OracleType.CLOB, ogracFactory.createOracleClobHandlerSafe());
        simpleTypeHandlers.put(OracleType.NCLOB, ogracFactory.createOracleClobHandlerSafe());
        simpleTypeHandlers.put(OracleType.JSON, ogracFactory.createOracleClobHandlerSafe());
        simpleTypeHandlers.put(OracleType.XMLTYPE, ogracFactory.createOracleXmlHandlerSafe());
        // LOB: BLOB family (mode-agnostic)
        simpleTypeHandlers.put(OracleType.LONG_RAW, ogracFactory.createOracleBlobHandlerSafe());
        simpleTypeHandlers.put(OracleType.BLOB, ogracFactory.createOracleBlobHandlerSafe());
        simpleTypeHandlers.put(OracleType.NBLOB, ogracFactory.createOracleBlobHandlerSafe());
        // Binary RAW (mode-agnostic)
        simpleTypeHandlers.put(OracleType.RAW, ogracFactory.createOracleRawHandler());
    }

    /**
     * Register INTERVAL type handlers.
     * YEAR TO MONTH is zero-padded in both modes; DAY TO SECOND keeps the original precision under
     * STRICT and is rounded to 6 digits under COMPATIBLE.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerIntervalHandlers(boolean isStrict) {
        // INTERVAL YEAR TO MONTH: zero-padded in both modes
        simpleTypeHandlers.put(OracleType.INTERVAL_YEAR_TO_MONTH, ogracFactory.createIntervalYearMonthHandler());
        // INTERVAL DAY TO SECOND: STRICT keeps the original precision (getString as-is), COMPATIBLE rounds to 6 digits
        simpleTypeHandlers.put(OracleType.INTERVAL_DAY_TO_SECOND,
            isStrict ? ogracFactory.createCharHandler() : ogracFactory.createIntervalDaySecondHandler());
    }

    /**
     * Register NUMBER family and float type handlers.
     * The NUMBER family keeps full precision under STRICT (getString + toPlainString) and is rounded
     * to 12 decimal places under COMPATIBLE; float types use getDouble (64-bit) under STRICT to keep
     * precision differences and getFloat (32-bit) under COMPATIBLE to align with oGRAC REAL.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerNumberHandlers(boolean isStrict) {
        CommonTypeHandler numberHandler = isStrict
            ? ogracFactory.createStrictOracleBigDecimalHandler()
            : ogracFactory.createOracleFloatCompatibleHandler();
        commonTypeHandlers.put(OracleType.NUMBER, numberHandler);
        commonTypeHandlers.put(OracleType.NUMBER0, numberHandler);
        commonTypeHandlers.put(OracleType.INTEGER, numberHandler);
        commonTypeHandlers.put(OracleType.INT, numberHandler);
        commonTypeHandlers.put(OracleType.SMALLINT, numberHandler);
        // FLOAT: STRICT uses getDouble (64-bit); COMPATIBLE reuses NUMBER's 12-decimal normalization
        commonTypeHandlers.put(OracleType.FLOAT,
            isStrict ? ogracFactory.createStrictFloatHandler() : numberHandler);
        CommonTypeHandler floatHandler = isStrict
            ? ogracFactory.createStrictFloatHandler()
            : ogracFactory.createBinaryFloatCompatibleHandler();
        commonTypeHandlers.put(OracleType.BINARY_FLOAT, floatHandler);
        commonTypeHandlers.put(OracleType.BINARY_DOUBLE, floatHandler);
        commonTypeHandlers.put(OracleType.DOUBLE_PRECISION, floatHandler);
    }

    /**
     * Register datetime type handlers.
     * DATE and TIMESTAMP WITH TIME ZONE are mode-agnostic; TIMESTAMP and TIMESTAMP WITH LOCAL TIME ZONE
     * keep sub-second digits under STRICT and truncate to seconds under COMPATIBLE.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerDateTimeHandlers(boolean isStrict) {
        // DATE has no sub-second part and both sides agree; both modes share the full-precision handler
        commonTypeHandlers.put(OracleType.DATE, ogracFactory.createFullPrecisionDateTimeHandler());
        // TIMESTAMP: STRICT keeps sub-second digits (per scale), COMPATIBLE truncates to seconds
        commonTypeHandlers.put(OracleType.TIMESTAMP, isStrict
            ? ogracFactory.createFullPrecisionDateTimeHandler()
            : ogracFactory.createTruncatedToSecondDateTimeCompatibleHandler());
        // TIMESTAMP WITH TIME ZONE: STRICT uses the TSTZ full-precision handler (GMT+8 normalization,
        // sub-second digits kept); COMPATIBLE uses the trailing-zero-stripped variant, aligned with
        // the oGRAC side's to_char output that pads sub-second digits to a fixed 6 digits
        // (the same value .680070 yields .68007 on both sides, avoiding new diffs from sub-second trailing zeros)
        CommonTypeHandler timestampTzHandler = isStrict
            ? ogracFactory.createTimestampTzStringHandlerNanosecond()
            : ogracFactory.createTimestampTzStringHandlerNanosecondCompat();
        commonTypeHandlers.put(OracleType.TIMESTAMPTZ, timestampTzHandler);
        commonTypeHandlers.put(OracleType.TIMESTAMPTZ_OFFICIAL, timestampTzHandler);
        // TIMESTAMP WITH LOCAL TIME ZONE: STRICT keeps sub-second digits, COMPATIBLE truncates to seconds
        CommonTypeHandler timestampLtzHandler = isStrict
            ? ogracFactory.createTimestampZoneGmt8HandlerStrict()
            : ogracFactory.createTimestampZoneGmt8HandlerCompatible();
        commonTypeHandlers.put(OracleType.TIMESTAMPLTZ, timestampLtzHandler);
        commonTypeHandlers.put(OracleType.TIMESTAMPLTZ_OFFICIAL, timestampLtzHandler);
    }

    // ---- conversion ----

    /**
     * Convert the current result set row to a Map. Instead of re-parsing metadata for every row and
     * column like the base class, "column label + type name + handler choice" is cached per
     * ResultSetMetaData, and the per-row loop only does handler.convert and Map writes:
     * - HashMap replaces the base class TreeMap (no sorting needed; values are read later in columns order)
     * - A plain for loop replaces the base class IntStream (avoids creating a stream per row)
     * - Column metadata and normalizeTypeName are parsed only once per ResultSet (cached by rsmd)
     *
     * @param tableName JDBC Data query table
     * @param rsmd JDBC Data query result set metadata
     * @param resultSet JDBC Data query result set
     * @return JDBC Data encapsulation results
     */
    @Override
    public Map<String, String> putOneResultSetToMap(final String tableName, ResultSetMetaData rsmd,
        ResultSet resultSet) {
        final int columnCount;
        try {
            columnCount = rsmd.getColumnCount();
        } catch (SQLException ex) {
            LOG.error("{} parse data metadata information exception", ErrorCode.EXECUTE_QUERY_SQL, ex);
            return new HashMap<>();
        }
        ColumnDescriptors columns;
        try {
            columns = resolveColumnDescriptors(rsmd, columnCount);
        } catch (SQLException ex) {
            LOG.error("{} parse data metadata information exception", ErrorCode.EXECUTE_QUERY_SQL, ex);
            return new HashMap<>();
        }
        ColumnDescriptor[] descriptors = columns.descriptors;
        // LONG/LONG RAW-first read order: computed once at the first parse of each ResultSet and
        // cached alongside the descriptors; here we only read in that order, no per-row recomputation
        // (see buildLongFirstReadOrder).
        int[] readOrder = columns.readOrder;
        Map<String, String> result = new HashMap<>(columnCount * 2);
        for (int orderIdx = 0; orderIdx < columnCount; orderIdx++) {
            int i = readOrder[orderIdx];
            ColumnDescriptor d = descriptors[i];
            int columnIdx = i + 1;
            try {
                String value = convertColumn(resultSet, rsmd, columnIdx, d);
                result.put(d.columnLabel, value);
                // LONG/LONG RAW can be read only once: the mapped value already consumed its stream,
                // so getObject for the raw value would always fail; captureRaw debug mode therefore
                // skips LONG columns (diff_detail queries already filter this type, so normally unreachable).
                if (isCaptureRaw && !isLongColumn(d)) {
                    result.put(d.columnLabel + RAW_VALUE_SUFFIX, readRawValueCopy(resultSet, columnIdx).orElse(null));
                }
            } catch (SQLException ex) {
                LOG.error("{} Convert data [{}:{}] {} error ", ErrorCode.EXECUTE_QUERY_SQL, tableName, d.columnLabel,
                    ex.getMessage(), ex);
            }
        }
        return result;
    }

    /**
     * Resolve each column's label, type name and handler from ResultSetMetaData (runs only at the
     * first parse of each ResultSet; later rows hit the cache directly). The same rsmd object is
     * reused across all rows of a result set, so caching by rsmd identity is safe.
     * Also computes the LONG/LONG RAW-first read order once and caches it with the descriptors for reuse.
     *
     * @param rsmd JDBC result set metadata
     * @param columnCount total column count of the result set
     * @return column descriptor array plus the read order
     * @throws SQLException thrown when reading column metadata fails
     */
    private ColumnDescriptors resolveColumnDescriptors(ResultSetMetaData rsmd, int columnCount)
        throws SQLException {
        ColumnDescriptors cached = metadataCache.get(rsmd);
        if (cached != null) {
            return cached;
        }
        ColumnDescriptor[] descriptors = new ColumnDescriptor[columnCount];
        for (int i = 0; i < columnCount; i++) {
            int columnIdx = i + 1;
            String columnLabel;
            String columnTypeName;
            try {
                columnLabel = rsmd.getColumnLabel(columnIdx);
                columnTypeName = rsmd.getColumnTypeName(columnIdx);
            } catch (SQLException ex) {
                // Single-column metadata parse failure: fall back to a positional label + the default
                // handler, so one bad column does not drop the whole row
                LOG.error("{} resolve column metadata [columnIdx={}] error, fallback to default",
                    ErrorCode.EXECUTE_QUERY_SQL, columnIdx, ex);
                columnLabel = "_col_" + columnIdx;
                columnTypeName = null;
            }
            String normalizedTypeName = normalizeTypeName(columnTypeName);
            SimpleTypeHandler simpleHandler = simpleTypeHandlers.get(normalizedTypeName);
            CommonTypeHandler commonHandler = null;
            if (simpleHandler == null) {
                commonHandler = commonTypeHandlers.get(normalizedTypeName);
                if (commonHandler == null) {
                    simpleHandler = defaultObjectHandler;
                }
            }
            descriptors[i] = new ColumnDescriptor(columnLabel, normalizedTypeName,
                simpleHandler, commonHandler);
        }
        ColumnDescriptors columns = new ColumnDescriptors(descriptors, buildLongFirstReadOrder(descriptors));
        if (metadataCache.size() > MAX_CACHE_SIZE) {
            metadataCache.clear();
        }
        metadataCache.put(rsmd, columns);
        return columns;
    }

    /**
     * Build a "LONG/LONG RAW columns first" read order (computed once per ResultSet, reused across rows).
     * Oracle streams LONG data, and reading any other column of the row discards that stream; reading
     * LONG afterwards then fails with "Stream has already been closed" (always hit when a LONG column
     * comes after other columns on a wide table). So the read indices are ordered "LONG/LONG RAW first,
     * then the rest"; with no LONG column it is just the natural order 0..n-1. result is an unordered
     * HashMap, so the write order does not affect downstream reads by the columns list.
     *
     * @param descriptors per-column metadata and handler descriptors
     * @return column indices arranged in read order
     */
    private int[] buildLongFirstReadOrder(ColumnDescriptor[] descriptors) {
        int columnCount = descriptors.length;
        int[] readOrder = new int[columnCount];
        int orderPos = 0;
        for (int i = 0; i < columnCount; i++) {
            if (isLongColumn(descriptors[i])) {
                readOrder[orderPos++] = i;
            }
        }
        for (int i = 0; i < columnCount; i++) {
            if (!isLongColumn(descriptors[i])) {
                readOrder[orderPos++] = i;
            }
        }
        return readOrder;
    }

    /**
     * Convert one column in-row using its resolved column descriptor.
     *
     * @param resultSet JDBC result set
     * @param rsmd JDBC result set metadata
     * @param columnIdx column number (1-based)
     * @param d the current column's metadata and handler descriptor
     * @return the converted column value string
     * @throws SQLException thrown when reading the column value fails
     */
    private String convertColumn(ResultSet resultSet, ResultSetMetaData rsmd, int columnIdx,
        ColumnDescriptor d) throws SQLException {
        String value;
        if (d.commonHandler != null) {
            value = d.commonHandler.convert(resultSet, columnIdx, rsmd);
        } else {
            value = d.simpleHandler.convert(resultSet, d.columnLabel);
        }
        return value;
    }

    /**
     * Read the raw database value before mapping (best-effort). The base class readRawValue is private,
     * so this provides an equivalent implementation for the subclass's per-row override path
     * (invoked only when captureRaw debug is enabled).
     *
     * @param resultSet JDBC result set
     * @param columnIdx column number (1-based)
     * @return the raw value string; {@link Optional#empty()} when the column is NULL or reading fails
     */
    private Optional<String> readRawValueCopy(ResultSet resultSet, int columnIdx) {
        try {
            Object raw = resultSet.getObject(columnIdx);
            return resultSet.wasNull() ? Optional.empty() : Optional.of(String.valueOf(raw));
        } catch (SQLException ex) {
            LOG.error("{} Read raw value [columnIdx={}] error: {}", ErrorCode.EXECUTE_QUERY_SQL, columnIdx,
                ex.getMessage(), ex);
            return Optional.empty();
        }
    }

    /**
     * Whether this is an Oracle streamed LONG/LONG RAW column (must be read first in its row).
     *
     * @param d the current column's metadata and handler descriptor
     * @return true if it is a LONG/LONG RAW column
     */
    private boolean isLongColumn(ColumnDescriptor d) {
        return OracleType.LONG.equals(d.normalizedTypeName)
            || OracleType.LONG_RAW.equals(d.normalizedTypeName);
    }

    /**
     * Resolved metadata and handler descriptor of a single column. Reused across rows
     * to avoid re-parsing types per row.
     */
    private static final class ColumnDescriptor {
        final String columnLabel;
        final String normalizedTypeName;
        final SimpleTypeHandler simpleHandler;  // handler registered in simpleTypeHandlers (or the default handler)
        final CommonTypeHandler commonHandler;  // handler registered in commonTypeHandlers,
                                                // mutually exclusive with simpleHandler

        ColumnDescriptor(String columnLabel, String normalizedTypeName,
            SimpleTypeHandler simpleHandler, CommonTypeHandler commonHandler) {
            this.columnLabel = columnLabel;
            this.normalizedTypeName = normalizedTypeName;
            this.simpleHandler = simpleHandler;
            this.commonHandler = commonHandler;
        }
    }

    /**
     * Per-ResultSet parse result: the column descriptor array + the read order computed once,
     * cached by rsmd and reused across rows.
     */
    private static final class ColumnDescriptors {
        final ColumnDescriptor[] descriptors;
        final int[] readOrder;

        ColumnDescriptors(ColumnDescriptor[] descriptors, int[] readOrder) {
            this.descriptors = descriptors;
            this.readOrder = readOrder;
        }
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        String normalizedTypeName = normalizeTypeName(columnTypeName);

        String value;
        if (simpleTypeHandlers.containsKey(normalizedTypeName)) {
            value = simpleTypeHandlers.get(normalizedTypeName)
                .convert(resultSet, columnLabel);
        } else if (commonTypeHandlers.containsKey(normalizedTypeName)) {
            value = commonTypeHandlers.get(normalizedTypeName)
                .convert(resultSet, columnIdx, rsmd);
        } else {
            value = defaultObjectHandler.convert(resultSet, columnLabel);
        }
        return value;
    }

    /**
     * Normalize the column type name: uppercase it, strip precision suffixes (e.g. CHAR(10 BYTE),
     * NCHAR(10)), and map the shorthand/variant names returned by Oracle JDBC (INTERVALYM,
     * INTERVALDS, etc.) to the constant names used by registered handlers.
     *
     * @param columnTypeName raw column type name returned by JDBC, possibly null
     * @return the normalized type name; an empty string when the input is null (all handler lookups
     *         miss and the default handler applies)
     */
    private String normalizeTypeName(String columnTypeName) {
        if (columnTypeName == null) {
            return "";
        }
        String upperTypeName = columnTypeName.toUpperCase(Locale.ENGLISH);
        if (upperTypeName.startsWith("CHAR(")) {
            return OracleType.CHAR;
        }
        if (upperTypeName.startsWith("VARCHAR2(")) {
            return OracleType.VARCHAR2;
        }
        // Oracle JDBC returns precision-carrying type names for NCHAR(n)/NVARCHAR2(n); normalize to
        // the suffix-free constant, otherwise the registered createCharHandler misses and the
        // fallback defaultObjectHandler makes the two sides inconsistent.
        if (upperTypeName.startsWith("NCHAR(")) {
            return OracleType.NCHAR;
        }
        if (upperTypeName.startsWith("NVARCHAR2(")) {
            return OracleType.NVARCHAR2;
        }
        // Oracle JDBC returns "TIMESTAMP(n)" or "TIMESTAMP WITH TIME ZONE" /
        // "TIMESTAMP WITH LOCAL TIME ZONE" (no parentheses) for the TIMESTAMP family. Match on the
        // "TIMESTAMP" prefix (parentheses not required), otherwise the parenthesis-free zone types
        // fall back to defaultObjectHandler.
        // Note: check WITH LOCAL TIME ZONE first, since it contains the WITH TIME ZONE substring.
        if (upperTypeName.startsWith("TIMESTAMP")) {
            if (upperTypeName.contains("WITH LOCAL TIME ZONE")) {
                return OracleType.TIMESTAMPLTZ_OFFICIAL;
            } else if (upperTypeName.contains("WITH TIME ZONE")) {
                return OracleType.TIMESTAMPTZ_OFFICIAL;
            } else {
                return OracleType.TIMESTAMP;
            }
        }
        // Oracle JDBC returns the shorthand "INTERVALYM" for INTERVAL YEAR TO MONTH and "INTERVALDS"
        // for INTERVAL DAY TO SECOND. Normalize to the full names to match the registered normalized handlers.
        if (upperTypeName.equals("INTERVALYM") || upperTypeName.startsWith("INTERVAL YEAR")) {
            return OracleType.INTERVAL_YEAR_TO_MONTH;
        }
        if (upperTypeName.equals("INTERVALDS") || upperTypeName.startsWith("INTERVAL DAY")) {
            return OracleType.INTERVAL_DAY_TO_SECOND;
        }
        if (upperTypeName.endsWith(".XMLTYPE")) {
            return OracleType.XMLTYPE;
        }
        return upperTypeName;
    }

    /**
     * Oracle-side JDBC column type name constants: covering character, number, float, datetime,
     * interval, binary and LOB types, plus the variant names Oracle JDBC returns.
     * Handler registration and this class's normalization logic both key on these constants.
     */
    interface OracleType {
        /**
         * Fixed-length character type CHAR
         */
        String CHAR = "CHAR";

        /**
         * Variable-length character type VARCHAR2
         */
        String VARCHAR2 = "VARCHAR2";

        /**
         * National charset fixed-length type NCHAR
         */
        String NCHAR = "NCHAR";

        /**
         * National charset variable-length type NVARCHAR2
         */
        String NVARCHAR2 = "NVARCHAR2";

        /**
         * Long text type LONG (streamed; must be read first in its row)
         */
        String LONG = "LONG";

        /**
         * Oracle number type NUMBER
         */
        String NUMBER = "NUMBER";

        /**
         * NUMBER variant name (may be returned by JDBC metadata)
         */
        String NUMBER0 = "NUMBER0";

        /**
         * Integer type INTEGER
         */
        String INTEGER = "INTEGER";

        /**
         * Integer type INT
         */
        String INT = "INT";

        /**
         * Small integer type SMALLINT
         */
        String SMALLINT = "SMALLINT";

        /**
         * Float type FLOAT
         */
        String FLOAT = "FLOAT";

        /**
         * Single-precision float BINARY_FLOAT
         */
        String BINARY_FLOAT = "BINARY_FLOAT";

        /**
         * Double-precision float BINARY_DOUBLE
         */
        String BINARY_DOUBLE = "BINARY_DOUBLE";

        /**
         * Double-precision type DOUBLE PRECISION
         */
        String DOUBLE_PRECISION = "DOUBLE PRECISION";

        /**
         * Date type DATE (no time part)
         */
        String DATE = "DATE";

        /**
         * Timestamp type TIMESTAMP
         */
        String TIMESTAMP = "TIMESTAMP";

        /**
         * Timezone timestamp name returned by JDBC (underscore form)
         */
        String TIMESTAMPTZ = "TIMESTAMP_WITH_TIMEZONE";

        /**
         * Standard spelling of the timezone timestamp type name
         */
        String TIMESTAMPTZ_OFFICIAL = "TIMESTAMP WITH TIME ZONE";

        /**
         * Local-timezone timestamp name returned by JDBC (underscore form)
         */
        String TIMESTAMPLTZ = "TIMESTAMP_WITH_LOCAL_TIMEZONE";

        /**
         * Standard spelling of the local-timezone timestamp type name
         */
        String TIMESTAMPLTZ_OFFICIAL = "TIMESTAMP WITH LOCAL TIME ZONE";

        /**
         * Year-month interval type INTERVAL YEAR TO MONTH
         */
        String INTERVAL_YEAR_TO_MONTH = "INTERVAL YEAR TO MONTH";

        /**
         * Day-second interval type INTERVAL DAY TO SECOND
         */
        String INTERVAL_DAY_TO_SECOND = "INTERVAL DAY TO SECOND";

        /**
         * Binary type RAW
         */
        String RAW = "RAW";

        /**
         * Long binary type LONG RAW (streamed; must be read first in its row)
         */
        String LONG_RAW = "LONG RAW";

        /**
         * Binary large object BLOB
         */
        String BLOB = "BLOB";

        /**
         * National charset binary large object NBLOB
         */
        String NBLOB = "NBLOB";

        /**
         * Character large object CLOB
         */
        String CLOB = "CLOB";

        /**
         * National charset large object NCLOB
         */
        String NCLOB = "NCLOB";

        /**
         * JSON type (read as CLOB)
         */
        String JSON = "JSON";

        /**
         * XML type XMLTYPE
         */
        String XMLTYPE = "XMLTYPE";

        /**
         * Numeric type name set (used to decide whether a column type is numeric/float)
         */
        List<String> DIGIT_TYPES = List.of(NUMBER, NUMBER0, INTEGER, INT, SMALLINT, FLOAT,
            BINARY_FLOAT, BINARY_DOUBLE, DOUBLE_PRECISION);

        /**
         * Whether the given type name is a numeric/float type.
         *
         * @param typeName the normalized column type name
         * @return true if it is a numeric/float type
         */
        static boolean isDigit(String typeName) {
            return DIGIT_TYPES.contains(typeName);
        }
    }
}
