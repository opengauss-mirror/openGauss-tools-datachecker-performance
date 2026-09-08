/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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
 * ResultSet handler for the oGRAC source database: converts each row of the extract
 * query into a column-name to value-string map, dispatching per column type to
 * handlers built by {@link OgracTypeHandlerFactory}.
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
public class OgracResultSetHandler extends ResultSetHandler {
    private static final int MAX_CACHE_SIZE = 1024;

    private final OgracTypeHandlerFactory ogracFactory = new OgracTypeHandlerFactory();
    private final Map<String, CommonTypeHandler> commonTypeHandlers = new ConcurrentHashMap<>();
    private final Map<String, SimpleTypeHandler> simpleTypeHandlers = new ConcurrentHashMap<>();
    private final SimpleTypeHandler defaultObjectHandler = ogracFactory.createObjectHandlerSafe();
    private final PrecisionMode precisionMode;

    private final Map<ResultSetMetaData, ColumnDescriptor[]> metadataCache = new ConcurrentHashMap<>();

    /**
     * Default constructor using COMPATIBLE (kernel-compatible) mode, matching historical behavior.
     */
    public OgracResultSetHandler() {
        this(PrecisionMode.COMPATIBLE);
    }

    /**
     * Construct with a precision mode, dispatching to the register method for that mode.
     * The Oracle and oGRAC sides must register handlers of the same mode so their
     * output strings stay symmetric.
     *
     * @param precisionMode precision mode
     */
    public OgracResultSetHandler(PrecisionMode precisionMode) {
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
        registerFloatHandlers(isStrict);
        registerDateTimeHandlers(isStrict);
    }

    /**
     * Register character type handlers (mode-agnostic, identical normalization on both sides).
     */
    private void registerCharHandlers() {
        simpleTypeHandlers.put(OgracType.CHAR, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.VARCHAR, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.VARCHAR2, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.TEXT, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.NCHAR, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.NVARCHAR2, ogracFactory.createCharHandler());
        simpleTypeHandlers.put(OgracType.LONG, ogracFactory.createCharHandler());
    }

    /**
     * Register INTERVAL type handlers.
     * YEAR TO MONTH is zero-padded in both modes; DAY TO SECOND keeps the original precision under
     * STRICT and is rounded to 6 digits under COMPATIBLE.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerIntervalHandlers(boolean isStrict) {
        // INTERVAL YEAR TO MONTH: zero-padded in both modes (including the migration target DATE_YEAR_MONTH)
        simpleTypeHandlers.put(OgracType.INTERVAL_YEAR_TO_MONTH, ogracFactory.createIntervalYearMonthHandler());
        simpleTypeHandlers.put(OgracType.DATE_YEAR_MONTH, ogracFactory.createIntervalYearMonthHandler());
        // INTERVAL DAY TO SECOND: STRICT keeps the original precision (getString as-is), COMPATIBLE rounds to 6 digits
        simpleTypeHandlers.put(OgracType.INTERVAL_DAY_TO_SECOND,
            isStrict ? ogracFactory.createCharHandler() : ogracFactory.createIntervalDaySecondHandler());
        simpleTypeHandlers.put(OgracType.DATE_DAY_HMS,
            isStrict ? ogracFactory.createCharHandler() : ogracFactory.createIntervalDaySecondHandler());
    }

    /**
     * Register LOB and binary type handlers (mode-agnostic).
     */
    private void registerLobAndBinaryHandlers() {
        // LOB: CLOB (mode-agnostic)
        simpleTypeHandlers.put(OgracType.CLOB, ogracFactory.createOracleClobHandlerSafe());
        simpleTypeHandlers.put(OgracType.NCLOB, ogracFactory.createOracleClobHandlerSafe());
        simpleTypeHandlers.put(OgracType.XMLTYPE, ogracFactory.createOracleXmlHandlerSafe());
        // LOB: BLOB (mode-agnostic), IMAGE being its variant
        simpleTypeHandlers.put(OgracType.BLOB, ogracFactory.createOracleBlobHandlerSafe());
        simpleTypeHandlers.put(OgracType.IMAGE, ogracFactory.createOracleBlobHandlerSafe());
        // Binary RAW (mode-agnostic)
        simpleTypeHandlers.put(OgracType.RAW, ogracFactory.createOracleRawHandler());
        // BINARY/VARBINARY: getObject returns byte[]; the default handler would print junk like [B@xxx,
        // so getBytes + hex must be used
        simpleTypeHandlers.put(OgracType.BINARY, ogracFactory.createOracleBlobHandlerSafe());
        simpleTypeHandlers.put(OgracType.VARBINARY, ogracFactory.createOracleBlobHandlerSafe());
    }

    /**
     * Register NUMBER family handlers.
     * STRICT keeps full precision (getString + toPlainString); COMPATIBLE rounds to 12 decimal places uniformly.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerNumberHandlers(boolean isStrict) {
        CommonTypeHandler numberHandler = isStrict
            ? ogracFactory.createStrictOgracBigDecimalHandler()
            : ogracFactory.createOracleFloatCompatibleHandler();
        commonTypeHandlers.put(OgracType.NUMBER, numberHandler);
        commonTypeHandlers.put(OgracType.NUMBER0, numberHandler);
        commonTypeHandlers.put(OgracType.INTEGER, numberHandler);
        commonTypeHandlers.put(OgracType.INT, numberHandler);
        commonTypeHandlers.put(OgracType.SMALLINT, numberHandler);
        commonTypeHandlers.put(OgracType.BIGINT, numberHandler);
        commonTypeHandlers.put(OgracType.NUMERIC, numberHandler);
        commonTypeHandlers.put(OgracType.DECIMAL, numberHandler);
        commonTypeHandlers.put(OgracType.UINT, numberHandler);
        commonTypeHandlers.put(OgracType.NUMBER2, numberHandler);
        // FLOAT: STRICT uses getDouble (64-bit); COMPATIBLE reuses NUMBER's 12-decimal normalization
        commonTypeHandlers.put(OgracType.FLOAT,
            isStrict ? ogracFactory.createStrictFloatHandler() : numberHandler);
    }

    /**
     * Register float type handlers.
     * STRICT uses getDouble (64-bit) to keep precision differences; COMPATIBLE uses getFloat (32-bit)
     * to align with oGRAC REAL.
     *
     * @param isStrict whether STRICT mode
     */
    private void registerFloatHandlers(boolean isStrict) {
        CommonTypeHandler floatHandler = isStrict
            ? ogracFactory.createStrictFloatHandler()
            : ogracFactory.createBinaryFloatCompatibleHandler();
        commonTypeHandlers.put(OgracType.BINARY_FLOAT, floatHandler);
        commonTypeHandlers.put(OgracType.BINARY_DOUBLE, floatHandler);
        commonTypeHandlers.put(OgracType.DOUBLE_PRECISION, floatHandler);
        commonTypeHandlers.put(OgracType.REAL, floatHandler);
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
        commonTypeHandlers.put(OgracType.DATE, ogracFactory.createFullPrecisionDateTimeHandler());
        // TIMESTAMP: STRICT keeps sub-second digits (per scale), COMPATIBLE truncates to seconds
        commonTypeHandlers.put(OgracType.TIMESTAMP, isStrict
            ? ogracFactory.createFullPrecisionDateTimeHandler()
            : ogracFactory.createTruncatedToSecondDateTimeCompatibleHandler());
        // TIMESTAMP WITH TIME ZONE: STRICT uses the TSTZ full-precision handler (GMT+8 normalization,
        // sub-second digits kept); COMPATIBLE uses the trailing-zero-stripped variant, aligning the
        // digit-count difference between oGRAC's to_char rewrite (sub-second padded to a fixed 6 digits)
        // and Oracle's per-column scale output (e.g. .998000 and .998 produce identical output for the same value)
        CommonTypeHandler timestampTzHandler = isStrict
            ? ogracFactory.createTimestampTzStringHandlerNanosecond()
            : ogracFactory.createTimestampTzStringHandlerNanosecondCompat();
        commonTypeHandlers.put(OgracType.TIMESTAMPTZ, timestampTzHandler);
        commonTypeHandlers.put(OgracType.TIMESTAMPTZ_OFFICIAL, timestampTzHandler);
        commonTypeHandlers.put(OgracType.TIMESTAMP_TZ, timestampTzHandler);
        // TIMESTAMP WITH LOCAL TIME ZONE: STRICT keeps sub-second digits, COMPATIBLE truncates to seconds
        CommonTypeHandler timestampLtzHandler = isStrict
            ? ogracFactory.createOgracTimestampZoneGmt8HandlerStrict()
            : ogracFactory.createOgracTimestampZoneGmt8HandlerCompatible();
        commonTypeHandlers.put(OgracType.TIMESTAMPLTZ, timestampLtzHandler);
        commonTypeHandlers.put(OgracType.TIMESTAMPLTZ_OFFICIAL, timestampLtzHandler);
        commonTypeHandlers.put(OgracType.TIMESTAMP_LTZ, timestampLtzHandler);
        commonTypeHandlers.put(OgracType.UTC, timestampLtzHandler);
    }

    /**
     * Convert the current result set row to a Map. Instead of re-parsing metadata for every row and
     * column like the base class, "column label + type name + handler choice" is cached per
     * ResultSetMetaData, and the per-row loop only does handler.convert and Map writes:
     * - HashMap replaces the base class TreeMap (no sorting needed; values are read later in columns order)
     * - A plain for loop replaces the base class IntStream (avoids creating a stream per row)
     * - Column metadata and normalizeTypeName are parsed only once per ResultSet (cached by rsmd)
     * - Character columns read via getString once: first check the value shape to decide whether to
     *   route to TSTZ normalization, and reuse that single read either way, avoiding the double
     *   getString ("check + handler read") on the same column in the base/original convert.
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
        ColumnDescriptor[] descriptors;
        try {
            descriptors = resolveColumnDescriptors(rsmd, columnCount);
        } catch (SQLException ex) {
            LOG.error("{} parse data metadata information exception", ErrorCode.EXECUTE_QUERY_SQL, ex);
            return new HashMap<>();
        }
        Map<String, String> result = new HashMap<>(columnCount * 2);
        for (int i = 0; i < columnCount; i++) {
            ColumnDescriptor d = descriptors[i];
            int columnIdx = i + 1;
            try {
                String value = convertColumn(resultSet, rsmd, columnIdx, d);
                result.put(d.columnLabel, value);
                if (isCaptureRaw) {
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
     *
     * @param rsmd JDBC result set metadata
     * @param columnCount total column count of the result set
     * @return per-column metadata and handler descriptor array
     * @throws SQLException thrown when reading column metadata fails
     */
    private ColumnDescriptor[] resolveColumnDescriptors(ResultSetMetaData rsmd, int columnCount)
        throws SQLException {
        ColumnDescriptor[] cached = metadataCache.get(rsmd);
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
            boolean isCharLikeColumn = isCharLike(normalizedTypeName);
            SimpleTypeHandler simpleHandler = simpleTypeHandlers.get(normalizedTypeName);
            CommonTypeHandler commonHandler = null;
            if (simpleHandler == null) {
                commonHandler = commonTypeHandlers.get(normalizedTypeName);
                if (commonHandler == null) {
                    simpleHandler = defaultObjectHandler;
                }
            }
            descriptors[i] = new ColumnDescriptor(columnLabel, normalizedTypeName,
                simpleHandler, commonHandler, isCharLikeColumn);
        }
        if (metadataCache.size() > MAX_CACHE_SIZE) {
            metadataCache.clear();
        }
        metadataCache.put(rsmd, descriptors);
        return descriptors;
    }

    /**
     * Convert one column in-row using its resolved column descriptor.
     * <p>Character-like columns read via getString once: after the oGRAC side rewrites a TIMESTAMP
     * WITH TIME ZONE column as to_char(col) in the SELECT, its JDBC type degrades to TEXT/VARCHAR,
     * so the value shape decides the route — a TSTZ string with a timezone suffix goes to GMT+8
     * normalization (symmetric with the Oracle side), otherwise it is returned as an ordinary
     * character value. Both cases reuse the same single getString result, avoiding a second read.
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
        if (d.isCharLike) {
            // Character-like columns (including TSTZ columns rewritten by to_char): read once, then
            // route by value shape. Values with a timezone suffix are normalized per the reference
            // TSTZ handler semantics (scale taken from column metadata, consistent with
            // createTimestampTzStringHandlerNanosecond), avoiding precision digit drift from a
            // hardcoded 0; otherwise the registered SimpleTypeHandler (e.g. createCharHandler)
            // applies, restoring the original handler call chain and keeping value and
            // NULL/wasNull semantics exactly as the original implementation.
            String rawValue = resultSet.getString(columnIdx);
            if (OgracTypeHandlerFactory.isTstzFormattedValue(rawValue)) {
                // Under COMPATIBLE, strip trailing fractional zeros: oGRAC to_char pads sub-second
                // digits to a fixed 6 (e.g. TIMESTAMP(3) .998 becomes .998000), which must align
                // with the Oracle side's per-scale .998; STRICT keeps the original normalization
                value = precisionMode == PrecisionMode.STRICT
                    ? OgracTypeHandlerFactory.normalizeTstzString(rawValue, rsmd.getScale(columnIdx))
                    : OgracTypeHandlerFactory.normalizeTstzStringCompat(rawValue, rsmd.getScale(columnIdx));
            } else {
                value = d.simpleHandler.convert(resultSet, d.columnLabel);
            }
        } else if (d.commonHandler != null) {
            value = d.commonHandler.convert(resultSet, columnIdx, rsmd);
        } else {
            value = d.simpleHandler.convert(resultSet, d.columnLabel);
        }
        return value;
    }

    /**
     * Read the raw database value before mapping (best-effort). The base class readRawValue is
     * private, so this provides an equivalent implementation for the subclass's per-row override
     * path (only called when captureRaw debugging is enabled).
     *
     * @param resultSet JDBC result set
     * @param columnIdx column number (1-based)
     * @return the raw value string; {@link Optional#empty()} when NULL or the read fails
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
     * Resolved metadata and handler descriptor for one column. Reused across rows to avoid
     * re-parsing the type on every row.
     */
    private static final class ColumnDescriptor {
        final String columnLabel;
        final String normalizedTypeName;
        final SimpleTypeHandler simpleHandler;  // handler registered in simpleTypeHandlers (or the default handler)

        // Handler registered in commonTypeHandlers, mutually exclusive with simpleHandler
        final CommonTypeHandler commonHandler;
        final boolean isCharLike; // character/text column; in-row routing to TSTZ normalization decided by value shape

        ColumnDescriptor(String columnLabel, String normalizedTypeName,
            SimpleTypeHandler simpleHandler, CommonTypeHandler commonHandler, boolean isCharLike) {
            this.columnLabel = columnLabel;
            this.normalizedTypeName = normalizedTypeName;
            this.simpleHandler = simpleHandler;
            this.commonHandler = commonHandler;
            this.isCharLike = isCharLike;
        }
    }

    @Override
    protected String convert(ResultSet resultSet, int columnIdx, ResultSetMetaData rsmd) throws SQLException {
        String columnLabel = rsmd.getColumnLabel(columnIdx);
        String columnTypeName = rsmd.getColumnTypeName(columnIdx);
        String normalizedTypeName = normalizeTypeName(columnTypeName);

        // oGRAC-side TIMESTAMP WITH TIME ZONE columns: the JDBC driver's getString() returns junk
        // for sub-second digits (wall-clock time is right but the fraction is garbled, e.g. .999999
        // becomes .064703). The SELECT layer already rewrites such columns as to_char(col) so the
        // server emits the real string (like "2024-07-04 08:00:00.999999 -05:00"), and the JDBC
        // type degrades to TEXT/VARCHAR. Returning that raw string through the plain text handler
        // would disagree with the Oracle side (normalized to GMT+8), so when a character-like
        // column holds a "TSTZ string with timezone suffix", route it to the TIMESTAMPTZ handler
        // for GMT+8 normalization.
        if (isCharLike(normalizedTypeName)
                && OgracTypeHandlerFactory.isTstzFormattedValue(resultSet.getString(columnIdx))) {
            normalizedTypeName = OgracType.TIMESTAMPTZ;
        }

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
     * Whether the normalized type is character/text-like. Only such columns may be routed back to
     * the TSTZ handler after the to_char(col) rewrite, which avoids hitting real
     * TIMESTAMP/TIMESTAMPLTZ columns (they go through their own datetime handlers).
     *
     * @param normalizedTypeName the normalized column type name
     * @return true if it is a character/text type
     */
    private boolean isCharLike(String normalizedTypeName) {
        return OgracType.CHAR.equals(normalizedTypeName)
                || OgracType.VARCHAR.equals(normalizedTypeName)
                || OgracType.VARCHAR2.equals(normalizedTypeName)
                || OgracType.NCHAR.equals(normalizedTypeName)
                || OgracType.NVARCHAR2.equals(normalizedTypeName)
                || OgracType.TEXT.equals(normalizedTypeName)
                || OgracType.LONG.equals(normalizedTypeName);
    }

    /**
     * Normalize the column type name: uppercase it, strip precision suffixes (e.g. CHAR(10 BYTE),
     * NUMBER(38,12)), and map the variant names oGRAC JDBC returns to the constant names used by
     * registered handlers.
     *
     * @param columnTypeName the raw column type name returned by JDBC, may be null
     * @return the normalized type name; an empty string when the input is null (all handler
     *         lookups miss and the default handler applies)
     */
    private String normalizeTypeName(String columnTypeName) {
        if (columnTypeName == null) {
            return "";
        }
        String upperTypeName = columnTypeName.toUpperCase(Locale.ENGLISH);

        // The migration converts Oracle CHAR/VARCHAR2/NCHAR/NVARCHAR2 into oGRAC CHAR/VARCHAR.
        // Type names returned by oGRAC JDBC may carry a precision suffix (e.g. CHAR(10 BYTE),
        // VARCHAR(100 CHAR)); normalize them, otherwise the suffixed names miss the registered
        // handlers and fall back to the default ObjectHandler.
        if (upperTypeName.startsWith("CHAR(")) {
            return OgracType.CHAR;
        }
        if (upperTypeName.startsWith("VARCHAR(")) {
            return OgracType.VARCHAR;
        }
        if (upperTypeName.startsWith("NCHAR(")) {
            return OgracType.NCHAR;
        }
        if (upperTypeName.startsWith("NVARCHAR2(")) {
            return OgracType.NVARCHAR2;
        }
        // The migration converts Oracle INTEGER/INT/SMALLINT/FLOAT(n!=126) into oGRAC NUMBER(p)
        // or NUMBER(p,s); type names returned by JDBC may carry precision (e.g. NUMBER(38),
        // NUMBER(38,12)), so normalize them to NUMBER.
        if (upperTypeName.startsWith("NUMBER(")) {
            return OgracType.NUMBER;
        }
        // RAW may be returned with a length (e.g. RAW(2000)); normalize to RAW
        if (upperTypeName.startsWith("RAW(")) {
            return OgracType.RAW;
        }
        if (upperTypeName.startsWith("TIMESTAMP(")) {
            if (upperTypeName.contains("WITH TIME ZONE")) {
                return OgracType.TIMESTAMPTZ_OFFICIAL;
            } else if (upperTypeName.contains("WITH LOCAL TIME ZONE")) {
                return OgracType.TIMESTAMPLTZ_OFFICIAL;
            } else {
                return OgracType.TIMESTAMP;
            }
        }
        if (upperTypeName.startsWith("INTERVAL YEAR")) {
            return OgracType.INTERVAL_YEAR_TO_MONTH;
        }
        if (upperTypeName.startsWith("INTERVAL DAY")) {
            return OgracType.INTERVAL_DAY_TO_SECOND;
        }
        if (upperTypeName.endsWith(".XMLTYPE")) {
            return OracleResultSetHandler.OracleType.XMLTYPE;
        }
        return upperTypeName;
    }

    /**
     * oGRAC-side JDBC column type name constants: covering character, number, float, datetime,
     * interval, binary and LOB types, plus the variant names oGRAC/the migration tool returns.
     * Handler registration and this class's normalization logic both key on these constants.
     */
    interface OgracType {
        /**
         * Fixed-length character type CHAR
         */
        String CHAR = "CHAR";

        /**
         * Variable-length character type VARCHAR
         */
        String VARCHAR = "VARCHAR";

        /**
         * Oracle variable-length character type VARCHAR2 (oGRAC compatibility alias)
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
         * Long text type LONG
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
         * Big integer type BIGINT
         */
        String BIGINT = "BIGINT";

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

        // Type names returned by openGauss JDBC (actual column types after migration); handlers
        // must be registered explicitly so they do not fall back to defaultObjectHandler
        /**
         * Number type NUMERIC (openGauss JDBC name)
         */
        String NUMERIC = "NUMERIC";

        /**
         * Number type DECIMAL
         */
        String DECIMAL = "DECIMAL";

        /**
         * Float type REAL (openGauss 32-bit float)
         */
        String REAL = "REAL";

        /**
         * Unsigned integer, same semantics as BIGINT
         */
        String UINT = "UINT";

        /**
         * NUMBER variant (getObject returns String), same semantics as NUMBER
         */
        String NUMBER2 = "NUMBER2";

        /**
         * Long text, same semantics as VARCHAR
         */
        String TEXT = "TEXT";

        /**
         * BLOB variant
         */
        String IMAGE = "IMAGE";

        /**
         * Binary; getObject returns byte[] (the default handler would print junk like [B@xxx,
         * so it must be registered explicitly)
         */
        String BINARY = "BINARY";

        /**
         * Variable-length binary, same as BINARY
         */
        String VARBINARY = "VARBINARY";

        /**
         * Name oGRAC JDBC actually returns for TIMESTAMP WITH TIME ZONE
         */
        String TIMESTAMP_TZ = "TIMESTAMP_TZ";

        /**
         * Name oGRAC JDBC actually returns for TIMESTAMP WITH LOCAL TIME ZONE
         */
        String TIMESTAMP_LTZ = "TIMESTAMP_LTZ";

        /**
         * Timestamp variant
         */
        String UTC = "UTC";

        /**
         * Presumed migration target of Oracle INTERVAL YEAR TO MONTH
         */
        String DATE_YEAR_MONTH = "DATE_YEAR_MONTH";

        /**
         * Presumed migration target of Oracle INTERVAL DAY TO SECOND
         */
        String DATE_DAY_HMS = "DATE_DAY_HMS";

        /**
         * Date type DATE (no time part)
         */
        String DATE = "DATE";

        /**
         * Timestamp type TIMESTAMP
         */
        String TIMESTAMP = "TIMESTAMP";

        /**
         * Timezone timestamp name returned by oGRAC JDBC (underscore form)
         */
        String TIMESTAMPTZ = "TIMESTAMP_WITH_TIMEZONE";

        /**
         * Standard spelling of the timezone timestamp type name
         */
        String TIMESTAMPTZ_OFFICIAL = "TIMESTAMP WITH TIME ZONE";

        /**
         * Local-timezone timestamp name returned by oGRAC JDBC (underscore form)
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
         * Binary large object BLOB
         */
        String BLOB = "BLOB";

        /**
         * Character large object CLOB
         */
        String CLOB = "CLOB";

        /**
         * National charset large object NCLOB
         */
        String NCLOB = "NCLOB";

        /**
         * XML type XMLTYPE
         */
        String XMLTYPE = "XMLTYPE";

        /**
         * Numeric type name set (used to decide whether a column type is numeric/float)
         */
        List<String> DIGIT_TYPES = List.of(NUMBER, NUMBER0, INTEGER, INT, SMALLINT, FLOAT, BIGINT,
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
