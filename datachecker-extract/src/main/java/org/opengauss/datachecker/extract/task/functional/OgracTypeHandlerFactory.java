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

package org.opengauss.datachecker.extract.task.functional;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.util.DateTimeFormatterMap;
import org.opengauss.datachecker.common.util.HexUtil;
import org.opengauss.datachecker.common.util.LogUtils;

import java.io.BufferedReader;
import java.io.IOException;
import java.math.BigDecimal;
import java.math.RoundingMode;
import java.sql.Clob;
import java.sql.Date;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Timestamp;
import java.time.DateTimeException;
import java.time.Instant;
import java.time.LocalDate;
import java.time.LocalDateTime;
import java.time.LocalTime;
import java.time.ZoneId;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Calendar;
import java.util.Objects;
import java.util.Optional;
import java.util.OptionalInt;
import java.util.TimeZone;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * Type handler factory for the oGRAC source database: builds value-to-string
 * handlers per column type, supporting both STRICT and COMPATIBLE precision modes.
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
public class OgracTypeHandlerFactory extends SimpleTypeHandlerFactory {
    private static final Logger LOG = LogUtils.getLogger(OgracTypeHandlerFactory.class);
    private static final String NULL = null;
    private static final DateTimeFormatter DATE = DateTimeFormatter.ofPattern("yyyy-MM-dd");
    private static final DateTimeFormatterMap TIMESTAMP_MAPPER = new DateTimeFormatterMap();

    /**
     * COMPATIBLE float mantissa rounding digits: makes 139840692224.0023245... compare equal to
     * the migration-truncated 139840692224.002. Default 3 decimal places (HALF_UP); adjust for
     * looser/stricter comparison as needed.
     */
    private static final int COMPATIBLE_FLOAT_SCALE = 3;

    private static final Pattern INTERVAL_YM_PATTERN = Pattern.compile("^(-?)(\\d+)-(\\d{1,2})$");

    private static final Pattern INTERVAL_DS_PATTERN =
        Pattern.compile("^(-?)(\\d+)\\s+(\\d+):(\\d+):(\\d+)(?:\\.(\\d+))?$");

    /**
     * Matches a TSTZ string value: datetime part + optional timezone suffix (offset or named zone).
     * Group 1=date, group 2=time (with optional fraction), group 3=timezone suffix
     */
    private static final Pattern TZ_VALUE_PATTERN =
        Pattern.compile("^(\\d{1,4}-\\d{2}-\\d{2})[ T](\\d{2}:\\d{2}:\\d{2}(?:\\.\\d+)?)\\s*(.*)$");

    // ==================== Kernel compatibility: COMPATIBLE ====================

    /**
     * <pre>
     * Create the datetime handler truncated to seconds (COMPATIBLE).
     * To keep Oracle and oGRAC checks consistent, times are uniformly formatted to whole seconds,
     * ignoring the microsecond part.
     * Applies to: TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createTruncatedToSecondDateTimeCompatibleHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            final Timestamp timestamp =
                resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
            if (resultSet.wasNull() || Objects.isNull(timestamp)) {
                return NULL;
            }
            DateTimeFormatter dateTimeFormatter = DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss");
            return dateTimeFormatter.format(timestamp.toLocalDateTime());
        };
    }

    /**
     * Create the oGRAC number handler (COMPATIBLE, low-precision mode).
     * Uses getString() (not getDouble() — the oGRAC driver throws for getDouble() on BIGINT)
     * + double flattening; both sides compare at double precision, tolerating precision loss.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createOgracBigDecimalHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            String stringValue = resultSet.getString(columnIdx);
            if (resultSet.wasNull() || stringValue == null) {
                return NULL;
            }
            String trimmed = stringValue.trim();
            if (trimmed.isEmpty()) {
                return NULL;
            }
            if (isNaNOrInfinityString(trimmed)) {
                return trimmed;
            }
            // Defensive: for some values the oGRAC driver's getString() may return a non-numeric
            // string (first char is a letter); new BigDecimal would throw NumberFormatException
            // and fail the whole slice.
            if (Character.isLetter(trimmed.charAt(0))) {
                LOG.warn("oGRAC COMPATIBLE number type got non-numeric string, return as-is: value=[{}], length={}",
                    trimmed, trimmed.length());
                return trimmed;
            }
            try {
                double doubleValue = new BigDecimal(trimmed).doubleValue();
                if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                    return trimmed;
                }
                return BigDecimal.valueOf(doubleValue).toPlainString();
            } catch (NumberFormatException e) {
                LOG.warn("oGRAC COMPATIBLE number type parse failed, return as-is: value=[{}]", trimmed);
                return trimmed;
            }
        };
    }

    /**
     * Create the generic object type handler (Safe version: no wasNull, direct null check).
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createObjectHandlerSafe() {
        return (resultSet, columnLabel) -> {
            Object object = resultSet.getObject(columnLabel);
            return object == null ? NULL : object.toString();
        };
    }

    /**
     * Create the Oracle CLOB type handler.
     * Gets the Clob object via getClob() and checks for null first, then calls getCharacterStream()
     * on the non-null Clob to read the content, avoiding an NPE from calling getCharacterStream()
     * directly on a null CLOB. Empty CLOBs (empty_clob) and null both return null, avoiding hash
     * mismatches from empty vs null between the two sides.
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleClobHandlerSafe() {
        return (resultSet, columnLabel) -> {
            // Both sides (Oracle/oGRAC) share this handler and must take the same
            // getClob + character-stream read path, so newline normalization stays consistent:
            // readLine swallows newlines, and if one side returns the getObject String as-is
            // while the other reads via stream, every newline-containing CLOB would hash differently.
            Clob clob = resultSet.getClob(columnLabel);
            if (clob == null) {
                return NULL;
            }
            StringBuilder sb = new StringBuilder();
            BufferedReader bf = null;
            try {
                bf = new BufferedReader(clob.getCharacterStream());
                String line;
                while ((line = bf.readLine()) != null) {
                    sb.append(line);
                }
            } catch (IOException io) {
                LOG.error("{}read clobToString error", ErrorCode.EXECUTE_QUERY_SQL);
            } finally {
                closeBufferedReader(bf);
                // Do not call clob.free(): ojdbc6's Clob.free() adds an extra network round trip
                // per row per column, which in large-slice scenarios means millions of RPCs that
                // crush extraction throughput; the openGauss side's free() is a local no-op.
                // LOB resources are released by the driver when the ResultSet/Statement closes.
            }
            String result = sb.toString();
            return result.isEmpty() ? NULL : result;
        };
    }

    /**
     * Create the Oracle BLOB type handler (Safe version: no wasNull).
     * Empty byte arrays (empty_blob) and null both return null, avoiding hash mismatches from
     * empty vs null between the two sides.
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleBlobHandlerSafe() {
        return (resultSet, columnLabel) -> {
            byte[] bytes = resultSet.getBytes(columnLabel);
            if (bytes == null || bytes.length == 0) {
                return NULL;
            }
            return HexUtil.byteToHexTrim(bytes);
        };
    }

    /**
     * Create the Oracle XML type handler (COMPATIBLE, lenient mode).
     * <p>
     * Oracle XMLTYPE is already serialized to CLOB at the SQL layer via XMLSERIALIZE(... AS CLOB)
     * and normally goes through {@link #createOracleClobHandlerSafe()} character-stream reading;
     * this is a pure fallback: it reads via getString() only when some path still dispatches to
     * this handler by the XMLTYPE type name, avoiding getObject() (which throws an NPE on XMLTYPE
     * under ojdbc6 without the xdb jar).
     * </p>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createOracleXmlHandlerSafe() {
        return (resultSet, columnLabel) -> {
            String value = resultSet.getString(columnLabel);
            if (value == null) {
                return NULL;
            }
            // Strip the XML declaration: <?xml version="1.0" ... ?> (may span lines)
            value = value.replaceFirst("<\\?xml[^>]*\\?>", "");
            // Trim leading/trailing whitespace/newlines to align with the oGRAC CLOB stream read's plain text
            value = value.trim();
            return value.isEmpty() ? NULL : value;
        };
    }

    // ==================== Exact comparison: STRICT ====================

    /**
     * <pre>
     * Create the full-precision datetime handler (STRICT).
     * Keeps sub-second digits per column scale, to catch TIMESTAMP precision truncation during
     * migration (e.g. Oracle TIMESTAMP(9) migrated to oGRAC TIMESTAMP(6)).
     * Applies to: TIMESTAMP, TIMESTAMP WITH TIME ZONE, TIMESTAMP WITH LOCAL TIME ZONE
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createFullPrecisionDateTimeHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            final Timestamp timestamp =
                resultSet.getTimestamp(columnIdx, Calendar.getInstance(TimeZone.getTimeZone("GMT+8")));
            if (resultSet.wasNull() || Objects.isNull(timestamp)) {
                return NULL;
            }
            LocalDateTime localDateTime = timestamp.toLocalDateTime();
            // The oGRAC JDBC driver always reports scale 0 for TIMESTAMP columns, so taking
            // precision from rsmd.getScale() would drop the sub-second part entirely, while
            // oGRAC actually stores microseconds. Derive the fractional precision from the
            // value's actual nanoseconds instead, uniformly on the Oracle/oGRAC sides.
            DateTimeFormatter dateTimeFormatter = TIMESTAMP_MAPPER.get(getNanoPrecision(localDateTime.getNano()));
            return dateTimeFormatter.format(localDateTime);
        };
    }

    /**
     * Derive how many sub-second fractional digits to keep from the nanosecond value.
     * E.g. nanos=123456000 -> 6 (.123456); nanos=120000000 -> 2 (.12); nanos=0 -> 0 (seconds only).
     *
     * @param nanos nanoseconds (0~999999999)
     * @return number of fractional digits (0~9)
     */
    private int getNanoPrecision(int nanos) {
        if (nanos == 0) {
            return 0;
        }
        int precision = 9;
        int value = nanos;
        while (value % 10 == 0) {
            value /= 10;
            precision--;
        }
        return precision;
    }

    /**
     * Create the DATE type handler (Safe version: wasNull + null check).
     * Shared by both precision schemes; DATE has no sub-second precision difference.
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createDateHandlerSafe() {
        return (resultSet, columnLabel) -> {
            final Date date = resultSet.getDate(columnLabel);
            if (resultSet.wasNull() || Objects.isNull(date)) {
                return NULL;
            }
            return DATE.format(date.toLocalDate());
        };
    }

    /**
     * <pre>
     * Create the Oracle number full-precision handler (STRICT, high-precision mode).
     * Reads the database's original string via getString(), keeps the original precision by
     * constructing BigDecimal(String), then flattens scientific notation with toPlainString().
     * Strict mode keeps real precision differences. Special values like NaN / Infinity are
     * returned as their string representations.
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createStrictOracleBigDecimalHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            String stringValue = resultSet.getString(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return NULL;
            }
            String trimmed = stringValue.trim();
            if (isNaNOrInfinityString(trimmed)) {
                return trimmed;
            }
            BigDecimal decimalValue = new BigDecimal(trimmed);
            return decimalValue.toPlainString();
        };
    }

    /**
     * <pre>
     * Create the oGRAC number full-precision handler (STRICT, high-precision mode).
     * Reads the database's original string via getString(), keeps the original precision by
     * constructing BigDecimal(String), then flattens scientific notation with toPlainString().
     * Strict mode keeps real precision differences. Special values like NaN / Infinity are
     * returned as their string representations.
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createStrictOgracBigDecimalHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            String stringValue = resultSet.getString(columnIdx);
            if (resultSet.wasNull()) {
                return NULL;
            }
            if (stringValue == null || stringValue.trim().isEmpty()) {
                return NULL;
            }
            String trimmed = stringValue.trim();
            if (isNaNOrInfinityString(trimmed)) {
                return trimmed;
            }
            // Symmetric with the COMPATIBLE shared handler (createOracleFloatCompatibleHandler):
            // for NULL number columns the oGRAC driver's getString() returns the literal "null"
            // (not Java null, wasNull() is false). Map it back to NULL explicitly so it matches
            // the Oracle source's real NULL in hash and key, instead of being returned as-is by
            // the Character.isLetter defensive branch below and causing a vHash mismatch.
            if ("null".equals(trimmed)) {
                return NULL;
            }
            // Defensive: for some values the oGRAC driver's getString() may return a non-numeric
            // string (first char is a letter, e.g. "null"/special values); new BigDecimal would
            // throw NumberFormatException and fail the whole slice. Use "first char is a letter"
            // as the explicit rule and return as-is, so precise mappings can be added later once
            // located (explicit mapping over exception catching).
            if (Character.isLetter(trimmed.charAt(0))) {
                return trimmed;
            }
            BigDecimal decimalValue = new BigDecimal(trimmed);
            return decimalValue.toPlainString();
        };
    }

    /**
     * <pre>
     * Create the float full-precision handler (STRICT).
     * Uses getDouble() (64-bit) instead of getFloat() (32-bit) to avoid precision loss;
     * converts the double to a plain (non-scientific) numeric string via
     * BigDecimal.valueOf().toPlainString(); special values like NaN and Infinity are returned
     * as their string representations.
     * </pre>
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createStrictFloatHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            // For NULL float/double columns the oGRAC driver's wasNull() always returns false
            // (getObject returning null is the reliable signal), so use getObject()==null to
            // detect NULL and avoid it being read as "0.0" and causing a vHash mismatch.
            if (resultSet.getObject(columnIdx) == null) {
                return NULL;
            }
            double doubleValue = resultSet.getDouble(columnIdx);
            if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
                return String.valueOf(doubleValue);
            }
            return BigDecimal.valueOf(doubleValue).toPlainString();
        };
    }

    /**
     * Create the Oracle NUMBER/FLOAT handler (COMPATIBLE).
     * Distinguishes two migration scenarios by scale/precision (symmetric on both sides):
     *   - FLOAT(126) (scale=-127, precision=126) -> oGRAC REAL: dispatch by the actual value
     *     type — getDouble() for double-precision storage, getFloat() for float4 storage
     *     (see mapFloatOrDouble).
     *   - Plain NUMBER / FLOAT(n&lt;126) -> oGRAC DECIMAL(38,12): getString() + round to
     *     12 decimal places.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createOracleFloatCompatibleHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            int scale = rsmd.getScale(columnIdx);
            int precision = rsmd.getPrecision(columnIdx);
            // Oracle FLOAT(126) / DOUBLE PRECISION has scale=-127 and precision=126; it migrates
            // to oGRAC REAL, read by dispatching on the actual value type (see mapFloatOrDouble).
            if (scale < 0 && precision >= 126) {
                // For NULL float columns the oGRAC driver's wasNull() always returns false
                // (getObject returning null is the reliable signal), so use getObject()==null to
                // detect NULL and avoid it being read as "0" and causing a vHash mismatch.
                // FLOAT(126) migrates to oGRAC REAL, but oGRAC actually stores it at double
                // precision (getObject returns Double); always calling getFloat() would round
                // off ~9 integer digits at the 1e11 magnitude (ULP=16384), so dispatch by the
                // actual value type: Float->getFloat(float32), Double->getDouble(float64).
                Object obj = resultSet.getObject(columnIdx);
                if (obj == null) {
                    return NULL;
                }
                return mapFloatOrDouble(obj, resultSet, columnIdx);
            }
            // Plain NUMBER / FLOAT(n<126) -> migrates to oGRAC DECIMAL(38,12); use getString +
            // setScale(12). The oGRAC driver's wasNull() is unreliable for NULL number columns,
            // so likewise use getObject()==null to prevent NULL from being read as "0" via
            // getString() (REAL/FLOAT columns) and causing a vHash mismatch.
            if (resultSet.getObject(columnIdx) == null) {
                return NULL;
            }
            String stringValue = resultSet.getString(columnIdx);
            if (resultSet.wasNull() || stringValue == null) {
                return NULL;
            }
            String trimmed = stringValue.trim();
            if (trimmed.isEmpty()) {
                return NULL;
            }
            // For NULL number columns the oGRAC driver's getString() may return the literal
            // "null" (not Java null); map it back to NULL explicitly so it matches the Oracle
            // source's real NULL (Java null) in hash and key, and to avoid triggering the
            // NumberFormatException fallback warning from new BigDecimal below.
            if ("null".equals(trimmed)) {
                return NULL;
            }
            if (isNaNOrInfinityString(trimmed)) {
                return trimmed;
            }
            try {
                BigDecimal decimalValue = new BigDecimal(trimmed);
                // Migration target is oGRAC DECIMAL(38,12); round uniformly to 12 decimal places
                BigDecimal rounded = decimalValue.setScale(12, RoundingMode.HALF_UP);
                return rounded.stripTrailingZeros().toPlainString();
            } catch (NumberFormatException e) {
                LOG.warn("Oracle FLOAT type parse failed, return as-is: value=[{}]", trimmed);
                return trimmed;
            }
        };
    }

    // ==================== INTERVAL and float normalization handlers ====================

    /**
     * Create the BINARY_FLOAT / BINARY_DOUBLE / DOUBLE_PRECISION / REAL handler (COMPATIBLE).
     * Dispatches by the actual value type (see mapFloatOrDouble): float4 (getObject returns
     * Float) uses getFloat() to output the shortest float32 decimal representation, avoiding
     * precision noise like "0.10000000149011612" from double widening; double precision
     * (getObject returns Double, including when the oGRAC driver misreports a double column as
     * REAL) uses getDouble() to keep the actual precision, avoiding float32 dropping too much
     * at large magnitudes. Symmetric and consistent on both sides.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createBinaryFloatCompatibleHandler() {
        return (resultSet, columnIdx, rsmd) -> {
            // For NULL float columns the oGRAC driver's wasNull() always returns false
            // (getObject returning null is the reliable signal), so use getObject()==null to
            // detect NULL and avoid it being read as "0" and causing a vHash mismatch.
            // This handler is registered for BINARY_FLOAT/BINARY_DOUBLE/DOUBLE_PRECISION/REAL,
            // but the oGRAC driver also reports double-precision columns as REAL (getObject
            // returns Double); always calling getFloat() would round off ~9 integer digits at
            // the 1e11 magnitude (ULP=16384), so dispatch by the actual value type to avoid
            // over-rounding in the lenient mode.
            Object obj = resultSet.getObject(columnIdx);
            if (obj == null) {
                return NULL;
            }
            return mapFloatOrDouble(obj, resultSet, columnIdx);
        };
    }

    /**
     * Normalize a float value by its actual type (shared by COMPATIBLE).
     * The oGRAC driver's metadata for REAL-like columns is unreliable: the same type name may
     * be a true float4 (getObject returns Float) or double-precision storage (getObject
     * returns Double).
     *   - Float  -> the shortest float32 decimal representation via getFloat(), avoiding
     *     double-widening noise like 0.10000000149011612;
     *   - Double -> getDouble() keeps the actual double precision, avoiding float32 dropping
     *     too much at large magnitudes.
     * Both sides use the same handler symmetrically, so the same stored value always yields
     * the same string, avoiding false hash mismatches.
     *
     * @param obj the actual object returned by resultSet.getObject (Float or Double)
     * @param resultSet JDBC result set
     * @param columnIdx column number (1-based)
     * @return the normalized float string
     * @throws SQLException thrown when reading the column value fails
     */
    private String mapFloatOrDouble(Object obj, ResultSet resultSet, int columnIdx) throws SQLException {
        if (obj instanceof Float) {
            float floatValue = (Float) obj;
            if (Float.isNaN(floatValue) || Float.isInfinite(floatValue)) {
                return String.valueOf(floatValue);
            }
            // Float.toString outputs scientific notation for large/small values (e.g. 1.79E38);
            // flatten with BigDecimal.toPlainString, and round the mantissa to
            // COMPATIBLE_FLOAT_SCALE digits (e.g. 0.1f -> "0.1"), same scale as the double path.
            return new BigDecimal(Float.toString(floatValue))
                .setScale(COMPATIBLE_FLOAT_SCALE, RoundingMode.HALF_UP)
                .stripTrailingZeros().toPlainString();
        }
        double doubleValue = (obj instanceof Double) ? (Double) obj : resultSet.getDouble(columnIdx);
        if (Double.isNaN(doubleValue) || Double.isInfinite(doubleValue)) {
            return String.valueOf(doubleValue);
        }
        // Lenient mode: keep up to COMPATIBLE_FLOAT_SCALE decimal places (default 3).
        // Makes migration tail-truncated values like 139840692224.0023245... and
        // 139840692224.002 compare equal, avoiding false mismatches from oGRAC storage
        // truncation even after getDouble preserves the value faithfully.
        return BigDecimal.valueOf(doubleValue)
            .setScale(COMPATIBLE_FLOAT_SCALE, RoundingMode.HALF_UP)
            .stripTrailingZeros().toPlainString();
    }

    /**
     * <pre>
     * Create the INTERVAL YEAR TO MONTH normalization handler (for the Oracle source side).
     * Oracle outputs "1-6" / "-2-11"; normalize to the canonical "[+/-]YY-MM" format (e.g.
     * "+01-06", "-02-11") consistent with the oGRAC side's to_char(DATE_YEAR_MONTH), keeping
     * the sign, so both sides compare equal.
     * The oGRAC side's DATE_YEAR_MONTH is already wrapped with to_char() by SelectSqlBuilder
     * and yields the canonical format directly.
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createIntervalYearMonthHandler() {
        return (resultSet, columnLabel) -> {
            String value = resultSet.getString(columnLabel);
            if (resultSet.wasNull() || value == null) {
                return NULL;
            }
            return normalizeIntervalYearMonth(value.trim());
        };
    }

    /**
     * Normalize an INTERVAL YEAR TO MONTH string to the canonical "[+/-]YY-MM" format (year and
     * month zero-padded to 2 digits, explicit sign).
     * Stays consistent with the oGRAC side's to_char(DATE_YEAR_MONTH) output (e.g. "+01-06",
     * "-02-11") so both sides compare equal.
     * Input examples: "1-6" -> "+01-06", "0-0" -> "+00-00", "-2-11" -> "-02-11".
     *
     * @param value the raw INTERVAL YEAR TO MONTH string (e.g. "1-6", "-2-11")
     * @return the normalized canonical-format string (e.g. "+01-06", "-02-11")
     */
    private static String normalizeIntervalYearMonth(String value) {
        // Format: [-]Y-M, where Y is the year (possibly multi-digit) and M the month (1-2 digits)
        Matcher matcher = INTERVAL_YM_PATTERN.matcher(value);
        if (matcher.matches()) {
            String sign = matcher.group(1);
            String year = matcher.group(2);
            String month = matcher.group(3);
            String normalizedSign = "-".equals(sign) ? "-" : "+";
            return normalizedSign + String.format("%02d", Integer.parseInt(year))
                + "-" + String.format("%02d", Integer.parseInt(month));
        }
        return value;
    }

    /**
     * <pre>
     * Create the INTERVAL DAY TO SECOND normalization handler.
     * Oracle can carry up to 9 fractional digits (e.g. "5 10:30:45.123456789") while oGRAC has
     * at most 6 (e.g. "5 10:30:45.123457").
     * Both sides normalize uniformly: round the fraction to 6 digits, handle carry, strip
     * trailing zeros, and zero-pad the time parts.
     * This resolves both precision and format differences (e.g. oGRAC outputs "0:0:0.0"
     * instead of "00:00:00").
     * </pre>
     *
     * @return SimpleTypeHandler
     */
    public SimpleTypeHandler createIntervalDaySecondHandler() {
        return (resultSet, columnLabel) -> {
            String value = resultSet.getString(columnLabel);
            if (resultSet.wasNull() || value == null) {
                return NULL;
            }
            return normalizeIntervalDaySecond(value.trim());
        };
    }

    /**
     * Normalize an INTERVAL DAY TO SECOND string.
     * Input format:  [-]D HH:MM:SS.nnnnnnnnn (variable fraction digits)
     * Output format: [-]D HH:MM:SS.nnnnnn (6 fraction digits with trailing zeros stripped;
     * outputs D HH:MM:SS when there is no fraction)
     * Handles rounding carry (second->minute->hour->day).
     *
     * @param value the raw INTERVAL DAY TO SECOND string (e.g. "5 10:30:45.123456789")
     * @return the normalized canonical-format string (e.g. "5 10:30:45.123457")
     */
    private static String normalizeIntervalDaySecond(String value) {
        // Format: [-]D H:M:S or [-]D H:M:S.nnn...
        Matcher matcher = INTERVAL_DS_PATTERN.matcher(value);
        if (!matcher.matches()) {
            return value;
        }
        String secondStr = matcher.group(5);
        String fractionStr = matcher.group(6); // may be null

        // Merge the second and fraction parts into a BigDecimal, then round to 6 digits
        BigDecimal secondWithFraction = new BigDecimal(secondStr);
        if (fractionStr != null && !fractionStr.isEmpty()) {
            secondWithFraction = secondWithFraction.add(new BigDecimal("0." + fractionStr));
        }
        // Round to 6 decimal places
        BigDecimal rounded = secondWithFraction.setScale(6, RoundingMode.HALF_UP);

        // Extract the integer seconds and the fraction part
        int intSecond = rounded.intValue();
        String fracPart = rounded.subtract(new BigDecimal(intSecond))
            .setScale(6, RoundingMode.UNNECESSARY)
            .toPlainString(); // "0.nnnnnn"
        // Strip the "0." prefix
        fracPart = fracPart.substring(2);

        // Handle carry: seconds may be 60 due to rounding
        int day = Integer.parseInt(matcher.group(2));
        int hour = Integer.parseInt(matcher.group(3));
        int minute = Integer.parseInt(matcher.group(4));
        if (intSecond >= 60) {
            intSecond -= 60;
            minute += 1;
        }
        if (minute >= 60) {
            minute -= 60;
            hour += 1;
        }
        if (hour >= 24) {
            hour -= 24;
            day += 1;
        }

        // Format: zero-pad the time parts, strip trailing fraction zeros
        String sign = matcher.group(1);
        String timePart = String.format("%02d:%02d:%02d", hour, minute, intSecond);
        // Strip trailing zeros from the fraction
        String trimmedFrac = fracPart.replaceAll("0+$", "");
        if (trimmedFrac.isEmpty()) {
            return sign + day + " " + timePart;
        }
        return sign + day + " " + timePart + "." + trimmedFrac;
    }

    /**
     * Create the Oracle-side timezone-timestamp GMT+8 normalization handler (STRICT:
     * sub-second kept).
     * Uses getTimestamp(col, Calendar UTC) to get the absolute instant, then
     * toInstant().atZone(GMT+8), avoiding the Oracle JDBC issue where timezone-carrying types
     * return the original zone's wall clock and end up 8 hours off (a binary-read class of bugs).
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createTimestampZoneGmt8HandlerStrict() {
        return (resultSet, columnIdx, rsmd) -> {
            // Use Calendar UTC to get the absolute instant, avoiding Oracle JDBC's getTimestamp(col)
            // on TIMESTAMP_LTZ returning the original zone's wall clock and ending up 8 hours off
            // (a binary-read class of bugs).
            Timestamp timestamp = resultSet.getTimestamp(columnIdx,
                Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (resultSet.wasNull() || timestamp == null) {
                return NULL;
            }
            // getTimestamp(col, Calendar UTC) returns the absolute instant; unify to GMT+8
            // via toInstant -> atZone(GMT+8)
            LocalDateTime gmt8 = timestamp.toInstant()
                .atZone(ZoneId.of("GMT+8"))
                .toLocalDateTime();
            DateTimeFormatter formatter = TIMESTAMP_MAPPER.get(rsmd.getScale(columnIdx));
            return formatter.format(gmt8);
        };
    }

    /**
     * Create the Oracle-side timezone-timestamp GMT+8 normalization handler (COMPATIBLE:
     * truncated to seconds).
     * Uses getTimestamp(col, Calendar UTC) to get the absolute instant, then
     * toInstant().atZone(GMT+8), avoiding the Oracle JDBC issue where timezone-carrying types
     * return the original zone's wall clock and end up 8 hours off (a binary-read class of bugs).
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createTimestampZoneGmt8HandlerCompatible() {
        return (resultSet, columnIdx, rsmd) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIdx,
                Calendar.getInstance(TimeZone.getTimeZone("UTC")));
            if (resultSet.wasNull() || timestamp == null) {
                return NULL;
            }
            LocalDateTime gmt8 = timestamp.toInstant()
                .atZone(ZoneId.of("GMT+8"))
                .toLocalDateTime();
            return gmt8.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        };
    }

    /**
     * Create the oGRAC timezone-timestamp handler (STRICT: GMT+8 normalization, sub-second kept).
     * oGRAC JDBC's getTimestamp(col, Calendar) for LTZ/TZ ignores the Calendar parameter and
     * treats the stored UTC value as local time, so use toLocalDateTime() to get the raw UTC
     * value, then atZone(UTC).withZoneSameInstant(GMT+8), symmetric with the Oracle side.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createOgracTimestampZoneGmt8HandlerStrict() {
        return (resultSet, columnIdx, rsmd) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIdx);
            if (resultSet.wasNull() || timestamp == null) {
                return NULL;
            }
            // oGRAC JDBC treats the stored UTC value as local time; toLocalDateTime() returns
            // the raw UTC value. Reinterpret it as UTC and convert to GMT+8, equivalent to the
            // Oracle side's getTimestamp(col, Calendar UTC) + atZone(GMT+8).
            LocalDateTime gmt8 = timestamp.toLocalDateTime()
                .atZone(ZoneId.of("UTC"))
                .withZoneSameInstant(ZoneId.of("GMT+8"))
                .toLocalDateTime();
            DateTimeFormatter formatter = TIMESTAMP_MAPPER.get(rsmd.getScale(columnIdx));
            return formatter.format(gmt8);
        };
    }

    /**
     * Create the oGRAC timezone-timestamp handler (COMPATIBLE: GMT+8 normalization, truncated
     * to seconds).
     * oGRAC JDBC's getTimestamp(col, Calendar) for LTZ/TZ ignores the Calendar parameter and
     * treats the stored UTC value as local time, so use toLocalDateTime() to get the raw UTC
     * value, then atZone(UTC).withZoneSameInstant(GMT+8), symmetric with the Oracle side.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createOgracTimestampZoneGmt8HandlerCompatible() {
        return (resultSet, columnIdx, rsmd) -> {
            Timestamp timestamp = resultSet.getTimestamp(columnIdx);
            if (resultSet.wasNull() || timestamp == null) {
                return NULL;
            }
            LocalDateTime gmt8 = timestamp.toLocalDateTime()
                .atZone(ZoneId.of("UTC"))
                .withZoneSameInstant(ZoneId.of("GMT+8"))
                .toLocalDateTime();
            return gmt8.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        };
    }

    /**
     * Whether the string is a "TSTZ string with a timezone offset / named-zone suffix".
     * Used on the oGRAC side: after the SELECT layer rewrites TIMESTAMP WITH TIME ZONE columns
     * as to_char(col), the JDBC type degrades to TEXT/VARCHAR; this value-shape check routes
     * such columns back to the TSTZ (GMT+8 normalization) handler.
     *
     * @param value the string to check
     * @return true if it matches the TSTZ shape with a timezone suffix
     */
    public static boolean isTstzFormattedValue(String value) {
        if (value == null) {
            return false;
        }
        Matcher m = TZ_VALUE_PATTERN.matcher(value.trim());
        if (!m.matches()) {
            return false;
        }
        String tz = m.group(3) == null ? "" : m.group(3).trim();
        return !tz.isEmpty();
    }

    /**
     * Parse a TSTZ string value to an absolute instant.
     * Tolerates various driver outputs:
     * - Named zones: e.g. "America/New_York", "GMT+8", "Z", "UTC"
     * - Numeric offsets: e.g. "+8:00", "08", "-5:00", "+05:00", "-0500", "Z"
     * When the timezone suffix is unrecognizable, interpret it as GMT+8 (the normalization target).
     *
     * @param raw the raw string value
     * @return the absolute instant
     */
    private static Instant parseTzStringToInstant(String raw) {
        String s = raw.trim();
        Matcher m = TZ_VALUE_PATTERN.matcher(s);
        if (!m.matches()) {
            // Unrecognized; interpret as GMT+8
            return parseFlexibleAsGmt8(s);
        }
        String tz = m.group(3) == null ? "" : m.group(3).trim();
        // Parse the datetime; tolerate 1-4 digit years (e.g. "1-01-01" means 0001-01-01)
        LocalDateTime ldt = LocalDateTime.of(parseFlexibleDate(m.group(1)), LocalTime.parse(m.group(2)));
        if (tz.isEmpty()) {
            // No timezone suffix; interpret as GMT+8 (the normalization target)
            return ldt.atZone(ZoneId.of("GMT+8")).toInstant();
        }
        // A suffix containing letters is a named zone / abbreviation (America/New_York,
        // GMT+8, Z, UTC); hand it to ZoneId
        if (tz.chars().anyMatch(Character::isLetter)) {
            try {
                return ldt.atZone(ZoneId.of(tz)).toInstant();
            } catch (DateTimeException e) {
                return ldt.atZone(ZoneId.of("GMT+8")).toInstant();
            }
        }
        // Numeric offset: supports ±H, ±HH, ±H:MM, ±HH:MM, ±HHMM (e.g. -5:00, +08:00, -0500, +8)
        Optional<ZoneOffset> offset = parseTzOffset(tz);
        if (offset.isEmpty()) {
            return ldt.atZone(ZoneId.of("GMT+8")).toInstant();
        }
        return ldt.atOffset(offset.get()).toInstant();
    }

    /**
     * Parse a date string into a {@link LocalDate}, tolerating 1-4 digit years.
     * E.g. "1-01-01" (year 1, i.e. 0001-01-01), "2024-07-04".
     *
     * @param date the date string, format [-]?[Y...Y]-MM-DD, year 1-4 digits
     * @return LocalDate
     */
    private static LocalDate parseFlexibleDate(String date) {
        String[] p = date.split("-");
        return LocalDate.of(Integer.parseInt(p[0]), Integer.parseInt(p[1]), Integer.parseInt(p[2]));
    }

    /**
     * Fallback parse: when the string does not match the standard TSTZ pattern, try hard to
     * parse the datetime and interpret it as GMT+8.
     * Tolerates single/multi-digit years; returns EPOCH on parse failure so a single bad row
     * does not abort the whole table extraction.
     *
     * @param raw the raw string
     * @return the absolute instant
     */
    private static Instant parseFlexibleAsGmt8(String raw) {
        try {
            String s = raw.trim();
            Matcher m = TZ_VALUE_PATTERN.matcher(s);
            if (m.matches()) {
                return LocalDateTime.of(parseFlexibleDate(m.group(1)), LocalTime.parse(m.group(2)))
                    .atZone(ZoneId.of("GMT+8")).toInstant();
            }
            return LocalDateTime.parse(s.replace(' ', 'T'))
                .atZone(ZoneId.of("GMT+8")).toInstant();
        } catch (DateTimeException e) {
            return Instant.EPOCH;
        }
    }

    /**
     * Parse a numeric timezone offset, tolerating multiple forms:
     * "+5", "+05", "+5:00", "+05:00", "+0500", "+0530" and the negative counterparts.
     * <p>
     * Additionally handles the oGRAC JDBC driver's 16-bit wraparound bug for negative
     * offsets: a negative offset (e.g. -5:00 = -300 minutes) gets formatted in the driver
     * string as "+1087:16" (65236 minutes), i.e. the original minutes plus 2^16 (65536).
     * Here, clearly out-of-range offsets are restored to the true negative offset.
     *
     * @param tz the offset string (may carry a sign)
     * @return the corresponding {@link ZoneOffset}; {@link Optional#empty()} when unparseable
     */
    private static Optional<ZoneOffset> parseTzOffset(String tz) {
        String t = tz.trim();
        if (t.isEmpty()) {
            return Optional.empty();
        }
        int sign = 1;
        if (t.charAt(0) == '-') {
            sign = -1;
            t = t.substring(1);
        } else if (t.charAt(0) == '+') {
            t = t.substring(1);
        } else {
            // No sign prefix: treat as a positive offset
            sign = 1;
        }
        OptionalInt hourMinute = parseHourMinute(t);
        if (hourMinute.isEmpty()) {
            return Optional.empty();
        }
        return toZoneOffset(sign, hourMinute.getAsInt());
    }

    /**
     * Parse hours/minutes from a pure-digit or HH:MM string: supports "H", "HH", "HMM",
     * "HHMM", "HH:MM".
     *
     * @param t the offset digit string with the sign removed
     * @return total minutes as hours*60+minutes; {@link OptionalInt#empty()} when malformed
     */
    private static OptionalInt parseHourMinute(String t) {
        if (t.contains(":")) {
            String[] parts = t.split(":");
            if (parts.length != 2 || parts[0].isEmpty() || parts[1].isEmpty()
                || !parts[0].chars().allMatch(Character::isDigit)
                || !parts[1].chars().allMatch(Character::isDigit)) {
                return OptionalInt.empty();
            }
            return OptionalInt.of(Integer.parseInt(parts[0]) * 60
                + Integer.parseInt(parts[1]));
        }
        if (!t.chars().allMatch(Character::isDigit)) {
            return OptionalInt.empty();
        }
        if (t.length() <= 2) {
            // "+5" / "05" / "8": hours only
            return OptionalInt.of(Integer.parseInt(t) * 60);
        } else if (t.length() == 3) {
            // "530": 1-digit hour + 2-digit minutes
            return OptionalInt.of(Integer.parseInt(t.substring(0, 1)) * 60
                + Integer.parseInt(t.substring(1)));
        } else {
            // "0500": 2-digit hour + 2-digit minutes
            return OptionalInt.of(Integer.parseInt(t.substring(0, 2)) * 60
                + Integer.parseInt(t.substring(2)));
        }
    }

    /**
     * Compute the signed offset from the total minutes and sign, restore out-of-range values
     * per oGRAC JDBC's 16-bit wraparound semantics, and finally validate that the real
     * timezone offset does not exceed ±12h (±720 minutes).
     *
     * @param sign      the sign (1 or -1)
     * @param hourMinute total minutes as hours*60+minutes
     * @return the corresponding {@link ZoneOffset}; {@link Optional#empty()} when unparseable
     */
    private static Optional<ZoneOffset> toZoneOffset(int sign, int hourMinute) {
        int totalMinutes = sign * hourMinute;
        // Restore the oGRAC JDBC negative-offset 16-bit wraparound: treat out-of-range
        // offsets as signed 16-bit
        final int wrappedMinutes;
        if (totalMinutes >= 32768) {
            wrappedMinutes = totalMinutes - 65536;
        } else if (totalMinutes <= -32768) {
            wrappedMinutes = totalMinutes + 65536;
        } else {
            // Offset already fits in signed 16-bit; no wraparound needed
            wrappedMinutes = totalMinutes;
        }
        totalMinutes = wrappedMinutes;
        // A real timezone offset never exceeds ±12h (±720 minutes); anything still
        // unreasonable is treated as unparseable
        if (totalMinutes < -720 || totalMinutes > 720) {
            return Optional.empty();
        }
        return Optional.of(ZoneOffset.ofTotalSeconds(totalMinutes * 60));
    }

    /**
     * Create the TSTZ full-precision normalization handler (aligned to the actual precision).
     * Reads the JDBC-formatted string, parses it to an absolute instant, converts to GMT+8,
     * and formats at the column's actual sub-second precision (trailing zeros stripped) —
     * this keeps real precision differences (e.g. Oracle .123456 vs oGRAC .123552) while
     * avoiding false diffs for the same value caused by differing scales.
     * Applies to TIMESTAMP WITH TIME ZONE / TIMESTAMP WITH LOCAL TIME ZONE.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createTimestampTzStringHandlerNanosecond() {
        return (resultSet, columnIdx, rsmd) -> {
            String value = resultSet.getString(columnIdx);
            if (resultSet.wasNull() || value == null) {
                return NULL;
            }
            return normalizeTstzString(value, rsmd.getScale(columnIdx));
        };
    }

    /**
     * Create the TSTZ normalization handler (COMPATIBLE, lenient version).
     * Same value-reading and normalization logic as
     * {@link #createTimestampTzStringHandlerNanosecond()}, with one extra step: strip the
     * trailing zeros of the fraction. After the oGRAC-side SELECT layer rewrites TSTZ columns
     * as to_char(col), the server pads sub-seconds to a fixed 6 digits (e.g. Oracle
     * TIMESTAMP(3)'s .998 is output as .998000 after migration), while the Oracle side
     * outputs .998 per the column scale — the same value differs in digit count across the
     * two sides and causes false hash mismatches.
     * Both sides strip trailing zeros under COMPATIBLE mode so the same value outputs identically.
     *
     * @return CommonTypeHandler
     */
    public CommonTypeHandler createTimestampTzStringHandlerNanosecondCompat() {
        return (resultSet, columnIdx, rsmd) -> {
            String value = resultSet.getString(columnIdx);
            if (resultSet.wasNull() || value == null) {
                return NULL;
            }
            return normalizeTstzStringCompat(value, rsmd.getScale(columnIdx));
        };
    }

    /**
     * Normalize a TSTZ string value to GMT+8 and output at the column precision. Reused by
     * two kinds of callers:
     * - Regular TIMESTAMPTZ columns: called by CommonTypeHandler after reading from the ResultSet;
     * - oGRAC-side character columns whose JDBC type degraded to TEXT/VARCHAR after the
     *   to_char(col) rewrite: routed here by value shape after a single inline read,
     *   avoiding a second getString.
     *
     * @param raw   the raw TSTZ string (null / blank allowed, returns null)
     * @param scale the column scale (0 after the oGRAC rewrite; falls back to deriving the
     *              digit count from raw's fraction part)
     * @return the normalized datetime string; null for null / blank input
     */
    public static String normalizeTstzString(String raw, int scale) {
        if (raw == null || raw.trim().isEmpty()) {
            return NULL;
        }
        String s = raw.trim();
        Instant instant = parseTzStringToInstant(s);
        LocalDateTime gmt8 = instant.atZone(ZoneId.of("GMT+8")).toLocalDateTime();
        // Format at the column precision: the fraction digit count follows the column scale,
        // not a fixed 9: F_TSTZ_6 (scale=6, microseconds) outputs 6 fraction digits, F_TSTZ_9
        // (scale=9, nanoseconds) outputs 9.
        // Note: after the oGRAC-side SELECT layer's to_char(col) rewrite the JDBC type
        // degrades to VARCHAR and rsmd.getScale() returns 0; fall back to deriving the digit
        // count from the raw string's fraction part (to_char outputs the column's real precision).
        String dateTime = gmt8.format(DateTimeFormatter.ofPattern("yyyy-MM-dd HH:mm:ss"));
        int nano = gmt8.getNano();
        int digits = scale;
        if (digits <= 0) {
            digits = fractionDigitsOf(s);
        }
        if (nano == 0 || digits <= 0) {
            return dateTime;
        }
        digits = Math.min(digits, 9);
        // Truncate nanoseconds to the column precision (scale=6 -> microseconds, scale=9 ->
        // nanoseconds) and zero-pad to that digit count
        int unit = (int) Math.pow(10, 9 - digits);
        String fraction = String.format("%0" + digits + "d", nano / unit);
        return dateTime + "." + fraction;
    }

    /**
     * Derive the fraction digit count from the raw TSTZ string's fraction part.
     * Used when the oGRAC side's to_char(col) rewrite makes rsmd.getScale() return 0 (to_char
     * outputs the column's real precision).
     * E.g. "2024-03-15 14:30:45.123456 +08:00" returns 6, "2024-07-04 08:00:00.999999 -05:00"
     * returns 6.
     *
     * @param raw the raw TSTZ string
     * @return the fraction digit count; 0 when there is no fraction or no match
     */
    private static int fractionDigitsOf(String raw) {
        Matcher m = TZ_VALUE_PATTERN.matcher(raw.trim());
        if (!m.matches()) {
            return 0;
        }
        String time = m.group(2);
        int idx = time.indexOf('.');
        if (idx < 0) {
            return 0;
        }
        return time.length() - idx - 1;
    }

    /**
     * COMPATIBLE-mode TSTZ normalization: strips the sub-second fraction's trailing zeros
     * (e.g. .998000 -> .998, .680070 -> .68007) on top of
     * {@link #normalizeTstzString(String, int)}'s output.
     * <p>
     * Background: after the oGRAC side's to_char(col) rewrite the sub-second part is padded
     * to a fixed 6 digits, while the Oracle side outputs per the column scale (TIMESTAMP(3)
     * outputs 3 digits); the same value differs in digit count across the two sides and
     * causes hash mismatches. With both sides stripping trailing zeros under COMPATIBLE
     * mode, the same value always outputs the same minimal fraction; STRICT mode still uses
     * the original method, keeping the full-precision digit-by-digit difference.
     * </p>
     *
     * @param raw   the raw TSTZ string (null / blank allowed, returns null)
     * @param scale the column scale (0 after the oGRAC rewrite; falls back to deriving the
     *              digit count from raw's fraction part)
     * @return the normalized datetime string with fraction trailing zeros stripped; null
     *         for null / blank input
     */
    public static String normalizeTstzStringCompat(String raw, int scale) {
        String normalized = normalizeTstzString(raw, scale);
        if (normalized == null) {
            return NULL;
        }
        int dotIdx = normalized.indexOf('.');
        if (dotIdx < 0) {
            return normalized;
        }
        String fraction = normalized.substring(dotIdx + 1);
        int end = fraction.length();
        while (end > 0 && fraction.charAt(end - 1) == '0') {
            end--;
        }
        // Drop the decimal point when all fraction digits are padding zeros, matching the
        // output shape of a zero sub-second (nano==0)
        if (end == 0) {
            return normalized.substring(0, dotIdx);
        }
        return normalized.substring(0, dotIdx + 1) + fraction.substring(0, end);
    }
}
