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

package org.opengauss.datachecker.extract.task.sql;

import cn.hutool.core.util.StrUtil;
import lombok.Getter;

import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.SqlUtil;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.COLUMN;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.DELIMITER;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.OFFSET;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.ORDER_BY;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.PK_CONDITION;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.PRIMARY_KEY;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.QUERY_BETWEEN_SET;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.SCHEMA;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.START;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.TABLE_NAME;

/**
 * OpenGaussSelectSqlBuilder  Data extraction SQL builder
 *
 * @author wang chao
 * @date 2022/5/12 19:17
 * @since 11
 **/
public class SelectSqlBuilder {
    /**
     * primary key condition :
     * mysql     primaryKey>= '1' COLLATE  utf8mb4_bin and primaryKey <= '100' COLLATE utf8mb4_bin
     * openGauss primaryKey>= '1' COLLATE  "C" and primaryKey <= '100' COLLATE "C"
     */
    private static final String TEMPLATE_PK_CONDITION_FULL = "%s >='%s' %s and %s <= '%s' %s ";
    private static final String TEMPLATE_PK_CONDITION_HALF = "%s >='%s' %s and %s < '%s' %s ";
    private static final String TEMPLATE_GREATER_THAN_OR_EQUAL_TO = "%s >='%s' %s ";
    private static final String TEMPLATE_LESS_THAN_OR_EQUAL_TO = "%s <='%s' %s ";
    private static final String TEMPLATE_LESS_THAN = "%s <'%s' %s ";
    private static final String TEMPLATE_EQUAL_TO = "%s='%s' %s ";
    private static final String QUERY_WHERE_BETWEEN
        = "SELECT :columnsList FROM :schema.:tableName where :pkCondition :orderBy ";
    private static final Map<DataBaseType, SqlGenerate> SQL_GENERATE = new HashMap<>();
    private static final SqlGenerateTemplate GENERATE_TEMPLATE = (template, sqlGenerateMeta) -> template.replace(COLUMN,
            sqlGenerateMeta.getColumns())
        .replace(SCHEMA, sqlGenerateMeta.getSchema())
        .replace(TABLE_NAME, sqlGenerateMeta.getTableName())
        .replace(ORDER_BY, sqlGenerateMeta.getOrder())
        .replace(START, String.valueOf(sqlGenerateMeta.getStart()))
        .replace(OFFSET, String.valueOf(sqlGenerateMeta.getOffset()));
    private static final SqlGenerateTemplate NO_OFFSET_SQL_GENERATE_TEMPLATE
        = (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
        .replace(SCHEMA, sqlGenerateMeta.getSchema())
        .replace(TABLE_NAME, sqlGenerateMeta.getTableName());
    private static final SqlGenerate OFFSET_GENERATE = (sqlGenerateMeta) -> GENERATE_TEMPLATE.replace(
        QuerySqlTemplate.QUERY_OFF_SET, sqlGenerateMeta);
    private static final SqlGenerate NO_OFFSET_GENERATE = (sqlGenerateMeta) -> NO_OFFSET_SQL_GENERATE_TEMPLATE.replace(
        QuerySqlTemplate.QUERY_NO_OFF_SET, sqlGenerateMeta);
    private static final SqlGenerate ORACLE_OFFSET_GENERATE = (sqlGenerateMeta) -> GENERATE_TEMPLATE.replace(
        QuerySqlTemplate.QUERY_ORACLE_OFFSET, sqlGenerateMeta);
    private static final SqlGenerateTemplate QUERY_BETWEEN_TEMPLATE = (template, sqlGenerateMeta) -> template.replace(
            COLUMN, sqlGenerateMeta.getColumns())
        .replace(SCHEMA, sqlGenerateMeta.getSchema())
        .replace(TABLE_NAME, sqlGenerateMeta.getTableName())
        .replace(ORDER_BY, sqlGenerateMeta.getOrder())
        .replace(PRIMARY_KEY, sqlGenerateMeta.getPrimaryKey())
        .replace(START, String.valueOf(sqlGenerateMeta.getStart()))
        .replace(OFFSET, String.valueOf(sqlGenerateMeta.getStart() + sqlGenerateMeta.getOffset() - 1));
    private static final SqlGenerate QUERY_BETWEEN_GENERATE = (sqlGenerateMeta -> QUERY_BETWEEN_TEMPLATE.replace(
        QUERY_BETWEEN_SET, sqlGenerateMeta));

    static {
        SQL_GENERATE.put(DataBaseType.MS, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.OG, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.O, ORACLE_OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.OGRAC, OFFSET_GENERATE);
    }

    private String schema;
    private TableMetadata tableMetadata;
    private long start = 0L;
    private long offset = 0L;
    private String seqStart = "";
    private String seqEnd = "";
    private DataBaseType dataBaseType;
    private boolean isDivisions;
    private boolean isFullCondition;
    private boolean isHalfOpenHalfClosed = true;
    private boolean isFirst = false;
    private boolean isDigit = false;
    private boolean isEnd = false;
    private boolean isCsvMode = false;
    private List<String> inIds = null;

    /**
     * Table fragment query SQL Statement Builder
     *
     * @param tableMetadata tableMetadata
     */
    public SelectSqlBuilder(TableMetadata tableMetadata) {
        this.tableMetadata = tableMetadata;
        this.schema = tableMetadata.getSchema();
        this.dataBaseType = tableMetadata.getDataBaseType();
    }

    /**
     * Table fragment query SQL Statement Builder
     *
     * @param start start
     * @param offset offset
     * @return builder
     */
    public SelectSqlBuilder offset(long start, long offset) {
        this.start = start;
        this.offset = offset;
        return this;
    }

    public SelectSqlBuilder offset(String start, String offset) {
        this.seqStart = start;
        this.seqEnd = offset;
        return this;
    }

    public SelectSqlBuilder offset(Object start, Object offset) {
        if (start instanceof Long) {
            this.start = (long) start;
            this.offset = (long) offset;
        } else {
            if (start instanceof String) {
                this.seqStart = escapeValue((String) start);
            }
            if (offset instanceof String) {
                this.seqEnd = escapeValue((String) offset);
            }
        }
        return this;
    }

    /**
     * current table query sql is divisions
     *
     * @param isDivisions isDivisions
     * @return builder
     */
    public SelectSqlBuilder isDivisions(boolean isDivisions) {
        this.isDivisions = isDivisions;
        return this;
    }

    /**
     * set current key  is digit
     *
     * @param isDigit is digit
     * @return builder
     */
    public SelectSqlBuilder isDigit(boolean isDigit) {
        this.isDigit = isDigit;
        return this;
    }

    public SelectSqlBuilder isFullCondition(boolean isFullCondition) {
        this.isFullCondition = isFullCondition;
        return this;
    }

    public SelectSqlBuilder isHalfOpenHalfClosed(boolean isHalfOpenHalfClosed) {
        this.isHalfOpenHalfClosed = isHalfOpenHalfClosed;
        return this;
    }

    /**
     * Set whether the current check mode is CSV mode.
     *
     * @param isCsvMode isCsvMode
     * @return builder
     */
    public SelectSqlBuilder isCsvMode(boolean isCsvMode) {
        this.isCsvMode = isCsvMode;
        return this;
    }

    /**
     * Table fragment query SQL Statement Builder
     *
     * @return build sql
     */
    public String builder() {
        Assert.isTrue(Objects.nonNull(tableMetadata), Message.TABLE_METADATA_NULL_NOT_TO_BUILD_SQL);
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        Assert.notEmpty(columnsMetas, Message.COLUMN_METADATA_EMPTY_NOT_TO_BUILD_SQL);
        final ConditionLimit conditionLimit = tableMetadata.getConditionLimit();
        String sql;
        if (Objects.nonNull(conditionLimit)) {
            sql = buildSelectSqlConditionLimit(tableMetadata, conditionLimit);
        } else if (isDivisions) {
            if (tableMetadata.isUnionPrimary()) {
                sql = buildSelectSqlWhereInUnionPrimary(tableMetadata);
            } else {
                sql = buildSelectSqlWherePrimary(tableMetadata);
            }
        } else {
            sql = buildSelectSqlOffsetZero(columnsMetas, tableMetadata.getTableName());
        }
        return sql;
    }

    private String buildSelectSqlWhereInUnionPrimary(TableMetadata tableMetadata) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String schemaEscape = escape(schema, dataBaseType);
        String tableName = escape(tableMetadata.getTableName(), dataBaseType);
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        ColumnsMetaData slicePrimaryColumn = tableMetadata.getSliceColumn();
        String primaryKey = escape(slicePrimaryColumn.getColumnName(), dataBaseType);
        final String orderBy = getOrderBy(tableMetadata.getPrimaryMetas(), dataBaseType);
        String pkCondition = primaryKey + " in (" + inIds.stream()
            .map(id -> isDigit ? id : "'" + escapeValue(id) + "'")
            .collect(Collectors.joining(",")) + ")";
        return QUERY_WHERE_BETWEEN.replace(COLUMN, columnNames)
            .replace(SCHEMA, schemaEscape)
            .replace(TABLE_NAME, tableName)
            .replace(PK_CONDITION, pkCondition)
            .replace(ORDER_BY, orderBy);
    }

    /**
     * Escape single quotes of a string primary key value before embedding it into SQL as
     * a literal; a raw quote in the value would break the statement syntax.
     *
     * @param value raw string value from a checkpoint, never null
     * @return escaped value, safe to embed inside a SQL string literal
     */
    private static String escapeValue(String value) {
        return value.replace("'", "''");
    }

    private String buildSelectSqlWherePrimary(TableMetadata tableMetadata) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String schemaEscape = escape(schema, dataBaseType);
        String tableName = escape(tableMetadata.getTableName(), dataBaseType);
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        String primaryKey = escape(tableMetadata.getPrimaryMetas().get(0).getColumnName(), dataBaseType);
        final String orderBy = getOrderBy(tableMetadata.getPrimaryMetas(), dataBaseType);
        String pkCondition;
        if (StringUtils.isNotEmpty(seqStart) && StringUtils.isNotEmpty(seqEnd)) {
            pkCondition = getPkCondition(primaryKey);
        } else {
            pkCondition = getNumberPkCondition(primaryKey);
        }
        return QUERY_WHERE_BETWEEN.replace(COLUMN, columnNames)
            .replace(SCHEMA, schemaEscape)
            .replace(TABLE_NAME, tableName)
            .replace(PK_CONDITION, pkCondition)
            .replace(ORDER_BY, orderBy);
    }

    private String getNumberPkCondition(String primaryKey) {
        if (start == offset) {
            return primaryKey + " = " + start;
        }
        if (isFullCondition) {
            return getNumberPkConditionFull(primaryKey);
        }
        if (isFirst) {
            if (isCsvMode) {
                return primaryKey + "<= " + offset;
            } else {
                return primaryKey + "< " + offset;
            }
        }
        if (isEnd) {
            return primaryKey + ">= " + start;
        }
        if (isHalfOpenHalfClosed) {
            return primaryKey + ">= " + start + " and " + primaryKey + " < " + offset;
        } else {
            return primaryKey + ">= " + start + " and " + primaryKey + " <= " + offset;
        }
    }

    private String getNumberPkConditionFull(String primaryKey) {
        return primaryKey + ">= " + start + " and " + primaryKey + " <= " + offset;
    }

    private String getPkConditionFull(String primaryKey) {
        return String.format(TEMPLATE_PK_CONDITION_FULL, primaryKey, seqStart, getCollate(), primaryKey, seqEnd,
            getCollate());
    }

    private String getPkCondition(String primaryKey) {
        if (StringUtils.equalsIgnoreCase(seqStart, seqEnd)) {
            return String.format(TEMPLATE_EQUAL_TO, primaryKey, seqStart, getCollate());
        }
        if (isFullCondition) {
            return getPkConditionFull(primaryKey);
        }
        if (isFirst) {
            if (isCsvMode) {
                return String.format(TEMPLATE_LESS_THAN_OR_EQUAL_TO, primaryKey, seqEnd, getCollate());
            } else {
                return String.format(TEMPLATE_LESS_THAN, primaryKey, seqEnd, getCollate());
            }
        }
        if (isEnd) {
            return String.format(TEMPLATE_GREATER_THAN_OR_EQUAL_TO, primaryKey, seqStart, getCollate());
        }
        if (isHalfOpenHalfClosed) {
            return String.format(TEMPLATE_PK_CONDITION_HALF, primaryKey, seqStart, getCollate(), primaryKey, seqEnd,
                getCollate());
        } else {
            return String.format(TEMPLATE_PK_CONDITION_FULL, primaryKey, seqStart, getCollate(), primaryKey, seqEnd,
                getCollate());
        }
    }

    private String getCollate() {
        if (isDigit) {
            return "";
        }
        if (StrUtil.isNotEmpty(tableMetadata.getTableCollation())) {
            return " COLLATE " + tableMetadata.getTableCollation();
        } else {
            switch (dataBaseType) {
                case MS:
                    return " COLLATE utf8mb4_bin";
                case OG:
                    return " COLLATE \"C\"";
                default:
                    return "";
            }
        }
    }

    private String buildSelectSqlConditionLimit(TableMetadata tableMetadata, ConditionLimit conditionLimit) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        final String schemaEscape = escape(schema, dataBaseType);
        final String tableName = escape(tableMetadata.getTableName(), dataBaseType);
        final String orderBy = getOrderBy(tableMetadata.getPrimaryMetas(), dataBaseType);
        SqlGenerateMeta sqlGenerateMeta = new SqlGenerateMeta(schemaEscape, tableName, columnNames, orderBy,
            conditionLimit.getStart(), conditionLimit.getOffset());
        return getSqlGenerate(dataBaseType).replace(sqlGenerateMeta);
    }

    private String getOrderBy(List<ColumnsMetaData> primaryMetas, DataBaseType dataBaseType) {
        return "order by " + primaryMetas.stream()
            .map(ColumnsMetaData::getColumnName)
            .map(key -> escape(key, dataBaseType) + " asc")
            .collect(Collectors.joining(DELIMITER));
    }

    public String buildSelectSqlOffset(TableMetadata tableMetadata, long start, long offset) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String schemaEscape = escape(schema, dataBaseType);
        String tableName = escape(tableMetadata.getTableName(), dataBaseType);
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        final String orderBy = getOrderBy(tableMetadata.getPrimaryMetas(), dataBaseType);
        SqlGenerateMeta sqlGenerateMeta = null;
        if (!tableMetadata.canUseBetween()) {
            sqlGenerateMeta = new SqlGenerateMeta(schemaEscape, tableName, columnNames, orderBy, start, offset);
            return getSqlGenerate(dataBaseType).replace(sqlGenerateMeta);
        } else {
            String primaryKey = escape(tableMetadata.getPrimaryMetas().get(0).getColumnName(), dataBaseType);
            sqlGenerateMeta = new SqlGenerateMeta(schemaEscape, tableName, columnNames, orderBy, start, offset,
                primaryKey);
            return QUERY_BETWEEN_GENERATE.replace(sqlGenerateMeta);
        }
    }

    private String escape(String content, DataBaseType dataBaseType) {
        if (tableMetadata.isOgCompatibilityB()) {
            return SqlUtil.escape(content, dataBaseType, tableMetadata.isOgCompatibilityB());
        }
        return SqlUtil.escape(content, dataBaseType);
    }

    private String buildSelectSqlOffsetZero(List<ColumnsMetaData> columnsMetas, String tableName) {
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        String schemaEscape = escape(schema, dataBaseType);
        SqlGenerateMeta sqlGenerateMeta = new SqlGenerateMeta(schemaEscape, escape(tableName, dataBaseType),
            columnNames);
        return NO_OFFSET_GENERATE.replace(sqlGenerateMeta);
    }

    private String getColumnNameList(@NonNull List<ColumnsMetaData> columnsMetas, DataBaseType dataBaseType) {
        return columnsMetas.stream()
            .map(column -> buildColumnExpression(column, dataBaseType))
            .collect(Collectors.joining(DELIMITER));
    }

    /**
     * Build the SELECT expression for a single column.
     * <p>
     * Oracle XMLTYPE is an object type; ojdbc6 without the xdb jar (oracle.xdb.XMLType)
     * cannot read it via getObject()/getString() and throws NPE or returns null.
     * So the SQL layer serializes XMLTYPE to CLOB, making the driver return it as a CLOB
     * that goes through createOracleClobHandlerSafe's character-stream reading, symmetric
     * with the oGRAC side (XML migrated to CLOB).
     * An alias keeps the column label unchanged so the column-name-based comparison mapping
     * between the two sides is unaffected.
     *
     * @param column       column metadata
     * @param dataBaseType database type
     * @return the SELECT expression for this column
     */
    private String buildColumnExpression(ColumnsMetaData column, DataBaseType dataBaseType) {
        String escapedName = escape(column.getColumnName(), dataBaseType);
        if (dataBaseType == DataBaseType.O && isXmlType(column)) {
            return "XMLSERIALIZE(CONTENT " + escapedName + " AS CLOB) AS " + escapedName;
        }
        // For oGRAC DATE_YEAR_MONTH / INTERVAL YEAR TO MONTH (the migration target of Oracle
        // INTERVAL YEAR TO MONTH), the driver's ORTimestampUtils.getDateYearMonth() behind
        // JDBC getString() takes the absolute value of the internal integer, losing the sign
        // of negative values (e.g. "-2-11" becomes "2-11").
        // So use to_char() to format the string server-side with the true sign (e.g.
        // "-02-11") and read it via the TEXT character handler.
        // Note: oGRAC metadata (ADM_TAB_COLUMNS.data_type) may report this column's type
        // name as "INTERVAL YEAR TO MONTH".
        if (dataBaseType == DataBaseType.OGRAC && isIntervalYearMonthType(column)) {
            return "to_char(" + escapedName + ") AS " + escapedName;
        }
        // oGRAC TIMESTAMP WITH TIME ZONE: the JDBC driver's getString() returns garbage for
        // sub-seconds (the timezone wall clock is correct but the fraction is scrambled,
        // e.g. .999999 becomes .064703); the real fraction digits cannot be recovered from
        // the driver. So use to_char() to output the real string server-side (like
        // "2024-07-04 08:00:00.999999 -05:00"), then OgracResultSetHandler recognizes the
        // value shape and routes it back to the TSTZ handler for GMT+8 normalization,
        // comparing symmetrically with the Oracle side.
        if (dataBaseType == DataBaseType.OGRAC && isTimestampTzType(column)) {
            return "to_char(" + escapedName + ") AS " + escapedName;
        }
        return escapedName;
    }

    private boolean isXmlType(ColumnsMetaData column) {
        String dataType = column.getDataType();
        return dataType != null && dataType.toUpperCase(Locale.ENGLISH).contains("XMLTYPE");
    }

    private boolean isIntervalYearMonthType(ColumnsMetaData column) {
        String dataType = column.getDataType();
        if (dataType == null) {
            return false;
        }
        String upper = dataType.toUpperCase(Locale.ENGLISH);
        return upper.equals("DATE_YEAR_MONTH") || upper.equals("INTERVAL YEAR TO MONTH");
    }

    /**
     * Whether the column is an oGRAC TIMESTAMP WITH TIME ZONE column.
     * oGRAC metadata (ADM_TAB_COLUMNS.data_type) reports the type name in two forms:
     * 1. "TIMESTAMP_TZ" (the name the oGRAC JDBC driver actually returns, per logs);
     * 2. "TIMESTAMP(6) WITH TIME ZONE" (the Oracle form).
     * Both must match ("TIMESTAMP_TZ" matches exactly, excluding LOCAL, i.e. TIMESTAMP_LTZ).
     *
     * @param column column metadata
     * @return true when it is a TIMESTAMP WITH TIME ZONE column
     */
    private boolean isTimestampTzType(ColumnsMetaData column) {
        String dataType = column.getDataType();
        if (dataType == null) {
            return false;
        }
        String upper = dataType.toUpperCase(Locale.ENGLISH);
        return upper.equals("TIMESTAMP_TZ")
            || (upper.contains("TIMESTAMP") && upper.contains("WITH TIME ZONE"));
    }

    private SqlGenerate getSqlGenerate(DataBaseType dataBaseType) {
        return SQL_GENERATE.get(dataBaseType);
    }

    public void isFirstCondition(boolean first) {
        this.isFirst = first;
    }

    public void isEndCondition(boolean end) {
        this.isEnd = end;
    }

    /**
     * union key table set slice key for sql select where in
     *
     * @param inIds ids
     * @param isDigit is Digit
     */
    public void inIds(List<String> inIds, boolean isDigit) {
        this.inIds = inIds;
        this.isDigit = isDigit;
    }

    @Getter
    static class SqlGenerateMeta {
        private String schema;
        private String tableName;
        private String columns;
        private String primaryKey;
        private String order;
        private long start;
        private long offset;

        public SqlGenerateMeta(String schema, String tableName, String columns) {
            this(schema, tableName, columns, null, 0, 0, null);
        }

        public SqlGenerateMeta(String schema, String tableName, String columns, long start, long offset) {
            this(schema, tableName, columns, "", start, offset, null);
        }

        public SqlGenerateMeta(String schema, String tableName, String columns, String order, long start, long offset) {
            this(schema, tableName, columns, order, start, offset, null);
        }

        public SqlGenerateMeta(String schema, String tableName, String columns, String order, long start, long offset,
            String primaryKey) {
            this.schema = schema;
            this.tableName = tableName;
            this.columns = columns;
            this.order = order;
            this.start = start;
            this.offset = offset;
            this.primaryKey = primaryKey;
        }
    }

    @FunctionalInterface
    interface SqlGenerate {
        /**
         * Generate SQL statement according to SQL generator metadata object
         *
         * @param sqlGenerateMeta SQL generator metadata
         * @return Return fragment query SQL statement
         */
        String replace(SqlGenerateMeta sqlGenerateMeta);
    }

    @FunctionalInterface
    interface SqlEscape {
        /**
         * @param key key
         * @return Return
         */
        String escape(String key);
    }

    @FunctionalInterface
    interface SqlGenerateTemplate {
        /**
         * Generate SQL statement according to SQL generator metadata object
         *
         * @param template SQL template
         * @param sqlGenerateMeta SQL generator metadata
         * @return sql
         */
        String replace(String template, SqlGenerateMeta sqlGenerateMeta);
    }

    interface Message {
        /**
         * error message tips
         */
        String TABLE_METADATA_NULL_NOT_TO_BUILD_SQL = "Abnormal table metadata information, failed to build SQL";

        /**
         * error message tips
         */
        String COLUMN_METADATA_EMPTY_NOT_TO_BUILD_SQL = "Abnormal column metadata information, failed to build SQL";
    }
}
