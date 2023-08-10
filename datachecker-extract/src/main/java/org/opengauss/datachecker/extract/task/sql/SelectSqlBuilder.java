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

import lombok.Getter;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.lang.NonNull;
import org.springframework.util.Assert;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.stream.Collectors;

import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.COLUMN;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.DELIMITER;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.MYSQL_ESCAPE;
import static org.opengauss.datachecker.extract.task.sql.QuerySqlTemplate.OPENGAUSS_ESCAPE;
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
    private static final Map<DataBaseType, SqlGenerate> SQL_GENERATE = new HashMap<>();
    private static final Map<DataBaseType, SqlEscape> ESCAPE = new HashMap<>();
    private static final long OFF_SET_ZERO = 0L;
    private static final SqlGenerateTemplate GENERATE_TEMPLATE =
        (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
                                               .replace(SCHEMA, sqlGenerateMeta.getSchema())
                                               .replace(TABLE_NAME, sqlGenerateMeta.getTableName())
                                               .replace(ORDER_BY, sqlGenerateMeta.getOrder())
                                               .replace(START, String.valueOf(sqlGenerateMeta.getStart()))
                                               .replace(OFFSET, String.valueOf(sqlGenerateMeta.getOffset()));
    private static final SqlGenerateTemplate NO_OFFSET_SQL_GENERATE_TEMPLATE =
        (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
                                               .replace(SCHEMA, sqlGenerateMeta.getSchema())
                                               .replace(TABLE_NAME, sqlGenerateMeta.getTableName());
    private static final SqlGenerate OFFSET_GENERATE =
        (sqlGenerateMeta) -> GENERATE_TEMPLATE.replace(QuerySqlTemplate.QUERY_OFF_SET, sqlGenerateMeta);
    private static final SqlGenerate NO_OFFSET_GENERATE = (sqlGenerateMeta) -> NO_OFFSET_SQL_GENERATE_TEMPLATE
        .replace(QuerySqlTemplate.QUERY_NO_OFF_SET, sqlGenerateMeta);

    private static final SqlGenerateTemplate QUERY_BETWEEN_TEMPLATE =
        (template, sqlGenerateMeta) -> template.replace(COLUMN, sqlGenerateMeta.getColumns())
                                               .replace(SCHEMA, sqlGenerateMeta.getSchema())
                                               .replace(TABLE_NAME, sqlGenerateMeta.getTableName())
                                               .replace(ORDER_BY, sqlGenerateMeta.getOrder())
                                               .replace(PRIMARY_KEY, sqlGenerateMeta.getPrimaryKey())
                                               .replace(START, String.valueOf(sqlGenerateMeta.getStart()))
                                               .replace(OFFSET, String.valueOf(
                                                   sqlGenerateMeta.getStart() + sqlGenerateMeta.getOffset() - 1));
    private static final SqlGenerate QUERY_BETWEEN_GENERATE =
        (sqlGenerateMeta -> QUERY_BETWEEN_TEMPLATE.replace(QUERY_BETWEEN_SET, sqlGenerateMeta));

    static {
        SQL_GENERATE.put(DataBaseType.MS, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.OG, OFFSET_GENERATE);
        SQL_GENERATE.put(DataBaseType.O, OFFSET_GENERATE);
        ESCAPE.put(DataBaseType.MS, (key) -> MYSQL_ESCAPE + key + MYSQL_ESCAPE);
        ESCAPE.put(DataBaseType.OG, (key) -> OPENGAUSS_ESCAPE + key + OPENGAUSS_ESCAPE);
        ESCAPE.put(DataBaseType.O, (key) -> key);
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
     * @param start  start
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
            this.seqStart = (String) start;
            this.seqEnd = (String) offset;
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

    public SelectSqlBuilder isFullCondition(boolean isFullCondition) {
        this.isFullCondition = isFullCondition;
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
        if (Objects.nonNull(conditionLimit)) {
            return buildSelectSqlConditionLimit(tableMetadata, conditionLimit);
        } else if (isDivisions) {
            return buildSelectSqlWherePrimary(tableMetadata);
        } else {
            return buildSelectSqlOffsetZero(columnsMetas, tableMetadata.getTableName());
        }
    }

    String QUERY_WHERE_BETWEEN = "SELECT :columnsList FROM :schema.:tableName where :pkCondition :orderBy ";

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
        return QUERY_WHERE_BETWEEN.replace(COLUMN, columnNames).replace(SCHEMA, schemaEscape)
                                  .replace(TABLE_NAME, tableName).replace(PK_CONDITION, pkCondition)
                                  .replace(ORDER_BY, orderBy);
    }

    private String getNumberPkCondition(String primaryKey) {
        if (isFullCondition) {
            return getNumberPkConditionFull(primaryKey);
        }
        return primaryKey + ">= " + start + " and " + primaryKey + " < " + offset;
    }

    private String getNumberPkConditionFull(String primaryKey) {
        return primaryKey + ">= " + start + " and " + primaryKey + " <= " + offset;
    }

    private String getPkConditionFull(String primaryKey) {
        return primaryKey + ">= '" + seqStart + "' and " + primaryKey + " <= '" + seqEnd + "'";
    }

    private String getPkCondition(String primaryKey) {
        if (isFullCondition) {
            return getPkConditionFull(primaryKey);
        }
        return primaryKey + ">= '" + seqStart + "' and " + primaryKey + " < '" + seqEnd + "'";
    }

    private String buildSelectSqlConditionLimit(TableMetadata tableMetadata, ConditionLimit conditionLimit) {
        List<ColumnsMetaData> columnsMetas = tableMetadata.getColumnsMetas();
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        final String schemaEscape = escape(schema, dataBaseType);
        final String tableName = escape(tableMetadata.getTableName(), dataBaseType);
        final String orderBy = getOrderBy(tableMetadata.getPrimaryMetas(), dataBaseType);
        SqlGenerateMeta sqlGenerateMeta =
            new SqlGenerateMeta(schemaEscape, tableName, columnNames, orderBy, conditionLimit.getStart(),
                conditionLimit.getOffset());
        return getSqlGenerate(dataBaseType).replace(sqlGenerateMeta);
    }

    private String getOrderBy(List<ColumnsMetaData> primaryMetas, DataBaseType dataBaseType) {
        return "order by " + primaryMetas.stream().map(ColumnsMetaData::getColumnName)
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
            sqlGenerateMeta =
                new SqlGenerateMeta(schemaEscape, tableName, columnNames, orderBy, start, offset, primaryKey);
            return QUERY_BETWEEN_GENERATE.replace(sqlGenerateMeta);
        }
    }

    private String escape(String content, DataBaseType dataBaseType) {
        return ESCAPE.get(dataBaseType).escape(content);
    }

    private String buildSelectSqlOffsetZero(List<ColumnsMetaData> columnsMetas, String tableName) {
        String columnNames = getColumnNameList(columnsMetas, dataBaseType);
        String schemaEscape = escape(schema, dataBaseType);
        SqlGenerateMeta sqlGenerateMeta =
            new SqlGenerateMeta(schemaEscape, escape(tableName, dataBaseType), columnNames);
        return NO_OFFSET_GENERATE.replace(sqlGenerateMeta);
    }

    private String getColumnNameList(@NonNull List<ColumnsMetaData> columnsMetas, DataBaseType dataBaseType) {
        return columnsMetas.stream().map(ColumnsMetaData::getColumnName).map(column -> escape(column, dataBaseType))
                           .collect(Collectors.joining(DELIMITER));
    }

    private SqlGenerate getSqlGenerate(DataBaseType dataBaseType) {
        return SQL_GENERATE.get(dataBaseType);
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
         * @param template        SQL template
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
