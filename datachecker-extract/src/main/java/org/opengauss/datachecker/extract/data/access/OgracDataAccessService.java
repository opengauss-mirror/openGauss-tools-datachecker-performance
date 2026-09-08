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

package org.opengauss.datachecker.extract.data.access;

import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.common.PointPair;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.PrimaryColumnBean;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.UniqueColumnBean;
import org.opengauss.datachecker.extract.data.mapper.OgracMetaDataMapper;

import java.sql.Connection;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.ConcurrentHashMap;

/**
 * Metadata and data access service for the oGRAC source database.
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
public class OgracDataAccessService extends AbstractDataAccessService {
    private OgracMetaDataMapper ogracMetaDataMapper;

    /**
     * Filtering oGRAC ADM_TAB_COLUMNS per table takes seconds per table (unusable for thousand-table schemas),
     * while one whole-schema query costs only milliseconds. So the first call loads and caches the whole schema,
     * and later calls filter by table name and return a copy.
     */
    private final Map<String, Map<String, List<ColumnsMetaData>>> schemaColumnsCache = new ConcurrentHashMap<>();

    public OgracDataAccessService(OgracMetaDataMapper ogracMetaDataMapper) {
        this.ogracMetaDataMapper = ogracMetaDataMapper;
    }

    @Override
    public String sqlMode() {
        return null;
    }

    @Override
    public Health health() {
        String schema = properties.getSchema();
        String sql = "SELECT OWNER tableSchema FROM ADM_TABLES WHERE OWNER = :schema LIMIT 1";
        return health(schema, sql);
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    @Override
    public List<String> dasQueryTableNameList() {
        String sql = "SELECT TABLE_NAME tableName FROM ADM_TABLES WHERE OWNER = :schema";
        return adasQueryTableNameList(sql, Map.of("schema", properties.getSchema()));
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        Map<String, List<ColumnsMetaData>> columnsByTable =
            schemaColumnsCache.computeIfAbsent(properties.getSchema(), this::loadSchemaColumns);
        List<ColumnsMetaData> columns = columnsByTable.get(tableName);
        if (columns == null) {
            // Case-insensitive fallback: table names in ADM_TAB_COLUMNS and ADM_TABLES may differ in case
            columns = columnsByTable.entrySet().stream()
                .filter(entry -> entry.getKey().equalsIgnoreCase(tableName))
                .map(Map.Entry::getValue).findFirst().orElse(null);
        }
        if (columns == null) {
            return new ArrayList<>();
        }
        // Return a copy so caller modifications to column objects never pollute the cache
        List<ColumnsMetaData> result = new ArrayList<>(columns.size());
        columns.forEach(column -> result.add(copyColumn(column)));
        return result;
    }

    private Map<String, List<ColumnsMetaData>> loadSchemaColumns(String schema) {
        List<ColumnsMetaData> allColumns = ogracMetaDataMapper.querySchemaColumnsMetaData(schema);
        // oGRAC quirk: OrdinalPosition needs +1 to align with other databases
        allColumns.forEach(column -> column.setOrdinalPosition(column.getOrdinalPosition() + 1));
        Map<String, List<ColumnsMetaData>> columnsByTable = new ConcurrentHashMap<>();
        allColumns.forEach(column -> columnsByTable
            .computeIfAbsent(column.getTableName(), key -> new ArrayList<>()).add(column));
        return columnsByTable;
    }

    private static ColumnsMetaData copyColumn(ColumnsMetaData source) {
        ColumnsMetaData target = new ColumnsMetaData();
        target.setSchema(source.getSchema());
        target.setTableName(source.getTableName());
        target.setColumnName(source.getColumnName());
        target.setColumnType(source.getColumnType());
        target.setDataType(source.getDataType());
        target.setOrdinalPosition(source.getOrdinalPosition());
        return target;
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        return wrapperTableMetadata(ogracMetaDataMapper.queryTableMetadata(properties.getSchema(), tableName));
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns() {
        String sql = "SELECT TABLE_NAME tableName, COLUMNS columnName FROM ADM_INDEXES WHERE IS_PRIMARY = 'Y' "
            + "AND OWNER = :schema";
        List<PrimaryColumnBean> rawList = adasQueryTablePrimaryColumns(sql, Map.of("schema", properties.getSchema()));
        List<PrimaryColumnBean> result = new ArrayList<>();
        for (PrimaryColumnBean bean : rawList) {
            if (bean.getColumnName() != null) {
                String[] columns = bean.getColumnName().split(",");
                for (String column : columns) {
                    result.add(new PrimaryColumnBean(bean.getTableName(), column.trim()));
                }
            }
        }
        return result;
    }

    @Override
    public List<PrimaryColumnBean> queryTableUniqueColumns(String tableName) {
        String sql = "SELECT INDEX_NAME indexIdentifier, OWNER, TABLE_NAME tableName, COLUMNS columnName, 1 colIdx "
            + "FROM ADM_INDEXES WHERE IS_UNIQUE='Y' AND OWNER=:schema AND TABLE_NAME=:tableName";
        List<UniqueColumnBean> uniqueColumns = adasQueryTableUniqueColumns(sql,
            Map.of("schema", properties.getSchema(), "tableName", tableName));
        List<PrimaryColumnBean> result = new ArrayList<>();
        for (UniqueColumnBean bean : uniqueColumns) {
            if (bean.getColumnName() != null) {
                String[] columns = bean.getColumnName().split(",");
                for (String column : columns) {
                    result.add(new PrimaryColumnBean(bean.getTableName(), column.trim()));
                }
            }
        }
        return result;
    }

    @Override
    public List<PrimaryColumnBean> queryTablePrimaryColumns(String tableName) {
        List<PrimaryColumnBean> rawList =
            ogracMetaDataMapper.queryTablePrimaryColumnsByTableName(properties.getSchema(), tableName);
        List<PrimaryColumnBean> result = new ArrayList<>();
        for (PrimaryColumnBean bean : rawList) {
            if (bean.getColumnName() != null) {
                String[] columns = bean.getColumnName().split(",");
                for (String column : columns) {
                    result.add(new PrimaryColumnBean(bean.getTableName(), column.trim()));
                }
            }
        }
        return result;
    }

    @Override
    public List<TableMetadata> dasQueryTableMetadataList() {
        LowerCaseTableNames lowerCaseTableNames = getLowerCaseTableNames();
        String colTableName = Objects.equals(LowerCaseTableNames.SENSITIVE, lowerCaseTableNames)
            ? "t.table_name tableName"
            : "lower(t.table_name) tableName";
        String sql = "SELECT t.owner tableSchema," + colTableName
            + ",nvl(t.num_rows, 0) tableRows,nvl(t.avg_row_len, 0) avgRowLength"
            + " FROM ADM_TABLES t WHERE t.OWNER = :schema";
        return wrapperTableMetadata(adasQueryTableMetadataList(sql, Map.of("schema", properties.getSchema())));
    }

    @Override
    public long rowCount(String tableName) {
        return ogracMetaDataMapper.rowCount(properties.getSchema(), tableName);
    }

    @Override
    public boolean tableExistsRows(String tableName) {
        return ogracMetaDataMapper.tableExistsRows(properties.getSchema(), tableName);
    }

    @Override
    public String min(Connection connection, DataAccessParam param) {
        String sql = " select min(" + param.getColName() + ") from " + param.getSchema() + "." + param.getName();
        return adasQueryOnePoint(connection, sql);
    }

    @Override
    public String max(Connection connection, DataAccessParam param) {
        String sql = " select max(" + param.getColName() + ") from " + param.getSchema() + "." + param.getName();
        return adasQueryOnePoint(connection, sql);
    }

    @Override
    public String next(DataAccessParam param) {
        return ogracMetaDataMapper.next(param);
    }

    @Override
    public List<Object> queryPointList(Connection connection, DataAccessParam param) {
        String sql = "select s.%s from (select row_number() over(order by r.%s asc) as rn,r.%s from %s.%s r) s"
            + " where mod(s.rn, %s) = 1";
        sql = String.format(sql, param.getColName(), param.getColName(), param.getColName(),
                param.getSchema(), param.getName(), param.getOffset());
        return adasQueryPointList(connection, sql);
    }

    @Override
    public List<PointPair> queryUnionFirstPrimaryCheckPointList(Connection connection, DataAccessParam param) {
        String sqlTmp = "select %s,count(1) from %s.%s group by %s";
        String sql = String.format(sqlTmp, param.getColName(), param.getSchema(), param.getName(), param.getColName());
        return adasQueryUnionPointList(connection, sql);
    }

    @Override
    public long queryUnionColumnCardinality(Connection connection, DataAccessParam param) {
        String sqlTmp = "select count(distinct %s) from %s.%s";
        String sql = String.format(sqlTmp, param.getColName(), param.getSchema(), param.getName());
        return adasQueryCardinality(connection, sql);
    }

    @Override
    public boolean dasCheckDatabaseNotEmpty() {
        return ogracMetaDataMapper.checkDatabaseNotEmpty(properties.getSchema());
    }

    @Override
    public LowerCaseTableNames queryLowerCaseTableNames() {
        return LowerCaseTableNames.SENSITIVE;
    }
}