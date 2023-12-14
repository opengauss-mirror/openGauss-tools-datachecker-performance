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

package org.opengauss.datachecker.extract.data.access;

import com.alibaba.fastjson.JSONObject;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.common.DataAccessParam;
import org.opengauss.datachecker.common.entry.csv.CsvTableColumnMeta;
import org.opengauss.datachecker.common.entry.csv.CsvTableMeta;
import org.opengauss.datachecker.common.entry.enums.ColumnKey;
import org.opengauss.datachecker.common.entry.extract.ColumnsMetaData;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.CsvDataAccessException;
import org.opengauss.datachecker.common.exception.ExtractDataAccessException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.jdbc.core.RowMapper;

import javax.sql.DataSource;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.TreeMap;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/**
 * CsvDataAccessService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/10
 * @since ：11
 */
public class CsvDataAccessService implements DataAccessService {
    private static final Logger log = LogUtils.getLogger();
    private Map<String, TableMetadata> tableMetadataMap = new ConcurrentHashMap<>();

    @Override
    public boolean health() {
        return true;
    }

    @Override
    public List<String> queryTableNameList() {
        Path pathOfTables = ConfigCache.getCsvMetadataTablesPath();
        try {
            if (Files.notExists(pathOfTables)) {
                log.error("csv metadata info does not exist {}", pathOfTables);
                throw new CsvDataAccessException("csv metadata load failed");
            }
            Stream<String> lineOfTables = Files.lines(pathOfTables);
            return lineOfTables.parallel().map(tableJson -> JSONObject.parseObject(tableJson, CsvTableMeta.class))
                               .filter(CsvTableMeta::isContain_primary_key).map(CsvTableMeta::getTable)
                               .collect(Collectors.toList());

        } catch (IOException e) {
            log.error("load table name of csv exception : ", e);
            throw new ExtractDataAccessException("load table name of csv exception");
        }
    }

    @Override
    public List<TableMetadata> queryTableMetadataList() {
        tableMetadataMap.clear();
        Path pathOfTables = ConfigCache.getCsvMetadataTablesPath();
        Path pathOfColumns = ConfigCache.getCsvMetadataColumnsPath();
        if (Files.notExists(pathOfTables) || Files.notExists(pathOfColumns)) {
            log.error("csv metadata info does not exist {} or {}", pathOfTables, pathOfColumns);
            throw new CsvDataAccessException("csv metadata load failed");
        }
        try {
            Stream<String> lineOfTables = Files.lines(pathOfTables);
            lineOfTables.forEach(tableJson -> {
                CsvTableMeta csvTableMeta = JSONObject.parseObject(tableJson, CsvTableMeta.class);
                tableMetadataMap.put(csvTableMeta.getTable(), csvTableMeta.toTableMetadata());
            });

            Stream<String> lineOfColumns = Files.lines(pathOfColumns);
            List<ColumnsMetaData> columns = new LinkedList<>();
            lineOfColumns.forEach(columnJson -> {
                CsvTableColumnMeta csvColumnMeta = JSONObject.parseObject(columnJson, CsvTableColumnMeta.class);
                columns.add(csvColumnMeta.toColumnsMetaData());
            });
            columns.stream().sorted().collect(Collectors.groupingBy(ColumnsMetaData::getTableName))
                   .forEach((table, tableColumns) -> {
                       TableMetadata tableMetadata = tableMetadataMap.get(table);
                       tableMetadata.setColumnsMetas(tableColumns);
                       tableMetadata.setPrimaryMetas(
                           tableColumns.stream().filter(col -> Objects.equals(col.getColumnKey(), ColumnKey.PRI))
                                       .sorted().collect(Collectors.toList()));
                   });
        } catch (IOException e) {
            log.error("load table name of csv exception : ", e);
            throw new ExtractDataAccessException("load table name of csv exception");
        }
        return new ArrayList<>(tableMetadataMap.values());
    }

    @Override
    public List<ColumnsMetaData> queryTableColumnsMetaData(String tableName) {
        TableMetadata tableMetadata = queryTableMetadata(tableName);
        Objects.requireNonNull(tableMetadata, tableName + " metadata not found");
        return tableMetadata.getColumnsMetas();
    }

    @Override
    public TableMetadata queryTableMetadata(String tableName) {
        if (tableMetadataMap.isEmpty()) {
            queryTableMetadataList();
        }
        if (tableMetadataMap.containsKey(tableName)) {
            return tableMetadataMap.get(tableName);
        }
        return null;
    }

    @Override
    public boolean isOgCompatibilityB() {
        return false;
    }

    /**
     * csv does not use it
     *
     * @param sql       sql
     * @param param     sql param
     * @param rowMapper row mapper
     * @param <T>
     * @return
     */
    @Override
    public <T> List<T> query(String sql, Map<String, Object> param, RowMapper<T> rowMapper) {
        return null;
    }

    @Override
    public List<Map<String, String>> query(String table, String fileName, List<Difference> differenceList) {
        try {
            if (!tableMetadataMap.containsKey(table)) {
                return new LinkedList<>();
            }
            List<Map<String, String>> diffRowList = new LinkedList<>();
            String csvDataRootPath = ConfigCache.getCsvData();
            String sliceFilePath = Path.of(csvDataRootPath, fileName).toString();
            TableMetadata metadata = tableMetadataMap.get(table);
            List<Integer> keyIdxList =
                differenceList.stream().map(Difference::getIdx).sorted().collect(Collectors.toList());
            int fileReadIdx = 0;
            try (CSVReader reader = new CSVReader(new FileReader(sliceFilePath))) {
                String[] nextLine;
                while ((nextLine = reader.readNext()) != null) {
                    fileReadIdx++;
                    if (keyIdxList.contains(fileReadIdx)) {
                        diffRowList.add(parse(nextLine, metadata.getColumnsMetas()));
                    }
                }
            }
            return diffRowList;
        } catch (CsvValidationException | IOException ex) {
            throw new ExtractDataAccessException();
        }
    }

    private Map<String, String> parse(String[] nextLine, List<ColumnsMetaData> columns) {
        Map<String, String> result = new TreeMap<>();
        for (int idx = 0; idx < nextLine.length && idx < columns.size(); idx++) {
            result.put(columns.get(idx).getColumnName(), nextLine[idx]);
        }
        return result;
    }

    @Override
    public DataSource getDataSource() {
        return null;
    }

    @Override
    public List<Object> queryPointList(DataAccessParam param) {
        return null;
    }

    @Override
    public long rowCount(String tableName) {
        return 0;
    }

    @Override
    public String min(DataAccessParam param) {
        return null;
    }

    @Override
    public String max(DataAccessParam param) {
        return null;
    }

    @Override
    public String next(DataAccessParam param) {
        return null;
    }
}
