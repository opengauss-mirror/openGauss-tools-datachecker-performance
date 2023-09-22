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

package org.opengauss.datachecker.extract.slice.common;

import lombok.Data;
import org.apache.commons.collections4.CollectionUtils;
import org.springframework.util.Assert;

import java.io.File;
import java.nio.file.Files;
import java.nio.file.Path;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.LinkedList;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

/**
 * CsvDataFileScanner
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public class CsvDataFileScanner {
    private final List<CsvTable> csvTableList = new ArrayList<>();
    private final Map<String, List<Path>> csvTablePaths = new HashMap<>();
    private final String schema_columns = "_information_schema_columns.csv";
    private final String schema_tables = "_information_schema_tables.csv";
    private final String schema;

    public CsvDataFileScanner(String schema, List<String> tableList) {
        this.schema = schema;
        this.csvTableList.addAll(buildCsvTables(schema, tableList));
    }

    private List<CsvTable> buildCsvTables(String schema, List<String> tableList) {
        return tableList.stream().map(table -> new CsvTable(schema, table)).collect(Collectors.toList());
    }

    public void scanCsvFile(Path dirPath) {
        if (!Files.isDirectory(dirPath)) {
            return;
        }
        File dir = dirPath.toFile();
        String[] fileNameList = dir.list();
        Assert.notNull(fileNameList, "chameleon dir does not have any files");
        Arrays.stream(fileNameList).forEach(fileName -> {
            if (fileName.equals(schema + schema_columns) || fileName.equals(schema + schema_tables)) {
                csvTablePaths.compute("metadata", (key, value) -> {
                    if (value == null) {
                        value = new ArrayList<>();
                    }
                    value.add(Path.of(fileName));
                    return value;
                });
            } else {
                csvTableList.forEach(table -> {
                    if (fileName.startsWith(table.getTableFilePrefix())) {
                        csvTablePaths.compute(table.getTable(), (key, value) -> {
                            if (value == null) {
                                value = new LinkedList<>();
                            }
                            value.add(Path.of(fileName));
                            return value;
                        });
                    }
                });
            }
        });
        csvTablePaths.forEach((key, paths) -> {
            Collections.sort(paths, pathComparator());
        });
    }

    public List<Path> getTablePaths(String table) {
        List<Path> paths = csvTablePaths.get(table);
        if (CollectionUtils.isNotEmpty(paths)) {
            paths.sort(pathComparator());
        }
        return paths;
    }

    private Comparator<Path> pathComparator() {
        return (o1, o2) -> {
            String[] o1Idx = o1.toString().replace(".csv", "").split("slice");
            String[] o2Idx = o2.toString().replace(".csv", "").split("slice");
            if (o1Idx.length == 2 && o2Idx.length == 2) {
                return Integer.parseInt(o1Idx[1]) - Integer.parseInt(o2Idx[1]);
            } else {
                return o1.compareTo(o2);
            }
        };
    }

    @Data
    static class CsvTable {
        private String schema;
        private String table;

        public CsvTable(String schema, String table) {
            this.schema = schema;
            this.table = table;
        }
        //        private Endpoint endpoint;

        public String getTableFilePrefix() {
            return schema + "_" + table + "_slice";
        }
    }
}
