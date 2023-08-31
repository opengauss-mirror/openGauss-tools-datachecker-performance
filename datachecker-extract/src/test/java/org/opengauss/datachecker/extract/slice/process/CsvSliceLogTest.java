package org.opengauss.datachecker.extract.slice.process;

import com.alibaba.fastjson.JSONObject;
import com.opencsv.CSVReader;
import com.opencsv.exceptions.CsvValidationException;
import lombok.Data;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.entry.csv.CsvTableMeta;
import org.opengauss.datachecker.common.entry.csv.SliceIndexVo;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.SliceIndexStatus;
import org.opengauss.datachecker.common.entry.enums.SliceLogType;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.FileUtils;
import org.springframework.util.Assert;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileReader;
import java.io.IOException;
import java.nio.file.Files;
import java.nio.file.Path;
import java.time.LocalDateTime;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicInteger;

@ExtendWith(MockitoExtension.class)
class CsvSliceLogTest {
    private static final int default_column_idx = 0;
    private final List<CsvTableMeta> csvTableMetaList = new ArrayList<>();
    private final List<SliceTable> sliceTableList = new ArrayList<>();
    private final Map<String, List<Path>> csvTablePaths = new HashMap<>();
    private final String schema_columns = "_information_schema_columns.csv";
    private final String schema_tables = "_information_schema_tables.csv";

    private static Path dirPath;
    private static String schema;

    @BeforeEach
    void setUp() throws IOException {
        schema = "";
        dirPath = Path.of("");
        initLogs();

        parseCsvTableMeta(dirPath, schema);

        for (CsvTableMeta table : csvTableMetaList) {
            if (table.isContain_primary_key()) {
                SliceTable sliceTable = new SliceTable();
                sliceTable.setSchema(table.getSchema());
                sliceTable.setTable(table.getTable());
                sliceTable.setEndpoint(Endpoint.SOURCE);
                sliceTableList.add(sliceTable);
            }
        }
        scanCsvFile(dirPath);
    }

    private static void initLogs() throws IOException {
        Path readerLogsDir = Path.of(dirPath.toString(), "logs");
        Files.createDirectories(readerLogsDir);
    }

    private void parseCsvTableMeta(Path dirPath, String schema) {
        String tablesFile = schema + schema_tables;
        Path sliceFileFullPath = Path.of(dirPath.toString(), tablesFile);
        try (BufferedReader reader = Files.newBufferedReader(sliceFileFullPath)) {
            String nextLine;
            while ((nextLine = reader.readLine()) != null) {
                CsvTableMeta csvTableMeta = JSONObject.parseObject(nextLine, CsvTableMeta.class);
                csvTableMetaList.add(csvTableMeta);
            }
        } catch (IOException e) {
            e.printStackTrace();
        }
    }


    private void scanCsvFile(Path dirPath) {
        if (Files.isDirectory(dirPath)) {
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
                    sliceTableList.forEach(table -> {
                        if (fileName.startsWith(table.getTableFilePrefix())) {
                            csvTablePaths.compute(table.getTable(), (key, value) -> {
                                if (value == null) {
                                    value = new ArrayList<>();
                                }
                                value.add(Path.of(fileName));
                                return value;
                            });
                        }
                    });
                }
            });
        }
    }

    @Test
    void testSourceRun() throws IOException {
        Path readerLog = Path.of(dirPath.toString(), "logs", "reader.log");
        if(Files.exists(readerLog)){
            Files.delete(readerLog);
        }
        Files.createFile(readerLog);
        for (SliceTable sliceTable : sliceTableList) {
            List<Path> tablePaths = csvTablePaths.get(sliceTable.getTable());
            if (Objects.isNull(tablePaths)) {
                continue;
            }
            AtomicInteger sliceNo = new AtomicInteger(1);
            tablePaths.stream().sorted(pathComparator()).forEach(sliceFile -> {
                SliceVo sliceVo = new SliceVo();
                sliceVo.setPtn(0);
                sliceVo.setName(sliceFile.toString());
                sliceVo.setTable(sliceTable.getTable());
                sliceVo.setSchema(sliceTable.getSchema());
                sliceVo.setNo(sliceNo.getAndIncrement());
                sliceVo.setPtnNum(1);
                sliceVo.setType(SliceLogType.SLICE);
                sliceVo.setTotal(tablePaths.size());
                sliceVo.setTableHash(sliceTable.getTable().hashCode());
                SliceFileData file = parseFileData(sliceFile);
                sliceVo.setFetchSize(file.getCount());
                sliceVo.setBeginIdx(file.getSeqStart());
                sliceVo.setEndIdx(file.getSeqEnd());
                FileUtils.writeAppendFile(readerLog.toString(), JSONObject.toJSONString(sliceVo) + System.lineSeparator());
            });
        }

    }

    private Comparator<Path> pathComparator() {
        return (o1, o2) -> {
            String[] o1Idx = o1.toString().replace(".csv", "").split("slice");
            String[] o2Idx = o2.toString().replace(".csv", "").split("slice");
            return Integer.parseInt(o1Idx[1]) - Integer.parseInt(o2Idx[1]);
        };
    }

    @Test
    void testSinkRun() throws IOException {
        Path writerLog = Path.of(dirPath.toString(), "logs", "writer.log");
        if(Files.exists(writerLog)){
            Files.delete(writerLog);
        }
        Files.createFile(writerLog);
        for (SliceTable sliceTable : sliceTableList) {
            List<Path> tablePaths = csvTablePaths.get(sliceTable.getTable());
            if (Objects.isNull(tablePaths)) {
                continue;
            }
            SliceIndexVo sIndex = new SliceIndexVo();
            sIndex.setSchema(sliceTable.getSchema());
            sIndex.setTable(sliceTable.getTable());
            sIndex.setContainsIndex(true);
            sIndex.setTimestamp(LocalDateTime.now());
            sIndex.setType(SliceLogType.INDEX);
            sIndex.setIndexStatus(SliceIndexStatus.START);
            FileUtils.writeAppendFile(writerLog.toString(), JSONObject.toJSONString(sIndex) + System.lineSeparator());

            AtomicInteger sliceNo = new AtomicInteger(1);
            tablePaths.stream().sorted(pathComparator()).forEach(sliceFile -> {
                SliceVo sliceVo = new SliceVo();
                sliceVo.setPtn(0);
                sliceVo.setName(sliceFile.toString());
                sliceVo.setTable(sliceTable.getTable());
                sliceVo.setSchema(sliceTable.getSchema());
                sliceVo.setNo(sliceNo.getAndIncrement());
                sliceVo.setPtnNum(1);
                sliceVo.setType(SliceLogType.SLICE);
                sliceVo.setTotal(tablePaths.size());
                sliceVo.setTableHash(sliceTable.getTable().hashCode());
                SliceFileData file = parseFileData(sliceFile);
                sliceVo.setFetchSize(file.getCount());
                sliceVo.setBeginIdx(file.getSeqStart());
                sliceVo.setEndIdx(file.getSeqEnd());
                FileUtils.writeAppendFile(writerLog.toString(), JSONObject.toJSONString(sliceVo) + System.lineSeparator());
            });

            sIndex.setIndexStatus(SliceIndexStatus.END);
            sIndex.setTimestamp(LocalDateTime.now());
            FileUtils.writeAppendFile(writerLog.toString(), JSONObject.toJSONString(sIndex) + System.lineSeparator());
        }

    }

    private SliceFileData parseFileData(Path sliceFile) {
        SliceFileData sliceFileData = new SliceFileData();
        Path sliceFileFullPath = Path.of(dirPath.toString(), sliceFile.toString());
        try (CSVReader reader = new CSVReader(new FileReader(sliceFileFullPath.toString()))) {
            String[] nextLine;
            int rowCount = 0;
            while ((nextLine = reader.readNext()) != null) {
                if (rowCount == 0) {
                    sliceFileData.setSeqStart(nextLine[default_column_idx]);
                } else {
                    sliceFileData.setSeqEnd(nextLine[default_column_idx]);
                }
                rowCount++;
            }
            sliceFileData.setCount(rowCount);
        } catch (CsvValidationException | IOException e) {
            e.printStackTrace();
        }

        return sliceFileData;
    }

    @Data
    static class SliceTable {
        private String schema;
        private String table;
        private Endpoint endpoint;

        public String getTableFilePrefix() {
            return schema + "_" + table + "_slice";
        }
    }

    @Data
    static class SliceFileData {
        private String seqStart;
        private String seqEnd;
        private int count;
    }
}
