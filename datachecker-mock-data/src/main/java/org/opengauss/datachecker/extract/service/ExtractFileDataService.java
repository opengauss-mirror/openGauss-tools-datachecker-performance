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

package org.opengauss.datachecker.extract.service;

import com.alibaba.fastjson.JSONObject;
import lombok.SneakyThrows;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.util.FileUtils;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Service;

import java.nio.file.Files;
import java.nio.file.Path;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.function.Function;
import java.util.stream.Collectors;

/**
 * ExtractFileDataService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/26
 * @since ：11
 */
@Slf4j
@Service
public class ExtractFileDataService {
    @Value("${spring.mock.data-path}")
    private String path;

    /**
     * checkFileData
     *
     * @param fileSource fileSource
     * @param fileSink   fileSource
     */
    public void checkFileData(String fileSource, String fileSink) {
        checkPathFileData(Path.of(path + fileSource), Path.of(path + fileSink));
    }

    /**
     * checkPathFileData
     *
     * @param fileSource fileSource
     * @param fileSink   fileSource
     */
    public void checkPathFileData(Path fileSource, Path fileSink) {
        final List<RowDataHash> sourceDataList = readAndParseFile(fileSource);
        log.info("read and parse file {} record size={}", fileSource.getFileName(), sourceDataList.size());
        final List<RowDataHash> sinkDataList = readAndParseFile(fileSink);
        log.info("read and parse file {} record size={}", fileSink.getFileName(), sinkDataList.size());
        final Map<String, RowDataHash> sourceMap = sourceDataList.parallelStream().collect(
            Collectors.toConcurrentMap(RowDataHash::getPrimaryKey, Function.identity()));
        log.info("transform sourceDataList to map {} ", sourceMap.size());
        final Map<String, RowDataHash> sinkMap = sinkDataList.parallelStream().collect(
            Collectors.toConcurrentMap(RowDataHash::getPrimaryKey, Function.identity()));
        log.info("transform sinkDataList to map {} ", sinkMap.size());
        Map<RowDataHash, RowDataHash> sourceDiffMap = new HashMap<>(InitialCapacity.CAPACITY_1);
        sourceMap.forEach((key, value) -> {
            if (sinkMap.containsKey(key)) {
                RowDataHash sinkValue = sinkMap.get(key);
                if (value.getPrimaryKeyHash() != sinkValue.getPrimaryKeyHash()) {
                    sourceDiffMap.put(value, sinkValue);
                }
            }
        });
        log.info("compare sourceMap and sinkMap result {} ", sourceDiffMap.size());
        String sourceReduceFile = path + "sourceReduce.json";
        String sinkReduceFile = path + "sinkReduce.json";
        FileUtils.deleteFile(sourceReduceFile);
        FileUtils.deleteFile(sinkReduceFile);
        FileUtils.writeAppendFile(sourceReduceFile, JsonObjectUtil.format(sourceDiffMap));
        log.info("data export file  name {} , {} ", sourceReduceFile, sinkReduceFile);
    }

    @SneakyThrows
    private List<RowDataHash> readAndParseFile(Path pathFileName) {
        final List<String> stringList = Files.readAllLines(pathFileName);
        StringBuffer buffer = new StringBuffer();
        stringList.forEach(line -> {
            buffer.append(line);
        });
        return JSONObject.parseArray(buffer.toString(), RowDataHash.class);
    }
}
