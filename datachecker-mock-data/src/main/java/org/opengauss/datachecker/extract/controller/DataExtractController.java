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

package org.opengauss.datachecker.extract.controller;

import org.opengauss.datachecker.extract.service.ExtractFileDataService;
import org.opengauss.datachecker.extract.service.ExtractKafkaDataService;
import org.opengauss.datachecker.extract.service.ExtractTableDataAnalyseService;
import org.opengauss.datachecker.extract.service.ExtractTableDataService;
import org.opengauss.datachecker.extract.service.KafkaAnalyseService;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import java.util.List;

/**
 * DataExtractController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@RestController
public class DataExtractController {
    @Autowired
    private ExtractKafkaDataService extractKafkaDataService;
    @Autowired
    private ExtractFileDataService extractFileDataService;
    @Autowired
    private ExtractTableDataService extractTableDataService;
    @Autowired
    private ExtractTableDataAnalyseService extractTableDataAnalyseService;
    @Autowired
    private KafkaAnalyseService kafkaAnalyseService;

    /**
     * checkKafkaTopicData
     *
     * @param topicSource topicSource
     * @param topicSink   topicSink
     * @return check kafka topic name
     */
    @GetMapping("/check/kafka/topic/data")
    public List<String> checkKafkaTopicData(@RequestParam("topicSource") String topicSource,
        @RequestParam("topicSink") String topicSink) {
        return extractKafkaDataService.checkKafkaTopicData(topicSource, topicSink);
    }

    /**
     * checkFileData
     *
     * @param fileSource fileSource
     * @param fileSink   fileSink
     */
    @GetMapping("/check/file/data")
    public void checkFileData(@RequestParam("fileSource") String fileSource,
        @RequestParam("fileSink") String fileSink) {
        extractFileDataService.checkFileData(fileSource, fileSink);
    }

    /**
     * checkTableData
     *
     * @param tableName tableName
     * @return result
     */
    @GetMapping("/check/table/data")
    public String checkTableData(@RequestParam("tableName") String tableName) {
        return "OK : diffCnt=" + extractTableDataService.checkTable(tableName);
    }

    /**
     * checkTableDataAnalyse
     *
     * @param tableName tableName
     * @return result
     */
    @GetMapping("/check/table/data/analyse")
    public String checkTableDataAnalyse(@RequestParam("tableName") String tableName) {
        extractTableDataAnalyseService.checkTable(tableName);
        return "OK";
    }

    /**
     * checkTableDataAnalyse
     *
     * @param tableName tableName
     * @return result
     */
    @GetMapping("/check/table/data/kafka/analyse")
    public String checkTableDataKafkaAnalyse(@RequestParam("tableName") String tableName) {
        kafkaAnalyseService.checkKafkaAnalyse(tableName);
        return "OK";
    }
}
