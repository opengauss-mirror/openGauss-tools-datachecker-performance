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

import lombok.extern.slf4j.Slf4j;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.concurrent.atomic.AtomicInteger;

/**
 * ExtractTableDataAnalyseService
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/26
 * @since ：11
 */
@Slf4j
@Service
public class ExtractTableDataAnalyseService {
    @Autowired
    private KafkaProperties properties;
    @Autowired
    private JdbcTemplate jdbcTemplateMysql;
    @Autowired
    private JdbcTemplate jdbcTemplateOpenGauss;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * checkTable
     *
     * @param tableName tableName
     */
    public void checkTable(String tableName) {
        String topicName = "quickstart";
        String topicName2 = "quickstart2";
        threadPoolTaskExecutor.submit(
            new DataAnalyseRunnable(tableName, "mysql", TableSqlWapper.SELECT_PRI_M, jdbcTemplateMysql, topicName));
        threadPoolTaskExecutor.submit(
            new DataAnalyseRunnable(tableName, "openGauss", TableSqlWapper.SELECT_PRI_O, jdbcTemplateOpenGauss,
                topicName2));
    }

    class DataAnalyseRunnable extends KafkaService implements Runnable {
        private String tableName;
        private String database;
        private String execSql;
        private String topicName;
        private JdbcTemplate jdbcTemplate;

        /**
         * DataAnalyseRunnable
         *
         * @param tableName    tableName
         * @param database     database
         * @param execSql      execSql
         * @param jdbcTemplate jdbcTemplate
         * @param topicName    topicName
         */
        public DataAnalyseRunnable(String tableName, String database, String execSql, JdbcTemplate jdbcTemplate,
            String topicName) {
            super(properties);
            this.tableName = tableName;
            this.database = database;
            this.execSql = execSql;
            this.jdbcTemplate = jdbcTemplate;
            this.topicName = topicName;
        }

        @Override
        public void run() {
            QueryDataWapper queryDataWapper = new QueryDataWapper();
            final List<String> primaryList = queryDataWapper.queryPrimaryValues(jdbcTemplate, execSql, tableName);
            log.info("query  {} : table={}, row-size={} ", database, tableName, primaryList.size());
            HashHandler hashHandler = new HashHandler();
            List<Long> hashList = new ArrayList<>();
            primaryList.forEach(primaryKey -> {
                hashList.add(hashHandler.xx3Hash(primaryKey));
            });
            final HashSet<Long> hashSet = new HashSet<>(hashList);
            log.info("{} row  hash list -> {} , set->{}", database, hashList.size(), hashSet.size());
            List<Long> hashMode0List = new ArrayList<>();
            List<Long> hashMode1List = new ArrayList<>();
            int partition = 2;
            hashList.forEach(hash -> {
                final int absMod = (int) Math.abs(hash % partition);
                if (absMod <= 0) {
                    hashMode0List.add(hash);
                } else {
                    hashMode1List.add(hash);
                }
            });
            log.info("{} row  hash partition 0 -> {} , 1->{}", database, hashMode0List.size(), hashMode1List.size());
            sendMessage(topicName, partition, hashList);
        }

        private void sendMessage(String topicName, int partition, List<Long> hashList) {
            KafkaProducer<String, String> kafkaProducer = buildKafkaProducer();
            AtomicInteger cnt = new AtomicInteger(0);
            hashList.forEach(record -> {
                final int absMod = (int) Math.abs(record % partition);
                ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, absMod, String.valueOf(record), String.valueOf(record));
                sendMessage(kafkaProducer, producerRecord, cnt);
            });
            kafkaProducer.flush();
        }
    }
}
