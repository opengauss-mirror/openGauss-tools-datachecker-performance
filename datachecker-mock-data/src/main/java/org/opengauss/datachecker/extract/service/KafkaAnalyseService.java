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
import org.opengauss.datachecker.common.constant.Constants.InitialCapacity;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.jdbc.core.JdbcTemplate;
import org.springframework.jdbc.core.namedparam.NamedParameterJdbcTemplate;
import org.springframework.scheduling.concurrent.ThreadPoolTaskExecutor;
import org.springframework.stereotype.Service;

import java.sql.ResultSetMetaData;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
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
public class KafkaAnalyseService {
    @Autowired
    private KafkaProperties properties;
    @Autowired
    private JdbcTemplate jdbcTemplateMysql;
    @Autowired
    private ThreadPoolTaskExecutor threadPoolTaskExecutor;

    /**
     * checkKafkaAnalyse
     *
     * @param tableName tableName
     */
    public void checkKafkaAnalyse(String tableName) {
        threadPoolTaskExecutor.submit(new DataAnalyseRunnable(tableName, jdbcTemplateMysql));
    }

    class DataAnalyseRunnable extends KafkaService implements Runnable {
        private String tableName;
        private String topicName = "quickSendKafkaTopic";
        private JdbcTemplate jdbcTemplate;

        /**
         * DataAnalyseRunnable
         *
         * @param tableName    tableName
         * @param jdbcTemplate jdbcTemplate
         */
        public DataAnalyseRunnable(String tableName, JdbcTemplate jdbcTemplate) {
            super(properties);
            this.tableName = tableName;
            this.jdbcTemplate = jdbcTemplate;
        }

        @Override
        public void run() {
            initAdminClient();
            deleteTopic(topicName);
            log.info("delete topic {}", topicName);
            final String execSql = TableSqlWapper.SELECT_M.replace(":table", tableName);
            List<Map<String, String>> dataRowList = queryColumnValues(execSql);
            log.info("queryColumnValues {} :count:{}", tableName, dataRowList.size());
            log.info("message length={}", JsonObjectUtil.format(dataRowList.get(0)).length());
            // Push the data to Kafka according to the fragmentation order
            sendMessage(topicName, dataRowList);
        }

        private void sendMessage(String topicName, List<Map<String, String>> dataRowList) {
            log.info("sendMessage {} start", topicName);
            KafkaProducer<String, String> kafkaProducer = buildKafkaProducer();
            AtomicInteger cnt = new AtomicInteger(0);
            dataRowList.forEach(record -> {
                final String message = JsonObjectUtil.format(record);
                ProducerRecord<String, String> producerRecord =
                    new ProducerRecord<>(topicName, 0, record.get("b_number"), message);
                sendMessage(kafkaProducer, producerRecord, cnt);
            });
            kafkaProducer.flush();
            log.info("sendMessage {} end", topicName);
        }

        private List<Map<String, String>> queryColumnValues(String sql) {
            Map<String, Object> map = new HashMap<>(InitialCapacity.CAPACITY_16);
            NamedParameterJdbcTemplate jdbc = new NamedParameterJdbcTemplate(jdbcTemplate);
            return jdbc.query(sql, map, (rs, rowNum) -> {
                ResultSetMetaData metaData = rs.getMetaData();
                ResultSetHandler handler = new ResultSetHandler();
                return handler.putOneResultSetToMap(rs, metaData);
            });
        }
    }
}
