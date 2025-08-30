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

package org.opengauss.datachecker.extract.debezium;

import com.alibaba.fastjson.JSONException;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.exception.DebeziumConfigException;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.springframework.stereotype.Service;

import jakarta.annotation.PostConstruct;
import jakarta.annotation.Resource;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DebeziumConsumerListener
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
@Service
public class DebeziumConsumerListener {
    private static final Logger log = LogUtils.getLogger();
    private static final LinkedBlockingQueue<DebeziumDataBean> DATA_LOG_QUEUE = new LinkedBlockingQueue<>();

    private DeserializerAdapter adapter = new DeserializerAdapter();
    private DebeziumDataHandler debeziumDataHandler;
    private int maxBachSize;
    @Resource
    private MetaDataService metaDataService;
    @Resource
    private ExtractProperties extractProperties;

    @PostConstruct
    public void initDebeziumDataHandler() {
        debeziumDataHandler = adapter.getHandler(extractProperties.getDebeziumSerializer());
        debeziumDataHandler.setSchema(extractProperties.getSchema());
        debeziumDataHandler.setDebeziumRowDisplay(extractProperties.isDebeziumRowDisplay());
        debeziumDataHandler.injectMetaDataServiceInstanceToHandler(metaDataService);
    }

    /**
     * listen
     *
     * @param record record
     */
    public void listen(ConsumerRecord<String, Object> record) {
        try {
            final long offset = record.offset();
            debeziumDataHandler.handler(offset, record.value(), DATA_LOG_QUEUE);
        } catch (DebeziumConfigException | JSONException ex) {
            // Abnormal message structure, ignoring the current message
            log.error("parse message abnormal: [{}] {} ignoring this message : {}", ex.getMessage(),
                    System.getProperty("line.separator"), record);
        }
    }

    /**
     * data log queue size
     *
     * @return size
     */
    public int size() {
        return DATA_LOG_QUEUE.size();
    }

    /**
     * pool DebeziumDataBean from queue
     *
     * @return DebeziumDataBean
     */
    public DebeziumDataBean poll() {
        return DATA_LOG_QUEUE.poll();
    }

    /**
     * maxBachSize
     *
     * @param maxBachSize maxBachSize
     */
    public void setMaxBatchSize(int maxBachSize) {
        this.maxBachSize = maxBachSize;
    }

    /**
     * maxBachSize
     *
     * @return maxBachSize
     */
    public int getMaxBatchSize() {
        return this.maxBachSize;
    }
}
