/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.datachecker.common.load;

import com.alibaba.fastjson.JSON;
import org.apache.kafka.clients.producer.KafkaProducer;
import org.apache.kafka.clients.producer.Producer;
import org.apache.kafka.clients.producer.ProducerConfig;
import org.apache.kafka.clients.producer.ProducerRecord;
import org.apache.kafka.common.serialization.StringSerializer;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;
import org.apache.logging.log4j.core.Appender;
import org.apache.logging.log4j.core.LoggerContext;
import org.apache.logging.log4j.core.config.Configuration;
import org.apache.logging.log4j.core.config.LoggerConfig;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.service.DynamicThreadPoolMonitor;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.boot.ApplicationArguments;
import org.springframework.boot.ApplicationRunner;
import org.springframework.stereotype.Component;

import java.util.HashMap;
import java.util.Map;

/**
 * AlertLogCollectionManager
 *
 * @since 2024/12/11
 */
@Component
public class AlertLogCollectionManager implements ApplicationRunner {
    private static final Logger log = LogUtils.getLogger(DynamicThreadPoolMonitor.class);
    private static final String KAFKA_APPENDER_NAME = "kafka";

    private static volatile boolean isAddAppender = false;
    private static boolean isAlertLogCollectionEnabled = false;
    private static String kafkaServer;
    private static String kafkaKey;
    private static String kafkaTopic;

    @Override
    public void run(ApplicationArguments args) {
        handle();
    }

    /**
     * load config and register error code
     */
    public static void handle() {
        if (!isAddAppender) {
            synchronized (AlertLogCollectionManager.class) {
                if (!isAddAppender) {
                    loadConfig();
                    if (isAlertLogCollectionEnabled) {
                        registerErrorCode();
                    } else {
                        removeKafkaAppender();
                    }
                    isAddAppender = true;
                }
            }
        }
    }

    private static void loadConfig() {
        if (!getSystemProperty("enable.alert.log.collection").equals("true")) {
            return;
        }

        isAlertLogCollectionEnabled = true;
        kafkaServer = getSystemProperty("kafka.bootstrapServers");
        kafkaKey = getSystemProperty("kafka.key");
        kafkaTopic = getSystemProperty("kafka.topic");

        if (kafkaServer.isEmpty() || kafkaKey.isEmpty() || kafkaTopic.isEmpty()) {
            log.error("Kafka configuration is missing. "
                    + "Please check kafka.bootstrapServers, kafka.key, and kafka.topic properties.");
        }
    }

    private static void registerErrorCode() {
        Map<Integer, String> codeCauseCnMap = new HashMap<>();
        Map<Integer, String> codeCauseEnMap = new HashMap<>();
        ErrorCode[] errorCodes = ErrorCode.values();
        for (ErrorCode errorCode : errorCodes) {
            codeCauseCnMap.put(errorCode.getCode(), errorCode.getCauseCn());
            codeCauseEnMap.put(errorCode.getCode(), errorCode.getCauseEn());
        }
        String codeCauseCnString = JSON.toJSONString(codeCauseCnMap);
        String codeCauseEnString = JSON.toJSONString(codeCauseEnMap);

        Map<String, Object> config = new HashMap<>();
        config.put(ProducerConfig.BOOTSTRAP_SERVERS_CONFIG, kafkaServer);
        config.put(ProducerConfig.KEY_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());
        config.put(ProducerConfig.VALUE_SERIALIZER_CLASS_CONFIG, StringSerializer.class.getName());

        Producer<String, String> producer = new KafkaProducer<String, String>(config);
        ProducerRecord<String, String> record = new ProducerRecord<>(kafkaTopic, "datachecker",
                "<CODE:causeCn>" + codeCauseCnString + "<CODE:causeEn>" + codeCauseEnString);
        producer.send(record);
        producer.close();
    }

    private static void removeKafkaAppender() {
        if (LogManager.getContext(false) instanceof LoggerContext) {
            LoggerContext context = (LoggerContext) LogManager.getContext(false);
            Configuration configuration = context.getConfiguration();

            LoggerConfig loggerConfig = configuration.getLoggerConfig(LogManager.ROOT_LOGGER_NAME);
            loggerConfig.removeAppender(KAFKA_APPENDER_NAME);

            Appender kafkaAppender = configuration.getAppenders().get(KAFKA_APPENDER_NAME);
            if (kafkaAppender != null && !kafkaAppender.isStopped()) {
                kafkaAppender.stop();
                log.info("KafkaAppender has been stopped.");
            }
        }
    }

    private static String getSystemProperty(String propertyName) {
        String propertyValue = System.getProperty(propertyName);
        if (propertyValue == null || propertyValue.trim().isEmpty()) {
            log.error("Required property " + propertyName + " is missing or empty.");
            return "";
        }
        return propertyValue;
    }
}
