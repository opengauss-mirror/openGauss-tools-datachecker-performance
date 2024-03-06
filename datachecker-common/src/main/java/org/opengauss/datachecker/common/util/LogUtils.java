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

package org.opengauss.datachecker.common.util;

import org.apache.logging.log4j.Level;
import org.apache.logging.log4j.LogManager;
import org.apache.logging.log4j.Logger;

/**
 * LogUtils
 *
 * @author ：wangchao
 * @date ：Created in 2023/6/14
 * @since ：11
 */
public class LogUtils {
    private static final String PROGRESS = "progress";
    private static final String BUSINESS = "business";
    private static final String KAFKA = "kafka";

    /**
     * get kafka business logger
     *
     * @return logger
     */
    public static Logger getKafkaLogger() {
        return LogManager.getLogger(KAFKA);
    }

    /**
     * logger
     *
     * @return logger
     */
    public static Logger getLogger() {
        return LogManager.getLogger(PROGRESS);
    }

    /**
     * logger
     *
     * @return logger
     */
    public static Logger getLogger(String name) {
        return LogManager.getLogger(name);
    }

    /**
     * logger
     *
     * @return logger
     */
    public static Logger getBusinessLogger() {
        return LogManager.getLogger(BUSINESS);
    }

    /**
     * 指定class 类型Logger
     *
     * @param classz classz
     * @return logger
     */
    public static Logger getLogger(Class classz) {
        return LogManager.getLogger(classz.getName());
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     */
    public static void debug(Logger logger, String message) {
        if (logger.isDebugEnabled()) {
            logger.debug(message);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0, Object p1) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0, p1);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0, Object p1, Object p2) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0, p1, p2);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0, Object p1, Object p2, Object p3) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0, p1, p2, p3);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0, Object p1, Object p2, Object p3, Object p4) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0, p1, p2, p3, p4);
        }
    }

    /**
     * Logs a message with parameters at debug level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     * @param p4      parameter to the message.
     * @param p5      parameter to the message.
     */
    public static void debug(Logger logger, String message, Object p0, Object p1, Object p2, Object p3, Object p4,
        Object p5) {
        if (logger.isDebugEnabled()) {
            logger.debug(message, p0, p1, p2, p3, p4, p5);
        }
    }

    /**
     * Logs a message object with the {@link Level#WARN WARN} level.
     *
     * @param logger  logger
     * @param message the message string to log.
     */
    public static void warn(Logger logger, String message) {
        logger.warn(message);
    }

    /**
     * Logs a message object with the {@link Level#WARN WARN} level.
     *
     * @param logger  logger
     * @param message the message string to log.
     * @param p0      parameter to the message.
     */
    public static void warn(Logger logger, String message, Object p0) {
        logger.warn(message, p0);
    }

    /**
     * Logs a message object with the {@link Level#WARN WARN} level.
     *
     * @param logger  logger
     * @param message the message string to log.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    public static void warn(Logger logger, String message, Object p0, Object p1) {
        logger.warn(message, p0, p1);
    }

    /**
     * Logs a message object with the {@link Level#WARN WARN} level.
     *
     * @param logger  logger
     * @param message the message string to log.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    public static void warn(Logger logger, String message, Object p0, Object p1, Object p2) {
        logger.warn(message, p0, p1, p2);
    }

    /**
     * Logs a message object with the {@link Level#WARN WARN} level.
     *
     * @param logger  logger
     * @param message the message string to log.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    public static void warn(Logger logger, String message, Object p0, Object p1, Object p2, Object p3) {
        logger.warn(message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     */
    public static void info(Logger logger, String message) {
        logger.info(message);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     */
    public static void info(Logger logger, String message, Object p0) {
        logger.info(message, p0);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    public static void info(Logger logger, String message, Object p0, Object p1) {
        logger.info(message, p0, p1);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    public static void info(Logger logger, String message, Object p0, Object p1, Object p2) {
        logger.info(message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at info level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    public static void info(Logger logger, String message, Object p0, Object p1, Object p2, Object p3) {
        logger.info(message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     */
    public static void error(Logger logger, String message) {
        logger.error(message);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     */
    public static void error(Logger logger, String message, Object p0) {
        logger.error(message, p0);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     */
    public static void error(Logger logger, String message, Object p0, Object p1) {
        logger.error(message, p0, p1);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     */
    public static void error(Logger logger, String message, Object p0, Object p1, Object p2) {
        logger.error(message, p0, p1, p2);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger  logger
     * @param message the message to log; the format depends on the message factory.
     * @param p0      parameter to the message.
     * @param p1      parameter to the message.
     * @param p2      parameter to the message.
     * @param p3      parameter to the message.
     */
    public static void error(Logger logger, String message, Object p0, Object p1, Object p2, Object p3) {
        logger.error(message, p0, p1, p2, p3);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger    logger
     * @param message   the message to log; the format depends on the message factory.
     * @param throwable the {@code Throwable} to log, including its stack trace.
     */
    public static void error(Logger logger, String message, Throwable throwable) {
        logger.error(message, throwable);
    }

    /**
     * Logs a message with parameters at error level.
     *
     * @param logger    logger
     * @param message   the message to log; the format depends on the message factory.
     * @param p0        parameter to the message.
     * @param throwable the {@code Throwable} to log, including its stack trace.
     */
    public static void error(Logger logger, String message, Object p0, Throwable throwable) {
        logger.error(message, p0, throwable);
    }
}
