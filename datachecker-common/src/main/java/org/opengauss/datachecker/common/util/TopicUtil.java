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

import net.openhft.hashing.LongHashFunction;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

/**
 * TopicUtil
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
public class TopicUtil {
    public static final int TOPIC_MAX_PARTITIONS = 16;
    public static final int TOPIC_MIN_PARTITIONS = 1;

    private static final String TOPIC_NAME_TEMPLATE = "CHECK_%s_%s_NUM_%s_TABLE_DATA";
    private static final String TOPIC_TEMPLATE = "CHECK_%s_%s_%s_%s";
    private static final String UPPER_CODE = "1";
    private static final String LOWER_CODE = "0";
    private static final LongHashFunction XX_3_HASH = LongHashFunction.xx3(199972221018L);

    /**
     * buildTopicName
     *
     * @param process   process
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return topicName
     */
    public static String buildTopicName(String process, Endpoint endpoint, String tableName) {
        return String.format(TOPIC_TEMPLATE, process, endpoint.getCode(), formatTableName(tableName),
            letterCaseEncoding(tableName));
    }

    /**
     * Format tableName , Because ,
     * The maximum length of topic name is 249 ,
     * The maximum length of table name is 64 ,
     * so, When the character is not a number or English character, convert it to the corresponding ascii code value
     *
     * @param tableName tableName
     * @return format tableName
     */
    public static String formatTableName(String tableName) {
        final char[] nameChars = tableName.toCharArray();
        StringBuilder sb = new StringBuilder();
        for (char nameChar : nameChars) {
            if (isAlphanumerics(nameChar) || isKafkaTopicAlpha(nameChar)) {
                sb.append(nameChar);
            } else {
                sb.append((int) nameChar);
            }
        }
        return sb.toString();
    }

    private static boolean isNumeric(char aChar) {
        return aChar >= '0' && aChar <= '9';
    }

    private static boolean isKafkaTopicAlpha(char aChar) {
        return aChar == '-' || aChar == '_';
    }

    private static boolean isAlphanumerics(char aChar) {
        return isEnglishLetters(aChar) || isNumeric(aChar);
    }

    private static boolean isEnglishLetters(char aChar) {
        return (aChar >= 'a' && aChar <= 'z') || (aChar >= 'A' && aChar <= 'Z');
    }

    /**
     * Calculate the Kafka partition according to the total number of task slices.
     * The total number of Kafka partitions shall not exceed {@value TOPIC_MAX_PARTITIONS}
     *
     * @param divisions Number of task slices extracted
     * @return Total number of Kafka partitions
     */
    public static int calcPartitions(int divisions) {
        if (divisions <= 1) {
            return TOPIC_MIN_PARTITIONS;
        }
        final int partitions = divisions / 2;
        return Math.min(partitions, TOPIC_MAX_PARTITIONS);
    }

    private static String letterCaseEncoding(String tableName) {
        final char[] chars = tableName.toCharArray();
        StringBuilder builder = new StringBuilder("0");
        for (char aChar : chars) {
            if (aChar >= 'A' && aChar <= 'Z') {
                builder.append(UPPER_CODE);
            } else if (aChar >= 'a' && aChar <= 'z') {
                builder.append(LOWER_CODE);
            }
        }
        final String encoding = builder.toString();
        return Long.toHexString(Long.valueOf(encoding, 2));
    }

    public static String getTableWithLetter(String tableName) {
        return tableName + "_" + letterCaseEncoding(tableName);
    }

    /**
     * 根据表名称的Hash值动态获取当前表对应的Topic名称
     *
     * @param processNo    processNo
     * @param endpoint     endpoint
     * @param tableName    tableName
     * @param maxTopicSize maxTopicSize
     * @return topic
     */
    public static String getMoreFixedTopicName(String processNo, Endpoint endpoint, String tableName,
        int maxTopicSize) {
        int no = (int) (XX_3_HASH.hashChars(tableName) % maxTopicSize);
        return String.format(TopicUtil.TOPIC_NAME_TEMPLATE, processNo, endpoint, Math.abs(no));
    }

    /**
     * 动态构建固定Topic名称
     *
     * @param processNo processNo
     * @param endpoint  endpoint
     * @param num       num
     * @return topic
     */
    public static String createMoreFixedTopicName(String processNo, Endpoint endpoint, int num) {
        return String.format(TopicUtil.TOPIC_NAME_TEMPLATE, processNo, endpoint, num);
    }
}
