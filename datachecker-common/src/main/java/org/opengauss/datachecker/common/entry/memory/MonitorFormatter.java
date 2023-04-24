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

package org.opengauss.datachecker.common.entry.memory;

import java.util.List;
import java.util.stream.Collectors;

/**
 * MonitorFormatter
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/25
 * @since ：11
 */
public interface MonitorFormatter {
    /**
     * line Separator
     */
    String LINE_SEPARATOR = System.lineSeparator();

    /**
     * title Separator
     */
    String TITLE_SEPARATOR = " --> ";

    /**
     * field Separator
     */
    String FIELD_SEPARATOR = ", ";

    /**
     * format information title
     *
     * @return title
     */
    String getTitle();

    /**
     * get format fields
     *
     * @return format fields
     */
    List<Field> getFormatFields();

    /**
     * Provide standardized formatting methods
     *
     * @return format String result
     */
    default String format() {
        List<Field> fieldList = getFormatFields();
        return getTitle() + TITLE_SEPARATOR + fieldList.stream().map(Field::toString)
                                                       .collect(Collectors.joining(FIELD_SEPARATOR)) + LINE_SEPARATOR;
    }

    /**
     * format field
     */
    class Field {
        private String name;
        private String value;

        /**
         * construct Filed Object
         *
         * @param name  field name
         * @param value field value
         */
        public Field(String name, String value) {
            this.name = name;
            this.value = value;
        }

        @Override
        public String toString() {
            return name + ": " + value + " ";
        }

        /**
         * construct Filed Object
         *
         * @param name  field name
         * @param value field value
         * @return field
         */
        public static Field of(String name, String value) {
            return new Field(name, value);
        }

        /**
         * construct Filed Object
         *
         * @param name  field name
         * @param value field value
         * @return field
         */
        public static Field of(String name, int value) {
            return new Field(name, Integer.toString(value));
        }

        /**
         * construct Filed Object
         *
         * @param name  field name
         * @param value field value
         * @return field
         */
        public static Field of(String name, Double value) {
            return new Field(name, Double.toString(value));
        }
    }
}
