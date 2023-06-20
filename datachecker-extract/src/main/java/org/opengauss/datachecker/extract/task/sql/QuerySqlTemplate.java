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

package org.opengauss.datachecker.extract.task.sql;

/**
 * Query SQL build template
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/2
 * @since ：11
 */
public interface QuerySqlTemplate {
    /**
     * Query SQL statement columnsList fragment
     */
    String COLUMN = ":columnsList";

    /**
     * Query SQL statement tableName fragment
     */
    String TABLE_NAME = ":tableName";

    /**
     * Query SQL statement primaryKey fragment
     */
    String PRIMARY_KEY = ":primaryKey";

    /**
     * Query SQL statement schema fragment
     */
    String SCHEMA = ":schema";

    /**
     * Query SQL statement start fragment: Start position of fragment query
     */
    String START = ":start";

    /**
     * Query SQL statement offset fragment: Fragment query offset
     */
    String OFFSET = ":offset";
    String ORDER_BY = ":orderBy";
    /**
     * Query SQL statement joinOn fragment: Query SQL statement joinOn fragment
     */
    String JOIN_ON = ":joinOn";

    /**
     * Query SQL statement fragment: Query SQL statements in the scenario without offset
     */
    String QUERY_OFF_SET_ZERO = "SELECT :columnsList FROM :schema.:tableName";

    /**
     * Query SQL statement fragment: SQL statement for fragment query using offset in single primary key scenario
     */
    String QUERY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName :orderBy LIMIT :start,:offset";

    /**
     * Query SQL statement fragment: SQL statement for fragment query using offset in single primary key scenario
     */
    String QUERY_NO_OFF_SET = "SELECT :columnsList FROM :schema.:tableName";

    /**
     * Query SQL statement fragment: SQL statement for fragment query using offset in multiple primary key scenario
     */
    String QUERY_MULTIPLE_PRIMARY_KEY_OFF_SET = "SELECT :columnsList FROM :schema.:tableName a  RIGHT JOIN "
        + " (SELECT :primaryKey FROM :schema.:tableName order by :primaryKey LIMIT :start,:offset) b ON :joinOn";
    
    /**
     * Query SQL statement fragment: SQL statement for fragment query using offset in single primary key scenario
     */
    String QUERY_BETWEEN_SET = "SELECT :columnsList FROM :schema.:tableName WHERE :primaryKey "
            + " BETWEEN :start AND :offset "
            +" :orderBy ";
    /**
     * Query SQL statement fragment: SQL statement field spacing symbol
     */
    String DELIMITER = ",";
    String MYSQL_ESCAPE = "`";
    String MYSQL_DELIMITER = MYSQL_ESCAPE + DELIMITER + MYSQL_ESCAPE;
    String OPENGAUSS_ESCAPE = "\"";
    String OPENGAUSS_DELIMITER = OPENGAUSS_ESCAPE + DELIMITER + OPENGAUSS_ESCAPE;
    /**
     * Query SQL statement fragment: SQL statement equality condition symbol
     */
    String EQUAL_CONDITION = "=";

    /**
     * Query SQL statement and fragment
     */
    String AND_CONDITION = " and ";

    /**
     * Query SQL statement table alias fragment: table alias
     */
    String TABLE_ALIAS = "a.";

    /**
     * Query SQL statement sub table alias fragment:  Sub query result alias
     */
    String SUB_TABLE_ALIAS = "b.";
}
