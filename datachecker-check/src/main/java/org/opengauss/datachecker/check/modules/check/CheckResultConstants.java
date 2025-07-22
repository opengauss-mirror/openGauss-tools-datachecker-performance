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

package org.opengauss.datachecker.check.modules.check;

/**
 * CheckResultConstants
 *
 * @author ：wangchao
 * @date ：Created in 2023/2/21
 * @since ：11
 */
public interface CheckResultConstants {
    String RESULT_FAILED = "failed";
    String FAILED_LOG_NAME = "failed.log";
    String CSV_FAILED_DETAIL_NAME = "failed_detail.log";
    String RESULT_SUCCESS = "success";

    /**
     * The display quantity of the verification result failure records
     */
    int MAX_DISPLAY_SIZE = 100;

    String FAILED_MESSAGE = " failed (insert=%d update=%d delete=%d)";
    String STRUCTURE_NOT_EQUALS = "table structure is not equals , please check the database sync !";
    String TABLE_NOT_EXISTS = "table [%s] , only exist in %s !";
    String CHECKED_ROW_CONDITION = "%s.%s checked, row limit %s,%s ";
    String CHECKED_PARTITIONS = "%s.%s_[%s] checked ";
    String CHECKED_DIFF_TOO_LARGE = " data error is too large , please check the database sync !";
    String LEFT_SQUARE_BRACKET = "[";
    String COMMA = ",";

    /**
     * Check result constants EmptyArray []
     */
    String EMPTY_ARRAY = "[]";

    /**
     * Check result constants right square  ]
     */
    String RIGHT_SQUARE_BRACKET = "]";
}
