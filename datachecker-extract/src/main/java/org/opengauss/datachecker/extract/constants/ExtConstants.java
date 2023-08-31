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

package org.opengauss.datachecker.extract.constants;

import org.opengauss.datachecker.common.constant.Constants;

/**
 * ExtConstants
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/25
 * @since ：11
 */
public interface ExtConstants {
    /**
     * Combined primary key splice connector
     */
    String PRIMARY_DELIMITER = Constants.PRIMARY_DELIMITER;

    /**
     * DELIMITER ,
     */
    String DELIMITER = Constants.DELIMITER;

    /**
     * query result parsing ResultSet data result set,default start index position
     */
    int COLUMN_INDEX_FIRST_ZERO = 0;

    String PROFILE_SINK = "sink";
    String PROFILE_SOURCE = "source";
    String CSV_LISTENER_END = "finished";
    int MIN_TOPIC_PTN_NUM = 1;
}
