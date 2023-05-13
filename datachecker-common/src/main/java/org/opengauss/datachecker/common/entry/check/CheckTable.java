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

package org.opengauss.datachecker.common.entry.check;

import lombok.Builder;
import lombok.Data;

/**
 * CheckTable
 *
 * @author ：wangchao
 * @date ：Created in 2023/5/13
 * @since ：11
 */
@Data
@Builder
public class CheckTable {
    private String tableName;
    private String topicName;
    private int partition;
    private long rowCount;
    private long avgRowLength;
    private long completeTimestamp;

    /**
     * get complete timestamp to second
     *
     * @return second
     */
    public long getCompleteSecond() {
        return completeTimestamp / 1000;
    }
}
