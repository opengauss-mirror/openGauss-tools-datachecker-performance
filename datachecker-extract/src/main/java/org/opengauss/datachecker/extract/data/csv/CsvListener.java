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

package org.opengauss.datachecker.extract.data.csv;

import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.extract.constants.ExtConstants;

/**
 * CsvListener
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
public interface CsvListener {

    /**
     * init csv listener
     */
    void initCsvListener();

    /**
     * poll slice vo from listener queue
     * Retrieves and removes the head of this queue, or returns null if this queue is empty
     *
     * @return slice
     */
    SliceVo poll();

    /**
     * stop tailer listener
     */
    void stop();

    /**
     * check slice ptn num, if ptn num is invalid, set min ptn num
     * @param slice slice
     */
    default void checkSlicePtnNum(SliceVo slice) {
        if (slice.getPtnNum() < ExtConstants.MIN_TOPIC_PTN_NUM) {
            slice.setPtnNum(ExtConstants.MIN_TOPIC_PTN_NUM);
        }
    };

    /**
     * listener is finished
     *
     * @return true | false
     */
    boolean isFinished();
}
