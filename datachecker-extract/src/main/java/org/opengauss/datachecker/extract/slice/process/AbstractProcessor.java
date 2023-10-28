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

package org.opengauss.datachecker.extract.slice.process;

import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;

import java.math.BigDecimal;

/**
 * AbstractProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public abstract class AbstractProcessor implements SliceProcessor {
    protected SliceProcessorContext context;

    public AbstractProcessor(SliceProcessorContext context) {
        this.context = context;
    }

    /**
     * estimated memory size
     *
     * @param rowLength avg row length
     * @param sliceSize slice row size
     * @return memory size
     */
    protected long estimatedMemorySize(long rowLength, long sliceSize) {
        BigDecimal rowLengthNum = BigDecimal.valueOf(rowLength);
        BigDecimal sliceSizeNum = BigDecimal.valueOf(sliceSize);
        return rowLengthNum.multiply(sliceSizeNum).longValue();
    }

    /**
     * feedback current slice complete status
     *
     * @param sliceExtend slice extend
     */
    protected void feedbackStatus(SliceExtend sliceExtend) {
        context.feedbackStatus(sliceExtend);
    }
}
