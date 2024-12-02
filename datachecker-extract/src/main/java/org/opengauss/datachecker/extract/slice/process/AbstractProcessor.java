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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.math.BigDecimal;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * AbstractProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public abstract class AbstractProcessor implements SliceProcessor {
    /**
     * JDBC fetch size
     */
    protected static final int FETCH_SIZE = 200;

    /**
     * log
     */
    private static final Logger log = LogUtils.getLogger(AbstractProcessor.class);

    protected SliceProcessorContext context;
    protected int objectSizeExpansionFactor;

    /**
     * AbstractProcessor
     *
     * @param context context
     */
    protected AbstractProcessor(SliceProcessorContext context) {
        this.context = context;
        this.objectSizeExpansionFactor = ConfigCache.getIntValue(ConfigConstants.OBJECT_SIZE_EXPANSION_FACTOR);
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
        return rowLengthNum.multiply(sliceSizeNum)
                           .multiply(BigDecimal.valueOf(objectSizeExpansionFactor))
                           .longValue();
    }

    /**
     * feedback current slice complete status
     *
     * @param sliceExtend slice extend
     */
    protected void feedbackStatus(SliceExtend sliceExtend) {
        context.feedbackStatus(sliceExtend);
    }

    /**
     * Analyze the sending result SendResult of the sharding record <br>
     * and obtain the offset range written to the topic in the current set, offset (min, max)
     *
     * @param batchFutures batchFutures
     * @return offset (min, max)
     */
    protected long[] getBatchFutureRecordOffsetScope(List<ListenableFuture<SendResult<String, String>>> batchFutures) {
        Iterator<ListenableFuture<SendResult<String, String>>> futureIterator = batchFutures.iterator();
        ListenableFuture<SendResult<String, String>> candidate = futureIterator.next();
        long minOffset = getFutureOffset(candidate);
        long maxOffset = minOffset;

        while (futureIterator.hasNext()) {
            long next = getFutureOffset(futureIterator.next());
            if (next < minOffset) {
                minOffset = next;
            }
            if (next > maxOffset) {
                maxOffset = next;
            }
        }
        return new long[]{minOffset, maxOffset};
    }

    private long getFutureOffset(ListenableFuture<SendResult<String, String>> next) {
        try {
            SendResult<String, String> sendResult = next.get();
            return sendResult.getRecordMetadata()
                    .offset();
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.warn(log, "get record offset InterruptedException  or ExecutionException");
        }
        return 0;
    }

    /**
     * min offset
     *
     * @param offsetList offsetList
     * @return min
     */
    protected long getMinOffset(List<long[]> offsetList) {
        return offsetList.stream()
                .mapToLong(a -> a[0])
                .min()
                .orElse(0L);
    }

    /**
     * 获取最大偏移量集合中的最大值
     *
     * @param maxOffsetList maxOffsetList
     * @return 最大值
     */
    protected long getMaxMaxOffset(List<Long> maxOffsetList) {
        return maxOffsetList.stream().max(Long::compareTo).get();
    }

    /**
     * 获取最小偏移量集合中的最小值
     *
     * @param minOffsetList minOffsetList
     * @return 最小值
     */
    protected long getMinMinOffset(List<Long> minOffsetList) {
        return minOffsetList.stream().min(Long::compareTo).get();
    }

    /**
     * max offset
     *
     * @param offsetList offsetList
     * @return max
     */
    protected long getMaxOffset(List<long[]> offsetList) {
        return offsetList.stream()
                .mapToLong(a -> a[1])
                .max()
                .orElse(0L);
    }
}
