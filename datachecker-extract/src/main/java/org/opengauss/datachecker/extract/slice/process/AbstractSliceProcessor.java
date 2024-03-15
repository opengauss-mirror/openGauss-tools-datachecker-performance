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
import org.opengauss.datachecker.common.entry.enums.SliceStatus;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.TopicUtil;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.springframework.kafka.support.SendResult;
import org.springframework.util.concurrent.ListenableFuture;

import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Iterator;
import java.util.List;
import java.util.concurrent.ExecutionException;

/**
 * AbstractSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public abstract class AbstractSliceProcessor extends AbstractProcessor {
    protected static final int FETCH_SIZE = 10000;
    protected static final Logger log = LogUtils.getLogger(AbstractSliceProcessor.class);

    protected SliceVo slice;
    protected final String topic;
    protected final String table;
    /**
     * AbstractSliceProcessor
     *
     * @param slice   slice
     * @param context context
     */
    protected AbstractSliceProcessor(SliceVo slice, SliceProcessorContext context) {
        super(context);
        this.slice = slice;
        this.table = slice.getTable();
        String process = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        int maximumTopicSize = ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TOPIC_SIZE);
        this.topic = TopicUtil.getMoreFixedTopicName(process, ConfigCache.getEndPoint(), slice.getTable(), maximumTopicSize);
    }

    /**
     * create table extend instance,feedback to check
     *
     * @param tableHash table hash
     * @return slice extend
     */
    protected SliceExtend createSliceExtend(long tableHash) {
        SliceExtend sliceExtend = new SliceExtend();
        sliceExtend.setName(slice.getName());
        sliceExtend.setNo(slice.getNo());
        sliceExtend.setStatus(SliceStatus.codeOf(ConfigCache.getEndPoint()));
        sliceExtend.setEndpoint(ConfigCache.getEndPoint());
        sliceExtend.setTableHash(tableHash);
        return sliceExtend;
    }

    /**
     * Duration between(start, end) and calc Millis times
     *
     * @param start start
     * @param end   end
     * @return millis
     */
    protected long durationBetweenToMillis(LocalDateTime start, LocalDateTime end) {
        return Duration.between(start, end)
                       .toMillis();
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
        return new long[] {minOffset, maxOffset};
    }

    private long getFutureOffset(ListenableFuture<SendResult<String, String>> next) {
        try {
            SendResult<String, String> sendResult = next.get();
            return sendResult.getRecordMetadata()
                             .offset();
        } catch (InterruptedException | ExecutionException e) {
            LogUtils.warn(log,"get record offset InterruptedException  or ExecutionException");
        }
        return 0;
    }
}
