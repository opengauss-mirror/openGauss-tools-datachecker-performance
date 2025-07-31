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

import java.time.Duration;
import java.time.LocalDateTime;

/**
 * AbstractSliceProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public abstract class AbstractSliceProcessor extends AbstractProcessor {
    private static final Logger log = LogUtils.getLogger(AbstractSliceProcessor.class);

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
}
