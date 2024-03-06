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

package org.opengauss.datachecker.check.slice;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.modules.check.AbstractCheckDiffResultBuilder.CheckDiffResultBuilder;
import org.opengauss.datachecker.check.modules.check.CheckDiffResult;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.CHECK_EXECUTOR;

/**
 * SliceCheckEventHandler
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@Component
public class SliceCheckEventHandler {
    private static final Logger log = LogUtils.getLogger(SliceCheckEventHandler.class);

    private SliceCheckContext sliceCheckContext;
    private TaskRegisterCenter registerCenter;
    private ThreadPoolExecutor executor;

    /**
     * init dynamic thread pool for slice check worker
     */
    public void initSliceCheckEventHandler(DynamicThreadPoolManager dynamicThreadPoolManager,
        SliceCheckContext sliceCheckContext, TaskRegisterCenter registerCenter) {
        this.sliceCheckContext = sliceCheckContext;
        this.registerCenter = registerCenter;
        this.executor = dynamicThreadPoolManager.getExecutor(CHECK_EXECUTOR);
    }

    /**
     * handle slice check event
     * if table structure is equal, then create slice check worker ,and add into executor.
     * if not equal,add check result that table structure not equal.
     *
     * @param checkEvent check event
     */
    public void handle(SliceCheckEvent checkEvent) {
        if (checkTableStructure(checkEvent)) {
            LogUtils.debug(log, "slice check event {} is dispatched, and checked level=[isTableLevel={}]",
                checkEvent.getCheckName(), checkEvent.isTableLevel());
            if (checkEvent.isTableLevel()) {
                executor.submit(new TableCheckWorker(checkEvent, sliceCheckContext));
            } else {
                executor.submit(new SliceCheckWorker(checkEvent, sliceCheckContext, registerCenter));
            }
        } else {
            LogUtils.info(log, "slice check event , table structure diff [{}][{} : {}]", checkEvent.getCheckName(),
                checkEvent.getSource()
                          .getTableHash(), checkEvent.getSink()
                                                     .getTableHash());
            handleTableStructureDiff(checkEvent);
            registerCenter.refreshCheckedTableCompleted(checkEvent.getSlice()
                                                                  .getTable());
        }
    }

    private void handleTableStructureDiff(SliceCheckEvent checkEvent) {
        SliceExtend source = checkEvent.getSource();
        SliceExtend sink = checkEvent.getSink();
        long count = Math.max(source.getCount(), sink.getCount());
        sliceCheckContext.refreshSliceCheckProgress(checkEvent.getSlice(), count);
        CheckDiffResult result = buildTableStructureDiffResult(checkEvent.getSlice(), (int) count);
        sliceCheckContext.addTableStructureDiffResult(checkEvent.getSlice(), result);
    }

    private CheckDiffResult buildTableStructureDiffResult(SliceVo slice, int count) {
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        builder.checkMode(ConfigCache.getCheckMode())
               .process(ConfigCache.getValue(ConfigConstants.PROCESS_NO))
               .schema(slice.getSchema())
               .table(slice.getTable())
               .sno(slice.getNo())
               .startTime(LocalDateTime.now())
               .endTime(LocalDateTime.now())
               .isTableStructureEquals(false)
               .isExistTableMiss(false, null)
               .rowCount(count)
               .error("table structure diff");
        return builder.build();
    }

    private boolean checkTableStructure(SliceCheckEvent checkEvent) {
        SliceExtend source = checkEvent.getSource();
        SliceExtend sink = checkEvent.getSink();
        return source.getTableHash() == sink.getTableHash();
    }

    /**
     * handleIgnoreTable
     *
     * @param slice  slice
     * @param source source
     * @param sink   sink
     */
    public void handleIgnoreTable(SliceVo slice, SliceExtend source, SliceExtend sink) {
        sliceCheckContext.refreshSliceCheckProgress(slice, 0);
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        Endpoint existEndpoint = Objects.nonNull(source) && Objects.isNull(sink) ? Endpoint.SOURCE : Endpoint.SINK;
        builder.checkMode(ConfigCache.getCheckMode())
               .process(ConfigCache.getValue(ConfigConstants.PROCESS_NO))
               .schema(slice.getSchema())
               .table(slice.getTable())
               .sno(slice.getNo())
               .startTime(LocalDateTime.now())
               .endTime(LocalDateTime.now())
               .isTableStructureEquals(false)
               .isExistTableMiss(true, existEndpoint)
               .rowCount(0)
               .error("table miss");
        CheckDiffResult result = builder.build();
        sliceCheckContext.addTableStructureDiffResult(slice, result);
        registerCenter.refreshCheckedTableCompleted(slice.getTable());
    }
}
