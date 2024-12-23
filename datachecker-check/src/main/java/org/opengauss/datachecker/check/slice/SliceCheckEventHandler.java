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
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.stereotype.Component;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.CHECK_EXECUTOR;

import javax.annotation.Resource;

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

    @Resource
    private EndpointMetaDataManager metaDataManager;
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
        String tableName = checkEvent.getSlice().getTable();
        boolean isTableHasRows = checkTableHasRows(tableName);
        if (!isTableHasRows) {
            handleTableEmpty(checkEvent);
            registerCenter.refreshCheckedTableCompleted(tableName);
        } else if (checkTableStructure(checkEvent)) {
            LogUtils.info(log, "slice event {} is dispatched, and checked level=[isTableLevel={} isTableEmpty={}]",
                checkEvent.getCheckName(), checkEvent.isTableLevel(), isTableHasRows);
            if (checkEvent.isTableLevel()) {
                executor.submit(new TableCheckWorker(checkEvent, sliceCheckContext));
            } else {
                executor.submit(new SliceCheckWorker(checkEvent, sliceCheckContext, registerCenter));
            }
        } else {
            LogUtils.info(log, "slice check event , table structure diff [{}]", checkEvent.toString());
            handleTableStructureDiff(checkEvent);
            registerCenter.refreshCheckedTableCompleted(tableName);
        }
    }

    private void handleTableEmpty(SliceCheckEvent checkEvent) {
        sliceCheckContext.refreshSliceCheckProgress(checkEvent.getSlice(), 0);
        CheckDiffResult result = buildSliceSuccessResult(checkEvent.getSlice(), 0, true);
        sliceCheckContext.addCheckResult(checkEvent.getSlice(), result);
    }

    private boolean checkTableHasRows(String tableName) {
        TableMetadata sourceMeta = metaDataManager.getTableMetadata(Endpoint.SOURCE, tableName);
        TableMetadata sinkMeta = metaDataManager.getTableMetadata(Endpoint.SINK, tableName);
        return sourceMeta.isExistTableRows() && sinkMeta.isExistTableRows();
    }

    /**
     * 添加校验失败分片事件处理流程
     *
     * @param checkEvent checkEvent
     */
    public void handleFailed(SliceCheckEvent checkEvent) {
        LogUtils.warn(log, "slice check event , table slice has unknown error [{}][{} : {}]", checkEvent.getCheckName(),
            checkEvent.getSource(), checkEvent.getSink());
        long count = getCheckSliceCount(checkEvent);
        sliceCheckContext.refreshSliceCheckProgress(checkEvent.getSlice(), count);
        CheckDiffResult result = buildSliceDiffResult(checkEvent.getSlice(), (int) count, true,
            "slice has unknown error");
        sliceCheckContext.addCheckResult(checkEvent.getSlice(), result);
        registerCenter.refreshCheckedTableCompleted(checkEvent.getSlice().getTable());
    }

    private static long getCheckSliceCount(SliceCheckEvent checkEvent) {
        SliceExtend source = checkEvent.getSource();
        SliceExtend sink = checkEvent.getSink();
        if (Objects.nonNull(sink) && Objects.nonNull(source)) {
            return Math.max(source.getCount(), sink.getCount());
        } else {
            return Objects.nonNull(sink) ? sink.getCount() : Objects.nonNull(source) ? source.getCount() : 0;
        }
    }

    private void handleTableStructureDiff(SliceCheckEvent checkEvent) {
        long count = getCheckSliceCount(checkEvent);
        sliceCheckContext.refreshSliceCheckProgress(checkEvent.getSlice(), count);
        CheckDiffResult result = buildSliceDiffResult(checkEvent.getSlice(), (int) count, false,
            "table structure diff");
        sliceCheckContext.addTableStructureDiffResult(checkEvent.getSlice(), result);
    }

    private CheckDiffResult buildSliceDiffResult(SliceVo slice, int count, boolean isTableStructure, String message) {
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        builder.checkMode(ConfigCache.getCheckMode())
            .process(ConfigCache.getValue(ConfigConstants.PROCESS_NO))
            .schema(slice.getSchema())
            .table(slice.getTable())
            .sno(slice.getNo())
            .startTime(LocalDateTime.now())
            .endTime(LocalDateTime.now())
            .isTableStructureEquals(isTableStructure)
            .isExistTableMiss(false, null)
            .rowCount(count)
            .error(message);
        return builder.build();
    }

    private CheckDiffResult buildSliceSuccessResult(SliceVo slice, int count, boolean isTableStructure) {
        CheckDiffResultBuilder builder = CheckDiffResultBuilder.builder();
        builder.checkMode(ConfigCache.getCheckMode())
            .process(ConfigCache.getValue(ConfigConstants.PROCESS_NO))
            .schema(slice.getSchema())
            .table(slice.getTable())
            .sno(slice.getNo())
            .startTime(LocalDateTime.now())
            .endTime(LocalDateTime.now())
            .isTableStructureEquals(isTableStructure)
            .isExistTableMiss(false, null)
            .rowCount(count);
        return builder.build();
    }

    private boolean checkTableStructure(SliceCheckEvent checkEvent) {
        SliceExtend source = checkEvent.getSource();
        SliceExtend sink = checkEvent.getSink();
        if (Objects.nonNull(source) && Objects.nonNull(sink)) {
            return source.getTableHash() == sink.getTableHash();
        } else {
            return false;
        }
    }

    /**
     * handleIgnoreTable
     *
     * @param slice slice
     * @param source source
     * @param sink sink
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
