/*
 * Copyright (c) Huawei Technologies Co.,Ltd. 2023. All rights reserved.
 */
package org.opengauss.datachecker.common.util;

import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.util.stream.IntStream;

/**
 * Title: the TaskUtilHelper class.
 * <p>
 * Description:
 *
 * @author justbk
 * @version [Tools 0.0.1, 2023/6/18]
 * @since 2023/6/18
 */
public class TaskUtilHelper {
    private static final float TABLE_ROWS_DEVIATION_RATE = 1.3f;
    private TableMetadata tableMetadata;
    private int maxLimitRowCount;
    
    public TaskUtilHelper(TableMetadata tableMetadata, int maxLimitRowCount) {
        this.tableMetadata = tableMetadata;
        this.maxLimitRowCount = maxLimitRowCount;
    }
    
    public int[][] calcAutoTaskOffset() {
        long tableRows = tableMetadata.getTableRows();
        if (tableRows < maxLimitRowCount) {
            return new int[][] {{0, maxLimitRowCount}};
        }
        long calcTableRow = tableMetadata.canUseBetween() ? tableMetadata.getMaxTableId() : tableRows;
        return oldCalcAutoTaskOffset(calcTableRow, maxLimitRowCount);
        
    }
    
    
    private float getTableRowsDeviationRate() {
        return tableMetadata.canUseBetween() ? 1.01f : TABLE_ROWS_DEVIATION_RATE;
    }
    
    private int[][] oldCalcAutoTaskOffset(long tableRows, int maxLimitRowCount) {
        if (tableRows <= maxLimitRowCount) {
            return new int[][] {{0, maxLimitRowCount}};
        }
        final int taskCount = (int) Math.round(tableRows * getTableRowsDeviationRate() / maxLimitRowCount) + 1;
        int[][] taskOffset = new int[taskCount][2];
        IntStream.range(0, taskCount).forEach(taskCountIdx -> {
            int start = taskCountIdx * maxLimitRowCount;
            taskOffset[taskCountIdx] = new int[] {start, maxLimitRowCount};
        });
        if (taskOffset.length > 1) {
            int[] lastOffset = taskOffset[taskOffset.length - 1];
            taskOffset[taskOffset.length - 1] = taskOffset[0];
            taskOffset[0] = lastOffset;
        }
        return taskOffset;
    }
}
