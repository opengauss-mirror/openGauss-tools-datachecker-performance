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

package org.opengauss.datachecker.check.modules.check;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;
import org.springframework.util.CollectionUtils;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.CHECKED_DIFF_TOO_LARGE;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.CHECKED_PARTITIONS;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.CHECKED_ROW_CONDITION;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.FAILED_MESSAGE;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.RESULT_FAILED;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.RESULT_SUCCESS;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.STRUCTURE_NOT_EQUALS;
import static org.opengauss.datachecker.check.modules.check.CheckResultConstants.TABLE_NOT_EXISTS;

/**
 * CheckDiffResult
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Data
@JSONType(
    orders = {"process", "schema", "table", "topic", "partitions", "beginOffset", "checkMode", "result", "message",
        "startTime", "endTime", "keyInsertSet", "keyUpdateSet", "keyDeleteSet"},
    ignores = {"totalRepair", "buildRepairDml", "isBuildRepairDml", "rowCondition"})
public class CheckDiffResult {
    private String process;
    private String schema;
    private String table;
    private String topic;
    private int partitions;
    private long beginOffset;
    private int totalRepair;
    private CheckMode checkMode;
    private LocalDateTime startTime;
    private LocalDateTime endTime;
    private String result;
    private String message;
    private ConditionLimit rowCondition;
    private Set<String> keyInsertSet;
    private Set<String> keyUpdateSet;
    private Set<String> keyDeleteSet;

    /**
     * constructor
     */
    public CheckDiffResult() {
    }

    /**
     * constructor
     *
     * @param builder builder
     */
    public CheckDiffResult(final AbstractCheckDiffResultBuilder<?, ?> builder) {
        table = Objects.isNull(builder.getTable()) ? "" : builder.getTable();
        partitions = builder.getPartitions();
        beginOffset = builder.getBeginOffset();
        topic = Objects.isNull(builder.getTopic()) ? "" : builder.getTopic();
        schema = Objects.isNull(builder.getSchema()) ? "" : builder.getSchema();
        process = Objects.isNull(builder.getProcess()) ? "" : builder.getProcess();
        startTime = builder.getStartTime();
        endTime = builder.getEndTime();
        rowCondition = builder.getConditionLimit();
        checkMode = builder.getCheckMode();
        if (builder.isExistTableMiss()) {
            initEmptyCollections();
            resultTableNotExist(builder.getOnlyExistEndpoint());
        } else if (builder.isTableStructureEquals()) {
            keyUpdateSet = builder.getKeyUpdateSet();
            keyInsertSet = builder.getKeyInsertSet();
            keyDeleteSet = builder.getKeyDeleteSet();
            totalRepair = keyUpdateSet.size() + keyInsertSet.size() + keyDeleteSet.size();
            resultAnalysis(builder.isNotLargeDiffKeys());
        } else {
            initEmptyCollections();
            resultTableStructureNotEquals();
        }
    }

    private void initEmptyCollections() {
        keyUpdateSet = new TreeSet<>();
        keyInsertSet = new TreeSet<>();
        keyDeleteSet = new TreeSet<>();
    }

    private void resultTableStructureNotEquals() {
        result = RESULT_FAILED;
        message = STRUCTURE_NOT_EQUALS;
    }

    private void resultTableNotExist(Endpoint onlyExistEndpoint) {
        result = RESULT_FAILED;
        message = String.format(TABLE_NOT_EXISTS, table, onlyExistEndpoint.getDescription());
    }

    private void resultAnalysis(boolean isNotLargeDiffKeys) {
        if (Objects.nonNull(rowCondition)) {
            message =
                String.format(CHECKED_ROW_CONDITION, schema, table, rowCondition.getStart(), rowCondition.getOffset());
        } else {
            message = String.format(CHECKED_PARTITIONS, schema, table, partitions);
        }
        if (CollectionUtils.isEmpty(keyInsertSet) && CollectionUtils.isEmpty(keyUpdateSet) && CollectionUtils
            .isEmpty(keyDeleteSet)) {
            result = RESULT_SUCCESS;
            message += result;
        } else {
            result = RESULT_FAILED;
            message += String.format(FAILED_MESSAGE, keyInsertSet.size(), keyUpdateSet.size(), keyDeleteSet.size());
            if (totalRepair > 0 && !isNotLargeDiffKeys) {
                message += CHECKED_DIFF_TOO_LARGE;
            }
        }
    }
}
