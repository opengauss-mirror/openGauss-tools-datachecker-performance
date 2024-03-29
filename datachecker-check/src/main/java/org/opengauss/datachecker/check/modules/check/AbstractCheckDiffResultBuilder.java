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

import lombok.Getter;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ConditionLimit;

import java.time.LocalDateTime;
import java.util.Objects;
import java.util.Set;
import java.util.TreeSet;

/**
 * AbstractCheckDiffResultBuilder
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/18
 * @since ：11
 */
@Slf4j
@Getter
public abstract class AbstractCheckDiffResultBuilder<C extends CheckDiffResult, B extends AbstractCheckDiffResultBuilder<C, B>> {
    private static final int MAX_DIFF_REPAIR_SIZE = 5000;

    private String table;
    private int partitions;
    private int rowCount;
    private int errorRate;
    private long beginOffset;
    private String topic;
    private String schema;
    private String process;
    private boolean isTableStructureEquals;
    private boolean isExistTableMiss;
    private Endpoint onlyExistEndpoint;
    private CheckMode checkMode;
    private ConditionLimit conditionLimit;
    private Set<String> keyUpdateSet = new TreeSet<>();
    private Set<String> keyInsertSet = new TreeSet<>();
    private Set<String> keyDeleteSet = new TreeSet<>();
    private LocalDateTime startTime;
    private LocalDateTime endTime;

    /**
     * construct
     */
    public AbstractCheckDiffResultBuilder() {
    }

    /**
     * the builder's own abstract method
     *
     * @return Return the builder's own object
     */
    protected abstract B self();

    /**
     * Execution builder abstract method
     *
     * @return Execution builder
     */
    public abstract C build();

    /**
     * Set the table properties of the builder
     *
     * @param table table name
     * @return CheckDiffResultBuilder
     */
    public B table(String table) {
        this.table = table;
        return self();
    }

    /**
     * Set the process properties of the builder
     *
     * @param process process
     * @return CheckDiffResultBuilder
     */
    public B process(String process) {
        this.process = process;
        return self();
    }

    /**
     * Set the conditionLimit properties of the builder
     *
     * @param conditionLimit conditionLimit
     * @return CheckDiffResultBuilder
     */
    public B conditionLimit(ConditionLimit conditionLimit) {
        this.conditionLimit = conditionLimit;
        return self();
    }

    /**
     * Set the startTime properties of the builder
     *
     * @param startTime startTime
     * @return CheckDiffResultBuilder
     */
    public B startTime(LocalDateTime startTime) {
        this.startTime = startTime;
        return self();
    }

    /**
     * Set the endTime properties of the builder
     *
     * @param endTime endTime
     * @return CheckDiffResultBuilder
     */
    public B endTime(LocalDateTime endTime) {
        this.endTime = endTime;
        return self();
    }

    /**
     * Set the table is TableStructureEquals
     *
     * @param isTableStructureEquals table is TableStructureEquals
     * @return CheckDiffResultBuilder
     */
    public B isTableStructureEquals(boolean isTableStructureEquals) {
        this.isTableStructureEquals = isTableStructureEquals;
        return self();
    }

    /**
     * isExistTableMiss
     *
     * @param isExistTableMiss  table is miss
     * @param onlyExistEndpoint only exist endpoint
     * @return builder
     */
    public B isExistTableMiss(boolean isExistTableMiss, Endpoint onlyExistEndpoint) {
        this.isExistTableMiss = isExistTableMiss;
        this.onlyExistEndpoint = onlyExistEndpoint;
        return self();
    }

    /**
     * Set the topic properties of the builder
     *
     * @param topic topic name
     * @return CheckDiffResultBuilder
     */
    public B topic(String topic) {
        this.topic = topic;
        return self();
    }

    /**
     * Set the schema properties of the builder
     *
     * @param schema schema
     * @return CheckDiffResultBuilder
     */
    public B schema(String schema) {
        this.schema = schema;
        return self();
    }

    /**
     * Set the partitions properties of the builder
     *
     * @param partitions partitions
     * @return CheckDiffResultBuilder
     */
    public B partitions(int partitions) {
        this.partitions = partitions;
        return self();
    }

    public B beginOffset(long beginOffset) {
        this.beginOffset = beginOffset;
        return self();
    }

    public B errorRate(int errorRate) {
        this.errorRate = errorRate;
        return self();
    }

    public B rowCount(int rowCount) {
        this.rowCount = rowCount;
        return self();
    }

    public B checkMode(CheckMode checkMode) {
        this.checkMode = checkMode;
        return self();
    }

    /**
     * Set the keyUpdateSet properties of the builder
     *
     * @param keyUpdateSet keyUpdateSet
     * @return CheckDiffResultBuilder
     */
    public B keyUpdateSet(Set<String> keyUpdateSet) {
        this.keyUpdateSet.addAll(keyUpdateSet);
        return self();
    }

    /**
     * Set the keyInsertSet properties of the builder
     *
     * @param keyInsertSet keyInsertSet
     * @return CheckDiffResultBuilder
     */
    public B keyInsertSet(Set<String> keyInsertSet) {
        this.keyInsertSet.addAll(keyInsertSet);
        return self();
    }

    /**
     * Set the keyDeleteSet properties of the builder
     *
     * @param keyDeleteSet keyDeleteSet
     * @return CheckDiffResultBuilder
     */
    public B keyDeleteSet(Set<String> keyDeleteSet) {
        this.keyDeleteSet.addAll(keyDeleteSet);
        return self();
    }

    /**
     * build CheckDiffResultBuilder
     *
     * @return CheckDiffResultBuilder
     */
    public static CheckDiffResultBuilder builder() {
        return new CheckDiffResultBuilder();
    }

    /**
     * CheckDiffResultBuilder
     */
    public static final class CheckDiffResultBuilder
        extends AbstractCheckDiffResultBuilder<CheckDiffResult, CheckDiffResultBuilder> {
        private CheckDiffResultBuilder() {
        }

        @Override
        protected CheckDiffResultBuilder self() {
            return this;
        }

        @Override
        public CheckDiffResult build() {
            return new CheckDiffResult(this);
        }
    }

    protected boolean isNotLargeDiffKeys() {
        if (Objects.equals(CheckMode.INCREMENT, checkMode)) {
            return true;
        }
        int totalRepair = getKeySetSize(keyDeleteSet) + getKeySetSize(keyInsertSet) + getKeySetSize(keyUpdateSet);
        int curErrorRate = rowCount > 0 ? (totalRepair * 100 / rowCount) : 0;
        if (totalRepair <= MAX_DIFF_REPAIR_SIZE || curErrorRate <= errorRate) {
            return true;
        } else {
            log.info("check table[{}.{}] diff-count={},error-rate={}%, error is too large ,not to build repair dml",
                schema, table, totalRepair, curErrorRate);
            return false;
        }
    }

    protected int getKeySetSize(Set<String> keySet) {
        return keySet == null ? 0 : keySet.size();
    }
}
