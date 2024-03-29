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

package org.opengauss.datachecker.check.client;

import org.opengauss.datachecker.common.entry.common.DistributeRuleEntry;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;
import java.util.Set;

/**
 * ExtractFallbackFactory
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/25
 * @since ：11
 */
@Component
public class ExtractFallbackFactory implements FallbackFactory<ExtractFeignClient> {
    /**
     * Returns an instance of the fallback appropriate for the given cause.
     *
     * @param cause cause of an exception.
     * @return fallback
     */
    @Override
    public ExtractFeignClient create(Throwable cause) {
        return new ExtractFeignClientImpl();
    }

    private class ExtractFeignClientImpl implements ExtractFeignClient {
        @Override
        public Result<Void> health() {
            return Result.error("Remote call service health check exception");
        }

        @Override
        public Result<Map<String, TableMetadata>> queryMetaDataOfSchema() {
            return Result.error("Remote call, endpoint loading metadata information exception");
        }

        @Override
        public Result<List<ExtractTask>> buildExtractTaskAllTables(String processNo) {
            return Result.error("Remote call, extract task construction exception");
        }

        @Override
        public Result<Void> buildExtractTaskAllTables(String processNo, List<ExtractTask> taskList) {
            return Result.error("Remote call, abnormal configuration of the destination extraction task");
        }

        @Override
        public Result<Void> execExtractTaskAllTables(String processNo) {
            return Result.error("Remote call, full extraction business processing process exception");
        }

        @Override
        public Result<List<RowDataHash>> queryTopicData(String tableName, int partitions) {
            return Result.error("Remote call, query the specified topic data exception");
        }

        @Override
        public Result<List<RowDataHash>> queryIncrementTopicData(String tableName) {
            return Result.error("Remote call, query the specified incremental topic data exception");
        }

        @Override
        public Result<Void> cleanEnvironment(String processNo) {
            return Result.error("Remote call, clean up the opposite end environment exception");
        }

        @Override
        public Result<Void> cleanTask() {
            return Result.error("Remote call, clear the task cache exception at the extraction end");
        }

        /**
         * Build repair statements based on parameters
         *
         * @param schema    The corresponding schema of the end DB to be repaired
         * @param tableName table Name
         * @param diffSet   Differential primary key set
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementUpdateDml(String schema, String tableName,
            Set<String> diffSet) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        /**
         * Build repair statements based on parameters
         *
         * @param schema    The corresponding schema of the end DB to be repaired
         * @param tableName table Name
         * @param diffSet   Differential primary key set
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementInsertDml(String schema, String tableName,
            Set<String> diffSet) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        /**
         * Build repair statements based on parameters
         *
         * @param schema    The corresponding schema of the end DB to be repaired
         * @param tableName table Name
         * @param diffSet   Differential primary key set
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementDeleteDml(String schema, String tableName,
            Set<String> diffSet) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        @Override
        public Result<TableMetadataHash> queryTableMetadataHash(String tableName) {
            return Result.error("Remote call, query table metadata hash information exception");
        }

        @Override
        public Result<List<RowDataHash>> querySecondaryCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call, query secondary verification increment log data exception");
        }

        @Override
        public Result<ExtractConfig> getEndpointConfig() {
            return Result.error("Remote call,  Get the current endpoint configuration information, abnormal“");
        }

        @Override
        public Result<Void> startIncrementMonitor() {
            return Result.error("Remote call,  start increment monitor failed ");
        }

        @Override
        public Result<Void> pauseOrResumeIncrementMonitor(boolean parseOrResume) {
            return null;
        }

        @Override
        public Result<Void> distributeRules(DistributeRuleEntry rules) {
            return Result.error("Remote call,  Distribution Filter Rules exception");
        }

        @Override
        public Result<Void> shutdown(String message) {
            return null;
        }

        @Override
        public Result<TableMetadata> queryIncrementMetaData(String tableName) {
            return Result.error("Remote call,  Distribution query Increment MetaData exception");
        }

        @Override
        public Result<Void> notifyCheckTableFinished(String tableName) {
            return Result.error("Remote call, notify Check Table Finished exception");
        }

        @Override
        public Result<Boolean> isCheckTableEmpty(boolean isForced) {
            return Result.error("Remote call, check table empty exception");
        }
    }
}
