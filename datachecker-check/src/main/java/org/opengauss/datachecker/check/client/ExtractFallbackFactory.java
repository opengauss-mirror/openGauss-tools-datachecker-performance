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

import org.opengauss.datachecker.common.entry.common.GlobalConfig;
import org.opengauss.datachecker.common.entry.common.Health;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.PageExtract;
import org.opengauss.datachecker.common.entry.extract.RowDataHash;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.entry.extract.TableMetadataHash;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.cloud.openfeign.FallbackFactory;
import org.springframework.stereotype.Component;

import java.util.List;
import java.util.Map;

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
        public Result<Boolean> checkTargetOgCompatibility() {
            return Result.error("Remote call, endpoint checkTargetOgCompatibility error");
        }

        @Override
        public Result<Health> health() {
            return Result.error("Remote call service health check exception");
        }

        @Override
        public Result<Map<String, TableMetadata>> queryMetaDataOfSchema(PageExtract pageExtract) {
            return Result.error("Remote call, endpoint loading page metadata information exception");
        }

        @Override
        public Result<PageExtract> getExtractMetaPageInfo() {
            return Result.error("Remote call, endpoint loading PageExtract information exception");
        }

        @Override
        public Result<PageExtract> buildExtractTaskAllTables(String processNo) {
            return Result.error("Remote call, extract task construction exception");
        }

        @Override
        public Result<List<ExtractTask>> fetchExtractTaskPageTables(PageExtract pageExtract) {
            return Result.error("Remote call, extract task construction exception");
        }

        @Override
        public Result<Void> dispatchSinkExtractTaskPage(List<ExtractTask> taskList) {
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
         * @param repairEntry repairEntry
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementUpdateDml(RepairEntry repairEntry) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        /**
         * Build repair statements based on parameters
         *
         * @param repairEntry repairEntry
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementInsertDml(RepairEntry repairEntry) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        /**
         * Build repair statements based on parameters
         *
         * @param repairEntry repairEntry
         * @return Return to repair statement collection
         */
        @Override
        public Result<List<String>> buildRepairStatementDeleteDml(RepairEntry repairEntry) {
            return Result.error("Remote call, build and repair statement exceptions according to parameters");
        }

        /**
         * querySourceTableMetadataHash
         *
         * @param tableName tableName
         * @return error
         */
        @Override
        public Result<TableMetadataHash> querySourceTableMetadataHash(String tableName) {
            return Result.error("Remote call failed");
        }

        /**
         * querySinkTableMetadataHash
         *
         * @param tableName tableName
         * @return error
         */
        @Override
        public Result<TableMetadataHash> querySinkTableMetadataHash(String tableName) {
            return Result.error("Remote call failed");
        }

        /**
         * querySourceCheckRowData
         *
         * @param dataLog data Log
         * @return error
         */
        @Override
        public Result<List<RowDataHash>> querySourceCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call failed");
        }

        /**
         * querySinkCheckRowData
         *
         * @param dataLog data Log
         * @return error
         */
        @Override
        public Result<List<RowDataHash>> querySinkCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call failed");
        }

        /**
         * querySourceSecondaryCheckRowData
         *
         * @param dataLog data Log
         * @return error
         */
        @Override
        public Result<List<RowDataHash>> querySourceSecondaryCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call failed");
        }

        /**
         * querySinkSecondaryCheckRowData
         *
         * @param dataLog data Log
         * @return error
         */
        @Override
        public Result<List<RowDataHash>> querySinkSecondaryCheckRowData(SourceDataLog dataLog) {
            return Result.error("Remote call failed");
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
        public Result<Void> distributeConfig(GlobalConfig config) {
            return Result.error("Remote call,  Distribution config exception");
        }

        @Override
        public Result<Void> distributeConfig(CsvPathConfig config) {
            return Result.error("Remote call,  Distribution csv config exception");
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
        public Result<Boolean> isCheckTableEmpty(boolean isForced) {
            return Result.error("Remote call, check table empty exception");
        }

        @Override
        public Result<Void> enableCsvExtractService() {
            return Result.error("Remote call, start csv extract exception");
        }

        @Override
        public Result<Integer> fetchCsvCheckTableCount() {
            return null;
        }

        @Override
        public void dispatcherTables(List<String> list) {
        }
    }
}
