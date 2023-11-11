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

import org.opengauss.datachecker.common.constant.WorkerSwitch;
import org.opengauss.datachecker.common.entry.common.GlobalConfig;
import org.opengauss.datachecker.common.entry.common.RepairEntry;
import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.DispatchClientException;
import org.opengauss.datachecker.common.web.Result;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.lang.NonNull;
import org.springframework.stereotype.Service;

import java.util.ArrayList;
import java.util.List;
import java.util.Map;

/**
 * Implement feign client interface call encapsulation
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Service
public class FeignClientService {
    @Autowired
    private ExtractSourceFeignClient extractSourceClient;
    @Autowired
    private ExtractSinkFeignClient extractSinkClient;

    /**
     * Get the specified feign client according to the endpoint type
     *
     * @param endpoint endpoint type
     * @return feignClient
     */
    public ExtractFeignClient getClient(@NonNull Endpoint endpoint) {
        return Endpoint.SOURCE == endpoint ? extractSourceClient : extractSinkClient;
    }

    /**
     * Service health check
     *
     * @param endpoint endpoint type
     * @return Return the corresponding result of the interface
     */
    public Result<Void> health(@NonNull Endpoint endpoint) {
        return getClient(endpoint).health();
    }

    /**
     * Endpoint loading metadata information
     *
     * @param endpoint endpoint type
     * @return Return metadata
     */
    public Map<String, TableMetadata> queryMetaDataOfSchema(@NonNull Endpoint endpoint) {
        Result<Map<String, TableMetadata>> result = getClient(endpoint).queryMetaDataOfSchema();
        if (result.isSuccess()) {
            Map<String, TableMetadata> metadata = result.getData();
            return metadata;
        } else {
            // Exception in scheduling source side service to obtain database metadata information
            throw new DispatchClientException(endpoint,
                "The scheduling source service gets the database metadata information abnormally,"
                    + result.getMessage());
        }
    }

    /**
     * Extraction task construction
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process number
     * @return Return to build task collection
     */
    public List<ExtractTask> buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<List<ExtractTask>> result = getClient(endpoint).buildExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            // Scheduling extraction service construction task exception
            throw new DispatchClientException(endpoint,
                "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    /**
     * Destination extraction task configuration
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process number
     * @param taskList  Source side task list
     * @return Request results
     */
    public boolean buildExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo,
        @NonNull List<ExtractTask> taskList) {
        Result<Void> result = getClient(endpoint).buildExtractTaskAllTables(processNo, taskList);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            // Scheduling extraction service construction task exception
            throw new DispatchClientException(endpoint,
                "The scheduling extraction service construction task is abnormal," + result.getMessage());
        }
    }

    /**
     * Full extraction business processing flow
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process sequence number
     * @return Request results
     */
    public boolean execExtractTaskAllTables(@NonNull Endpoint endpoint, String processNo) {
        Result<Void> result = getClient(endpoint).execExtractTaskAllTables(processNo);
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            // Scheduling extraction service execution task failed
            throw new DispatchClientException(endpoint,
                "Scheduling extraction service execution task failed," + result.getMessage());
        }
    }

    public boolean startIncrementMonitor() {
        Result<Void> result = getClient(Endpoint.SOURCE).startIncrementMonitor();
        if (result.isSuccess()) {
            return result.isSuccess();
        } else {
            // Scheduling extraction service execution task failed
            throw new DispatchClientException(Endpoint.SOURCE, "start increment monitor failed," + result.getMessage());
        }
    }

    public void pauseIncrementMonitor() {
        Result<Void> result = getClient(Endpoint.SOURCE).pauseOrResumeIncrementMonitor(WorkerSwitch.PAUSE);
        if (!result.isSuccess()) {
            // Scheduling extraction service execution task failed
            throw new DispatchClientException(Endpoint.SOURCE, "pause increment monitor failed," + result.getMessage());
        }
    }

    public void resumeIncrementMonitor() {
        Result<Void> result = getClient(Endpoint.SOURCE).pauseOrResumeIncrementMonitor(WorkerSwitch.RESUME);
        if (!result.isSuccess()) {
            // Scheduling extraction service execution task failed
            throw new DispatchClientException(Endpoint.SOURCE,
                "resume increment monitor failed," + result.getMessage());
        }
    }

    /**
     * Clean up the opposite environment
     *
     * @param endpoint  endpoint type
     * @param processNo Execution process sequence number
     */
    public void cleanEnvironment(@NonNull Endpoint endpoint, String processNo) {
        getClient(endpoint).cleanEnvironment(processNo);
    }

    /**
     * Clear the extraction end task cache
     *
     * @param endpoint endpoint type
     */
    public void cleanTask(@NonNull Endpoint endpoint) {
        getClient(endpoint).cleanTask();
    }

    /**
     * Build repair statements based on parameters
     *
     * @param endpoint    endpoint type
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    public List<String> buildRepairStatementInsertDml(Endpoint endpoint, RepairEntry repairEntry) {
        Result<List<String>> result = getClient(endpoint).buildRepairStatementInsertDml(repairEntry);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Build repair statements based on parameters
     *
     * @param endpoint    endpoint type
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    public List<String> buildRepairStatementDeleteDml(Endpoint endpoint, RepairEntry repairEntry) {
        Result<List<String>> result = getClient(endpoint).buildRepairStatementDeleteDml(repairEntry);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Build repair statements based on parameters
     *
     * @param endpoint    endpoint type
     * @param repairEntry repairEntry
     * @return Return to repair statement collection
     */
    public List<String> buildRepairStatementUpdateDml(Endpoint endpoint, RepairEntry repairEntry) {
        Result<List<String>> result = getClient(endpoint).buildRepairStatementUpdateDml(repairEntry);
        if (result.isSuccess()) {
            return result.getData();
        } else {
            return new ArrayList<>();
        }
    }

    /**
     * Query the schema information of the extraction end database
     *
     * @param endpoint endpoint type
     * @return schema
     */
    public ExtractConfig getEndpointConfig(Endpoint endpoint) {
        Result<ExtractConfig> result = null;
        try {
            result = getClient(endpoint).getEndpointConfig();
            if (result.isSuccess()) {
                return result.getData();
            } else {
                return null;
            }
        } catch (Exception exception) {
            return null;
        }
    }

    public void distributeConfig(CheckMode checkMode, Map<RuleType, List<Rule>> rules) {
        GlobalConfig config = new GlobalConfig();
        config.setRules(rules);
        config.setCheckMode(checkMode);
        getClient(Endpoint.SOURCE).distributeConfig(config);
        getClient(Endpoint.SINK).distributeConfig(config);
    }

    public void distributeConfig(CsvPathConfig config) {
        getClient(Endpoint.SOURCE).distributeConfig(config);
        getClient(Endpoint.SINK).distributeConfig(config);
    }

    public void shutdown(String message) {
        List.of(Endpoint.SOURCE, Endpoint.SINK)
            .parallelStream()
            .forEach(endpoint -> {
                shutdown(endpoint, message);
            });
    }

    private void shutdown(Endpoint endpoint, String message) {
        try {
            getClient(endpoint).shutdown(message);
        } catch (Exception ignored) {
        }
    }

    /**
     * queryIncrementMetaData
     *
     * @param endpoint  endpoint
     * @param tableName tableName
     * @return TableMetadata
     */
    public TableMetadata queryIncrementMetaData(Endpoint endpoint, String tableName) {
        try {
            Result<TableMetadata> result = getClient(endpoint).queryIncrementMetaData(tableName);
            if (result.isSuccess()) {
                return result.getData();
            } else {
                throw new CheckingException("query metadata failed: " + endpoint.getDescription() + " : " + tableName);
            }
        } catch (Exception ignored) {
            throw new DispatchClientException(endpoint, "get table metadata error: " + ignored.getMessage());
        }
    }

    public void notifyCheckTableFinished(Endpoint endpoint, String tableName) {
        try {
            getClient(endpoint).notifyCheckTableFinished(tableName);
        } catch (Exception ignored) {
            throw new DispatchClientException(endpoint, "notify error: " + ignored.getMessage());
        }
    }

    public boolean isCheckTableEmpty(Endpoint endpoint, boolean isForced) {
        try {
            Result<Boolean> result = getClient(endpoint).isCheckTableEmpty(isForced);
            if (result.isSuccess()) {
                return result.getData();
            } else {
                throw new CheckingException("check database failed: " + endpoint.getDescription());
            }
        } catch (Exception ignored) {
            throw new DispatchClientException(endpoint, "check database error: " + ignored.getMessage());
        }
    }

    public boolean checkTargetOgCompatibility() {
        try {
            Result<Boolean> result = getClient(Endpoint.SINK).checkTargetOgCompatibility();
            if (result.isSuccess()) {
                return result.getData();
            } else {
                throw new CheckingException("check target OgCompatibility failed: ");
            }
        } catch (Exception ignored) {
            throw new DispatchClientException(Endpoint.SINK,
                "check target OgCompatibility failed: " + ignored.getMessage());
        }
    }

    public void enableCsvExtractService() {
        getClient(Endpoint.SOURCE).enableCsvExtractService();
        getClient(Endpoint.SINK).enableCsvExtractService();
    }

    public int fetchCheckTableCount() {
        int source = fetchCheckTableCount(Endpoint.SOURCE);
        int sink = fetchCheckTableCount(Endpoint.SINK);
        return Math.max(source, sink);
    }

    public int fetchCheckTableCount(Endpoint endpoint) {
        try {
            Result<Integer> result = getClient(endpoint).fetchCheckTableCount();
            if (result.isSuccess()) {
                return result.getData();
            } else {
                throw new CheckingException("fetchCheckTableCount failed: " + Endpoint.SOURCE);
            }
        } catch (Exception ignored) {
            throw new DispatchClientException(Endpoint.SOURCE, "check database error: " + ignored.getMessage());
        }
    }
}
