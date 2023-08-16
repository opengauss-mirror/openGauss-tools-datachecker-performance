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

package org.opengauss.datachecker.check.load;

import org.opengauss.datachecker.common.entry.common.Rule;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.RuleType;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.atomic.AtomicReference;

/**
 * CheckEnvironment
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Service
public class CheckEnvironment {
    private static final Map<Endpoint, Database> EXTRACT_DATABASE = new HashMap<>();
    private static final AtomicReference<CheckMode> CHECK_MODE_REF = new AtomicReference<>();
    private static final Map<RuleType, List<Rule>> RULES = new HashMap<>();
    protected boolean checkTableEmpty;
    private static String exportCheckPath = "";
    private static boolean metaLoading = false;

    private ExecutorService threadPoolExecutor = null;

    /**
     * Set the configuration information related to the endpoint database
     *
     * @param endpoint endpoint
     * @param database database
     */
    protected void addExtractDatabase(Endpoint endpoint, Database database) {
        EXTRACT_DATABASE.put(endpoint, database);
    }

    /**
     * Set metadata loading success
     */
    protected void setMetaLoading() {
        metaLoading = true;
    }

    /**
     * Get whether the metadata is loaded successfully
     *
     * @return true | false
     */
    public boolean isLoadMetaSuccess() {
        return metaLoading;
    }

    /**
     * Get endpoint database information
     *
     * @param endpoint endpoint
     * @return database
     */
    public Database getDatabase(Endpoint endpoint) {
        return EXTRACT_DATABASE.get(endpoint);
    }

    /**
     * Set Verification Mode
     *
     * @param checkMode checkMode
     */
    protected void setCheckMode(CheckMode checkMode) {
        CHECK_MODE_REF.set(checkMode);
    }

    /**
     * Set verification thread pool
     *
     * @param executorService thread pool
     */
    protected void setCheckExecutorService(ExecutorService executorService) {
        this.threadPoolExecutor = executorService;
    }

    /**
     * Set the export path of verification results
     *
     * @param checkPath checkPath
     */
    protected void setExportCheckPath(String checkPath) {
        exportCheckPath = checkPath;
    }

    /**
     * get the export path of verification results
     *
     * @return checkPath
     */
    public String getExportCheckPath() {
        return exportCheckPath;
    }

    /**
     * get  Verification Mode
     *
     * @return CheckMode
     */
    public CheckMode getCheckMode() {
        return CHECK_MODE_REF.get();
    }

    /**
     * get verification thread pool
     *
     * @return thread pool
     */
    public ExecutorService getCheckExecutorService() {
        return threadPoolExecutor;
    }

    public void addRules(Map<RuleType, List<Rule>> rules) {
        RULES.putAll(rules);
    }

    public boolean isCheckTableEmpty() {
        return checkTableEmpty;
    }

    public void setCheckTableEmpty(boolean checkTableEmpty) {
        this.checkTableEmpty = checkTableEmpty;
    }
}
