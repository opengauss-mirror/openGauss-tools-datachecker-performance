/*
 * Copyright (c) 2026 Huawei Technologies Co., Ltd.
 * openGauss-tools-datachecker-performance is licensed under Mulan PSL v2.
 * You can use this software according to the terms and conditions of the Mulan PSL v2.
 * You may obtain a copy of Mulan PSL v2 at:
 *          http://license.coscl.org.cn/MulanPSL2
 * THIS SOFTWARE IS PROVIDED ON AN "AS IS" BASIS, WITHOUT WARRANTIES OF ANY KIND,
 * EITHER EXPRESS OR IMPLIED, INCLUDING BUT NOT LIMITED TO NON-INFRINGEMENT,
 * MERCHANTABILITY OR FIT FOR A PARTICULAR PURPOSE.
 * See the Mulan PSL v2 for more details.
 */

package org.opengauss.datachecker.check.modules.task;

import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.util.List;
import java.util.Map;

/**
 * TaskManagerService
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
public interface TaskManagerService {
    /**
     * Refresh the execution status of the data extraction table of the specified task
     *
     * @param tableName tableName
     * @param endpoint  endpoint {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     * @param status    status
     */
    void refreshTableExtractStatus(String tableName, Endpoint endpoint, int status);

    /**
     * Initialize task status
     *
     * @param tableNameList table name list
     */
    void initTableExtractStatus(List<String> tableNameList);

    /**
     * Clean up task status information
     */
    void cleanTaskStatus();

    /**
     * query check status of current table
     *
     * @return status
     */
    Map<String, Integer> queryTableCheckStatus();

    /**
     * check a table that is checked completed
     *
     * @param tableName tableName
     * @return true|false
     */
    boolean isChecked(String tableName);
}
