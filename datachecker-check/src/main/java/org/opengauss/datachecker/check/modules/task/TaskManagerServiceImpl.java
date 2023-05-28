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

package org.opengauss.datachecker.check.modules.task;

import com.alibaba.fastjson.JSON;
import lombok.extern.slf4j.Slf4j;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Service;

import java.util.HashSet;
import java.util.List;
import java.util.Map;

import static org.opengauss.datachecker.check.cache.TableStatusRegister.TASK_STATUS_CONSUMER_VALUE;

/**
 * TaskManagerServiceImpl
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@Slf4j
@Service
public class TaskManagerServiceImpl implements TaskManagerService {
    @Autowired
    private TableStatusRegister tableStatusRegister;

    /**
     * Refresh the execution status of the data extraction table of the specified task
     *
     * @param tableName tableName
     * @param endpoint  endpoint {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     * @param status    status
     */
    @Override
    public void refreshTableExtractStatus(String tableName, Endpoint endpoint, int status) {
        tableStatusRegister.update(tableName, status);
        log.debug("refreshes extract status: [{} : {} -> {}]  ", endpoint.getDescription(), tableName, status);
    }

    /**
     * Initialize task status
     *
     * @param tableNameList table name list
     */
    @Override
    public void initTableExtractStatus(List<String> tableNameList) {
        if (tableStatusRegister.isEmpty() || tableStatusRegister.isCheckCompleted()) {
            cleanTaskStatus();
            tableStatusRegister.init(new HashSet<>(tableNameList));
            log.info("check server init extract tableNameList=[{}] ", tableNameList.size());
        } else {
            // The last verification process is being executed,
            // and the table verification status data cannot be reinitialized!
            throw new CheckingException("The last verification process is being executed,"
                + " and the table verification status data cannot be reinitialized!");
        }
    }

    /**
     * Clean up task status information
     */
    @Override
    public void cleanTaskStatus() {
        tableStatusRegister.removeAll();
    }

    /**
     * query check status of current table
     *
     * @return status
     */
    @Override
    public Map<String, Integer> queryTableCheckStatus() {
        return tableStatusRegister.get();
    }

    @Override
    public boolean isChecked(String tableName) {
        return tableStatusRegister.get(tableName) == TASK_STATUS_CONSUMER_VALUE;
    }
}
