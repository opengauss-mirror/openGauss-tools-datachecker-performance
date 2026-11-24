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

package org.opengauss.datachecker.extract.debezium;

import org.opengauss.datachecker.common.entry.check.IncrementCheckConfig;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;

import java.util.List;

/**
 * Debezium incremental log data merge service
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
public interface DataConsolidationService {
    /**
     * Get the topic records of debezium, and analyze and merge the topic records
     *
     * @param fetchOffset fetchOffset
     * @return topic records
     */
    List<SourceDataLog> getDebeziumTopicRecords(int fetchOffset);

    /**
     * initIncrementConfig
     */
    void initIncrementConfig();

    /**
     * Get the debezium listening table and record the offset information of the message corresponding to the topic
     *
     * @return return offset
     */
    int getDebeziumTopicRecordEndOffSet();

    /**
     * Check whether the current extraction end is the source end
     *
     * @return Is it the source end
     */
    boolean isSourceEndpoint();

    /**
     * Configure incremental verification (debezium configuration)
     *
     * @param config Incremental verification (debezium configuration)
     */
    void configIncrementCheckEnvironment(IncrementCheckConfig config);

    void pauseOrResumeIncrementMonitor(Boolean pauseOrResume);
}
