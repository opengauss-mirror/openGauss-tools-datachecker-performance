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

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.config.DataCheckConfig;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.service.EndpointMetaDataManager;
import org.opengauss.datachecker.common.entry.check.DataCheckParam;
import org.opengauss.datachecker.common.entry.check.IncrementDataCheckParam;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.concurrent.ThreadPoolExecutor;

import static org.opengauss.datachecker.common.constant.DynamicTpConstant.CHECK_EXECUTOR;

/**
 * DataCheckService
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Service
public class DataCheckService {
    private static final Logger log = LogUtils.getLogger();
    @Resource
    private KafkaProperties kafkaProperties;
    @Resource
    private DataCheckRunnableSupport dataCheckRunnableSupport;
    @Resource
    private DataCheckConfig dataCheckConfig;
    @Resource
    private CheckEnvironment checkEnvironment;
    @Resource
    private EndpointMetaDataManager endpointMetaDataManager;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;

    /**
     * submit check table data runnable
     *
     * @param process                process
     * @param tableName              tableName
     * @param partitions             partitions
     * @param tablePartitionRowCount tablePartitionRowCount
     */
    public void checkTableData(String process, String tableName, int partitions, int tablePartitionRowCount) {
        ThreadPoolExecutor executor = dynamicThreadPoolManager.getExecutor(CHECK_EXECUTOR);
        final int bucketCapacity = dataCheckConfig.getBucketCapacity();
        final int errorRate = dataCheckConfig.getDataCheckProperties().getErrorRate();
        final TableMetadata sourceMeta = endpointMetaDataManager.getTableMetadata(Endpoint.SOURCE, tableName);
        DataCheckParam checkParam = new DataCheckParam();
        checkParam.setProcess(process).setTableName(tableName).setSchema(getSinkSchema()).setSourceMetadata(sourceMeta)
                  .setTablePartitionRowCount(tablePartitionRowCount).setBucketCapacity(bucketCapacity)
                  .setPartitions(partitions).setProperties(kafkaProperties).setErrorRate(errorRate);
        executor.submit(new DataCheckRunnable(checkParam, dataCheckRunnableSupport));
        log.info("add check worker dp-executor : {} , {} , {}", process, tableName, partitions);
    }

    private String getSinkSchema() {
        return checkEnvironment.getDatabase(Endpoint.SINK).getSchema();
    }

    /**
     * incrementCheckTableData
     *
     * @param tableName tableName
     * @param process   process
     * @param dataLog   dataLog
     */
    public Runnable incrementCheckTableData(String tableName, String process, SourceDataLog dataLog) {
        IncrementDataCheckParam checkParam = new IncrementDataCheckParam();
        checkParam.setSchema(getSinkSchema()).setTableName(tableName)
                  .setBucketCapacity(dataCheckConfig.getBucketCapacity()).setDataLog(dataLog).setProcess(process);
        return new IncrementCheckThread(checkParam, dataCheckRunnableSupport);
    }
}
