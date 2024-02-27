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

package org.opengauss.datachecker.extract.service;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.slice.SliceDispatcher;
import org.opengauss.datachecker.extract.slice.SliceRegister;
import org.opengauss.datachecker.extract.data.BaseDataService;
import org.opengauss.datachecker.extract.data.csv.CsvListener;
import org.opengauss.datachecker.extract.data.csv.CsvReaderListener;
import org.opengauss.datachecker.extract.data.csv.CsvWriterListener;
import org.opengauss.datachecker.extract.slice.TableDispatcher;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;

/**
 * CsvManagementService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
@Service
public class CsvManagementService {
    private static final Logger log = LogUtils.getLogger();

    @Resource
    private BaseDataService baseDataService;
    @Resource
    private MetaDataService metaDataService;
    @Resource
    private DynamicThreadPoolManager dynamicThreadPoolManager;
    @Resource
    private SliceRegister sliceRegister;
    @Resource
    private CheckingFeignClient checkingClient;
    private CsvListener listener;
    private SliceDispatcher sliceDispatcher = null;
    private Endpoint currentEndpoint;

    /**
     * start csv process.
     * if endpoint is source , then start reader listener,
     * if endpoint is sink, then start writer listener.
     */
    public void startCsvProcess() {
        // init dynamic thread pool monitor
        dynamicThreadPoolManager.dynamicThreadPoolMonitor();
        currentEndpoint = ConfigCache.getValue(ConfigConstants.ENDPOINT, Endpoint.class);
        // start listener of reader or writer logs
        if (Objects.equals(Endpoint.SOURCE, currentEndpoint)) {
            // load check table list
            listener = new CsvReaderListener();
        } else {
            listener = new CsvWriterListener();
        }
        listener.initCsvListener(checkingClient);
        // start slice dispatcher core thread
        sliceDispatcher = new SliceDispatcher(dynamicThreadPoolManager, sliceRegister, baseDataService, listener);
        Thread thread = new Thread(sliceDispatcher);
        thread.start();
    }

    public void startCsvNoSliceLogProcess() {
        dynamicThreadPoolManager.dynamicThreadPoolMonitor();
        TableDispatcher tableDispatcher = new TableDispatcher(dynamicThreadPoolManager, sliceRegister, baseDataService);
        Thread thread = new Thread(tableDispatcher);
        thread.start();
    }

    public void close() {
        if (Objects.nonNull(listener)) {
            listener.stop();
        }
        dynamicThreadPoolManager.closeDynamicThreadPoolMonitor();
        if (Objects.nonNull(sliceDispatcher)) {
            sliceDispatcher.stop();
        }
    }

    public void dispatcherTables(List<String> list) {
        sliceDispatcher.addSliceTables(list);
    }

    /**
     * fetchCheckTableCount
     *
     * @return table count
     */
    public int fetchCheckTableCount() {
        Map<String, TableMetadata> tableMetadataMap = metaDataService.mdsQueryMetaDataOfSchema();
        currentEndpoint = ConfigCache.getValue(ConfigConstants.ENDPOINT, Endpoint.class);
        return Objects.equals(Endpoint.SOURCE, currentEndpoint) ? tableMetadataMap.size() : 0;
    }
}
