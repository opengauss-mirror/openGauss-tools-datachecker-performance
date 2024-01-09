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

package org.opengauss.datachecker.check.service;

import org.apache.commons.collections4.CollectionUtils;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.LinkedList;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.BlockingQueue;
import java.util.concurrent.LinkedBlockingQueue;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.TimeUnit;

/**
 * CsvProcessManagement
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
@Component
public class CsvProcessManagement {
    private static final Logger log = LogUtils.getLogger();

    @Resource
    private FeignClientService feignClient;
    private ScheduledExecutorService scheduledExecutor;
    private final BlockingQueue<String> tableDispatcherQueue = new LinkedBlockingQueue<>();

    public void taskDispatcher(List<String> completedTableList) {
        tableDispatcherQueue.addAll(completedTableList);
        log.info("add tables to task dispatcher queue [{}]", completedTableList);
    }

    public void startTaskDispatcher() {
        scheduledExecutor = ThreadUtil.newSingleThreadScheduledExecutor("table-dispatcher");
        int delay = ConfigCache.getIntValue(ConfigConstants.CSV_TASK_DISPATCHER_INTERVAL);
        int maxDispatcherSize = ConfigCache.getIntValue(ConfigConstants.CSV_MAX_DISPATCHER_SIZE);
        scheduledExecutor.scheduleWithFixedDelay(() -> {
            List<String> list = new LinkedList<>();
            while (!tableDispatcherQueue.isEmpty() && list.size() < maxDispatcherSize) {
                list.add(tableDispatcherQueue.poll());
            }
            if (CollectionUtils.isNotEmpty(list)) {
                feignClient.dispatcherTables(list);
                log.info("dispatcher tables to extract service [{}]", list);
            }
        }, delay, delay, TimeUnit.SECONDS);
        log.info("create task dispatcher schedule period [{}] seconds", delay);
    }

    public void closeTaskDispatcher() {
        if (Objects.nonNull(scheduledExecutor)) {
            scheduledExecutor.shutdownNow();
            log.info("shutdown task dispatcher schedule");
        }
    }
}
