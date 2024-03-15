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

package org.opengauss.datachecker.check.service.impl;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.check.cache.TableStatusRegister;
import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.check.load.CheckEnvironment;
import org.opengauss.datachecker.check.service.CheckService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.ExtractTask;
import org.opengauss.datachecker.common.exception.CheckingException;
import org.opengauss.datachecker.common.exception.CommonException;
import org.opengauss.datachecker.common.util.JsonObjectUtil;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import javax.annotation.Resource;
import java.time.LocalDateTime;
import java.util.List;
import java.util.Objects;
import java.util.concurrent.atomic.AtomicBoolean;
import java.util.concurrent.atomic.AtomicReference;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Service(value = "checkService")
public class CheckServiceImpl implements CheckService {
    private static final Logger log = LogUtils.getLogger(CheckService.class);
    private static final AtomicBoolean STARTED = new AtomicBoolean(false);
    private static final AtomicBoolean CHECKING = new AtomicBoolean(true);
    private static final AtomicReference<String> PROCESS_SIGNATURE = new AtomicReference<>();
    private static final String START_MESSAGE = "the execution time of %s process is %s";

    @Resource
    private FeignClientService feignClientService;
    @Resource
    private TableStatusRegister tableStatusRegister;
    @Resource
    private CheckEnvironment checkEnvironment;

    /**
     * Enable verification service
     *
     * @param checkMode check Mode
     */
    @Override
    public String start(CheckMode checkMode) {
        Assert.isTrue(checkEnvironment.isLoadMetaSuccess(), "current meta data is loading, please wait a moment");
        LogUtils.info(log, CheckMessage.CHECK_SERVICE_STARTING, checkEnvironment.getCheckMode()
                                                                                .getCode());
        Assert.isTrue(Objects.equals(CheckMode.FULL, checkEnvironment.getCheckMode()),
            "current check mode is " + CheckMode.INCREMENT.getDescription() + " , not start full check.");
        if (STARTED.compareAndSet(false, true)) {
            try {
                startCheckFullMode();
            } catch (CheckingException ex) {
                cleanCheck();
                throw new CheckingException(ex.getMessage());
            }
        } else {
            String message = String.format(CheckMessage.CHECK_SERVICE_START_ERROR, checkMode.getDescription());
            LogUtils.error(log, message);
            cleanCheck();
            throw new CheckingException(message);
        }
        return String.format(START_MESSAGE, PROCESS_SIGNATURE.get(), JsonObjectUtil.formatTime(LocalDateTime.now()));
    }

    interface CheckMessage {
        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_STARTING = "check service is starting, start check mode is [{}]";

        /**
         * Verify the startup message template
         */
        String CHECK_SERVICE_START_ERROR = "check service is running, current check mode is [%s] , exit.";
    }

    /**
     * Turn on full calibration mode
     */
    private void startCheckFullMode() {
        String processNo = ConfigCache.getValue(ConfigConstants.PROCESS_NO);
        // Source endpoint task construction
        final List<ExtractTask> extractTasks = feignClientService.buildExtractTaskAllTables(Endpoint.SOURCE, processNo);
        LogUtils.debug(log, "check full mode : build extract task source {}", processNo);
        // Sink endpoint task construction
        feignClientService.buildExtractTaskAllTables(Endpoint.SINK, processNo, extractTasks);
        LogUtils.debug(log, "check full mode : build extract task sink {}", processNo);

        // Perform all tasks
        feignClientService.execExtractTaskAllTables(Endpoint.SOURCE, processNo);
        feignClientService.execExtractTaskAllTables(Endpoint.SINK, processNo);
        LogUtils.info(log, "check full mode : exec extract task (source and sink ) {}", processNo);
        PROCESS_SIGNATURE.set(processNo);
    }

    /**
     * Query the currently executed process number
     *
     * @return process number
     */
    @Override
    public String getCurrentCheckProcess() {
        return PROCESS_SIGNATURE.get();
    }

    /**
     * Clean up the verification environment
     */
    @Override
    public synchronized void cleanCheck() {
        final String processNo = PROCESS_SIGNATURE.get();
        if (Objects.nonNull(processNo)) {
            cleanBuildTask(processNo);
        }
        ThreadUtil.sleep(3000);
        PROCESS_SIGNATURE.set(null);
        STARTED.set(false);
        CHECKING.set(true);
        LogUtils.info(log, "clear and reset the current verification service!");
    }

    private void cleanBuildTask(String processNo) {
        try {
            feignClientService.cleanEnvironment(Endpoint.SOURCE, processNo);
            feignClientService.cleanEnvironment(Endpoint.SINK, processNo);
        } catch (CommonException ex) {
            LogUtils.error(log, "ignore error:", ex);
        }
        tableStatusRegister.removeAll();
        LogUtils.info(log,
            "The task registry of the verification service clears the data extraction task status information");
    }
}
