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

import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.check.slice.SliceCheckContext;
import org.opengauss.datachecker.check.slice.SliceCheckEventHandler;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.service.DynamicThreadPoolManager;
import org.opengauss.datachecker.common.service.ProcessLogService;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.springframework.scheduling.annotation.Async;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;

/**
 * AsyncCheckStarter
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/29
 * @since ：11
 */
@Service
public class AsyncCheckStarter {
    private CheckEnvironment environment;
    @Resource
    private ProcessLogService processLogService;

    /**
     * load check environment
     *
     * @param checkEnvironment checkEnvironment
     */
    public void initCheckEnvironment(CheckEnvironment checkEnvironment) {
        this.environment = checkEnvironment;
        // load
        HeartBeatStartLoader heartBeat = SpringUtil.getBean(HeartBeatStartLoader.class);
        heartBeat.load(environment);

        CheckResultPathLoader resultPath = SpringUtil.getBean(CheckResultPathLoader.class);
        resultPath.load(environment);

        CheckDatabaseLoader database = SpringUtil.getBean(CheckDatabaseLoader.class);
        database.load(environment);

        CheckModeLoader checkMode = SpringUtil.getBean(CheckModeLoader.class);
        checkMode.load(environment);

        CheckConfigDistributeLoader checkRule = SpringUtil.getBean(CheckConfigDistributeLoader.class);
        checkRule.load(environment);

        SliceCheckEventHandler sliceCheckEventHandler = SpringUtil.getBean(SliceCheckEventHandler.class);
        SliceCheckContext sliceCheckContext = SpringUtil.getBean(SliceCheckContext.class);
        TaskRegisterCenter registerCenter = SpringUtil.getBean(TaskRegisterCenter.class);
        DynamicThreadPoolManager dynamicThreadPoolManager = SpringUtil.getBean(DynamicThreadPoolManager.class);
        sliceCheckEventHandler.initSliceCheckEventHandler(dynamicThreadPoolManager, sliceCheckContext, registerCenter);
    }

    /**
     * start check
     */
    @Async
    public void start() {
        CheckMode checkMode = environment.getCheckMode();
        switch (checkMode) {
            case INCREMENT:
                checkedIncrement();
                return;
            case CSV:
                checkedCsv();
                return;
            default:
                checkedFullMode();
        }
    }

    private void checkedCsv() {
        CheckStartCsvLoader checkStartCsv = SpringUtil.getBean(CheckStartCsvLoader.class);
        checkStartCsv.load(environment);
    }

    private void checkedIncrement() {
        CheckStartIncrementLoader checkStartIncrement = SpringUtil.getBean(CheckStartIncrementLoader.class);
        checkStartIncrement.load(environment);
    }

    private void checkedFullMode() {
        processLogService.saveProcessLog();
        EmptyDataBaseCheckLoader emptyDatabase = SpringUtil.getBean(EmptyDataBaseCheckLoader.class);
        emptyDatabase.load(environment);
        MetaDataLoader metadata = SpringUtil.getBean(MetaDataLoader.class);
        metadata.load(environment);
        CheckStartLoader checkStart = SpringUtil.getBean(CheckStartLoader.class);
        checkStart.load(environment);
    }
}
