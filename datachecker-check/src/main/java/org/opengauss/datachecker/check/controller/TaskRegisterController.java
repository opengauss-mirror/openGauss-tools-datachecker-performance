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

package org.opengauss.datachecker.check.controller;

import org.opengauss.datachecker.check.service.CheckPointRegister;
import org.opengauss.datachecker.check.service.TaskRegisterCenter;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;
import org.springframework.web.bind.annotation.RestController;

import javax.annotation.Resource;

/**
 * TaskRegisterController
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/25
 * @since ：11
 */
@RestController
public class TaskRegisterController {
    @Resource
    private TaskRegisterCenter taskRegisterCenter;
    @Resource
    private CheckPointRegister checkPointRegister;

    /**
     * register slice ,when extract endpoint listener slice log
     *
     * @param slice slice
     */
    @PostMapping("/register/slice")
    public void registerSlice(@RequestBody SliceVo slice) {
        taskRegisterCenter.register(slice);
    }

    /**
     * update slice extract progress, when extract endpoint fetch slice data from jdbc/csv completed.
     *
     * @param sliceExt slice extend info
     */
    @PostMapping("/update/register/slice")
    public void refreshRegisterSlice(@RequestBody SliceExtend sliceExt) {
        taskRegisterCenter.update(sliceExt);
    }

    /**
     * start table checkpoint monitor
     */
    @GetMapping("/register/checkpoint/monitor/start")
    public void startCheckPointMonitor() {
        checkPointRegister.startMonitor();
    }

    /**
     * stop table checkpoint monitor
     *
     * @param endpoint endpoint
     */
    @GetMapping("/register/checkpoint/monitor/stop")
    public void stopCheckPointMonitor(@RequestParam(value = "endpoint") Endpoint endpoint) {
        checkPointRegister.stopMonitor(endpoint);
    }
}
