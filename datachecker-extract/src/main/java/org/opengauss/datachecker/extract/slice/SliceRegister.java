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

package org.opengauss.datachecker.extract.slice;

import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;
import java.util.ArrayList;
import java.util.List;

/**
 * SliceRegister
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/21
 * @since ：11
 */
@Component
public class SliceRegister {
    @Resource
    private CheckingFeignClient checkingClient;

    /**
     * register slice to check service
     *
     * @param sliceList slice
     */
    public void batchRegister(List<SliceVo> sliceList) {
        List<SliceVo> tableSliceTmpList = new ArrayList<>();
        sliceList.forEach(sliceVo -> {
            sliceVo.setEndpoint(ConfigCache.getEndPoint());
            tableSliceTmpList.add(sliceVo);
            if (tableSliceTmpList.size() >= 10) {
                checkingClient.batchRegisterSlice(tableSliceTmpList);
                tableSliceTmpList.clear();
            }
        });
        if (tableSliceTmpList.size() > 0) {
            checkingClient.batchRegisterSlice(tableSliceTmpList);
        }
    }

    /**
     * start table checkpoint monitor
     */
    public void startCheckPointMonitor() {
        checkingClient.startCheckPointMonitor();
    }

    /**
     * stop table checkpoint monitor
     *
     * @param endpoint endpoint
     */
    public void stopCheckPointMonitor(Endpoint endpoint) {
        checkingClient.stopCheckPointMonitor(endpoint);
    }
}