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

import org.opengauss.datachecker.check.client.FeignClientService;
import org.springframework.stereotype.Component;

import javax.annotation.Resource;

/**
 * CsvProcessManagement
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
@Component
public class CsvProcessManagement {
    @Resource
    private FeignClientService feignClient;

    /**
     * csv process management, start csv extract process<br>
     * listener slice task status,source and sink callback slice info and status<br>
     * check slice data (match source and sink slice success,then checked it  )<br>
     * summary slice check result, refresh process log<br>
     * summary check result, refresh process log<br>
     */
    public void start() {
        feignClient.enableCsvExtractService();
    }
}
