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

package org.opengauss.datachecker.check.client;

import org.springframework.cloud.openfeign.FeignClient;

/**
 * <pre>
 * Create an internal class and declare the API interface of the callee. If the interface of the callee is abnormal,
 * the exception class will be called back for exception declaration
 * Name can declare value,datachecker-extract-sink refers to the service name that directly calls the system.
 * The name usually adopts Eureka registration information. We have not introduced eurka, and configure URL to call
 * ExtractSinkFallBack .
 * This is the class that needs to fuse and prompt error information if the fegin call fails
 * <pre/>
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@FeignClient(name = "datachecker-extract-sink", url = "${data.check.sink-uri}",
    fallbackFactory = ExtractFallbackFactory.class)
public interface ExtractSinkFeignClient extends ExtractFeignClient {

}