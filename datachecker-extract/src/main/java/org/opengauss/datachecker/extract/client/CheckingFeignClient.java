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

package org.opengauss.datachecker.extract.client;

import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.entry.extract.SourceDataLog;
import org.opengauss.datachecker.common.entry.extract.Topic;
import org.springframework.cloud.openfeign.FeignClient;
import org.springframework.lang.NonNull;
import org.springframework.web.bind.annotation.GetMapping;
import org.springframework.web.bind.annotation.PostMapping;
import org.springframework.web.bind.annotation.RequestBody;
import org.springframework.web.bind.annotation.RequestParam;

import javax.validation.constraints.NotEmpty;
import java.util.List;
import java.util.Map;

/**
 * <pre>
 * create an internal class to declare the API interface of the called party. If the API of the called party is
 * abnormal ,the exception class is called back for exception declaration.
 *
 * The value can be declared in name. The datachecker-check is the service name and directly invokes the system.
 * Generally,the name uses the Eureka registration information. The Eureka is not introduced.
 * The URL is configured for invoking.
 * </pre>
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@FeignClient(name = "datachecker-check", url = "${spring.check.server-uri}")
public interface CheckingFeignClient {

    /**
     * Refresh the execution status of the data extraction table of a specified task.
     *
     * @param tableName table name
     * @param endpoint  endpoint enum type {@link org.opengauss.datachecker.common.entry.enums.Endpoint}
     * @param status    status
     */
    @PostMapping("/table/extract/status")
    void refreshTableExtractStatus(@RequestParam(value = "tableName") @NotEmpty String tableName,
        @RequestParam(value = "endpoint") @NonNull Endpoint endpoint, @RequestParam(value = "status") int status);

    /**
     * Incremental verification log notification
     *
     * @param dataLogList Incremental verification log
     */
    @PostMapping("/notify/source/increment/data/logs")
    void notifySourceIncrementDataLogs(@RequestBody @NotEmpty List<SourceDataLog> dataLogList);

    /**
     * register topic
     *
     * @param table           tableName
     * @param ptnNum ptnNum
     * @param endpoint        current endpoint
     * @return topic
     */
    @PostMapping("/register/topic")
    Topic registerTopic(@RequestParam(value = "tableName") @NotEmpty String table,
        @RequestParam(value = "ptnNum") int ptnNum,
        @RequestParam(value = "endpoint") @NonNull Endpoint endpoint);

    /**
     * health check
     */
    @GetMapping("/check/health")
    void health();

    /**
     * query check status of all table
     *
     * @return table status
     */
    @GetMapping("/query/all/table/status")
    Map<String, Integer> queryTableCheckStatus();

    @GetMapping("/get/feign/request")
    boolean getFeignRequest(@RequestParam(value = "requestName") String requestName, @RequestParam(value = "value") String value);

    @GetMapping("/release/feign/request")
    boolean releaseFeignRequest(@RequestParam(value = "requestName") String requestName);

    /**
     * register slice
     *
     * @param sliceVo sliceVo
     */
    @PostMapping("/register/slice")
    void registerSlice(@RequestBody SliceVo sliceVo);

    /**
     * register slice
     *
     * @param sliceExt sliceExt
     */
    @PostMapping("/update/register/slice")
    void refreshRegisterSlice(@RequestBody SliceExtend sliceExt);

    @PostMapping("/notify/dispatch/csv/slice/finished")
    void notifyDispatchCsvSliceFinished();

    /**
     * start table checkpoint monitor
     */
    @GetMapping("/register/checkpoint/monitor/start")
    void startCheckPointMonitor();

    /**
     * stop table checkpoint monitor
     *
     * @param endpoint endpoint
     */
    @GetMapping("/register/checkpoint/monitor/stop")
    void stopCheckPointMonitor(@RequestParam(value = "endpoint") Endpoint endpoint);

    /**
     * register table checkpoint list
     *
     * @param endpoint endpoint
     * @param tableName tableName
     * @param checkPoint checkPoint
     */
    @PostMapping("/register/checkpoint")
    void registerCheckPoint(@RequestParam(name = "endpoint") @NotEmpty Endpoint endpoint,
                            @RequestParam(name = "tableName") @NotEmpty String tableName,
                            @RequestBody List<Object> checkPoint);
}