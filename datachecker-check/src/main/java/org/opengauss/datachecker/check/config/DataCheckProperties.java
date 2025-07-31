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

package org.opengauss.datachecker.check.config;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.hibernate.validator.constraints.Range;
import org.opengauss.datachecker.common.exception.CheckingAddressConflictException;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;
import org.springframework.validation.annotation.Validated;

import javax.annotation.PostConstruct;
import jakarta.validation.constraints.NotEmpty;
import java.util.Objects;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
@Validated
@Component
@ConfigurationProperties(prefix = "data.check")
@JSONType(orders = {"sourceUri", "sinkUri", "bucketExpectCapacity", "healthCheckApi", "dataPath", "blackWhiteMode",
    "incrementMaxDiffCount"})
public class DataCheckProperties {

    @PostConstruct
    private void checkUrl() {
        if (Objects.equals(sourceUri, sinkUri)) {
            // The access addresses of the source end and the destination end conflict, please reconfigure.
            throw new CheckingAddressConflictException(
                "The access addresses of the source end and the destination end conflict, please reconfigure.");
        }
    }

    @Range(min = 10, max = 5000, message = "The max diff count scop is [10,5000]")
    private int incrementMaxDiffCount = 10;

    /**
     * Data verification service address: the source address cannot be empty
     */
    @NotEmpty(message = "Source address cannot be empty")
    private String sourceUri;

    /**
     * Data verification service address: the destination address cannot be empty ")
     */
    @NotEmpty(message = "The destination address cannot be empty")
    private String sinkUri;

    /**
     * Bucket capacity default capacity size is 1
     */
    @Range(min = 1, message = "The minimum barrel capacity is 1")
    private int bucketExpectCapacity = 1;

    /**
     * Health check address
     */
    private String healthCheckApi;
    /**
     * The root directory of data results and the root directory of data verification results cannot be empty
     */
    @NotEmpty(message = "The root directory of data verification results cannot be empty")
    private String dataPath;
    /**
     * statistical-enable : Configure whether to perform verification time statistics.
     * If true, the execution time of the verification process will be statistically analyzed automatically.
     */
    private boolean canStatisticalEnable;
    /**
     * auto-clean-environment： Configure whether to automatically clean the execution environment.
     * If set to true, the environment will be cleaned automatically after the full verification process is completed.
     */
    private boolean canAutoCleanEnvironment;

    /**
     * 0 is not delete; 1 is delete when checked all completed ; 2 is deleted when checked a table
     */
    @Range(min = 0, max = 2, message = "config auto delete topic not valid")
    private int autoDeleteTopic;

    private int errorRate;
}
