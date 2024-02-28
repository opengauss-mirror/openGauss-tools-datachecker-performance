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

package org.opengauss.datachecker.extract.config;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import org.hibernate.validator.constraints.Range;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.DataLoad;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Data
@Component
@ConfigurationProperties(prefix = "spring.extract")
@JSONType(orders = {"schema", "dataLoadMode", "databaseType", "endpoint", "isDebeziumEnable", "debeziumTopic",
    "debeziumGroupId", "debeziumSerializer", "debeziumAvroRegistry"})
public class ExtractProperties {
    @NotNull(message = DataLoad.API_DESCRIPTION + "data load mode configuration, must [jdbc/csv] cannot be empty")
    private DataLoad dataLoadMode;

    /**
     * Database instance : database instance configuration,which cannot be empty
     */
    @NotNull(message = "database instance configuration, cannot be empty")
    private String schema;

    /**
     * Database type: the database type configuration cannot be empty
     */
    @NotNull(message = DataBaseType.API_DESCRIPTION + ",database type configuration, cannot be empty")
    private DataBaseType databaseType;

    /**
     * Endpoint type: Endpoint type,which cannot be empty
     */
    @NotNull(message = Endpoint.API_DESCRIPTION + ",endpoint type configuration, cannot be empty")
    private Endpoint endpoint;

    private String debeziumSerializer;
    private String debeziumAvroRegistry;
    private boolean isDebeziumRowDisplay = false;

    /**
     * Indicates whether to enable incremental debezium configuration.
     * By default,this function is disabled. the default value is false.
     */
    @NotNull(message = "whether to enable debezium configuration, which cannot be empty")
    private boolean isDebeziumEnable = false;
    /**
     * Debezium incremental migration verification topic.
     * Debezium listens to incremental data in tables and uses a single topic for incremental data management.
     * The debezium incremental verification configuration is not checked here.
     * If the incremental verification service is required,you need to manually configure it.
     */
    private String debeziumTopic;
    /**
     * Group parameter setting for the debezium incremental migration verification topic
     */
    private String debeziumGroupId;

    private Boolean countRow = true;

    /**
     * openGauss query dop config
     */
    @Range(min = 1, max = 64, message = "invalid openGauss query dop config 1~64 , please check it.")
    private int queryDop = 8;

    private int debeziumTopicPartitions = 1;
    /**
     * incremental migration table name list
     */
    private List<String> debeziumTables;
    /**
     * debezium incremental migration verification period: 24 x 60 (unit:minute)
     */
    private int debeziumTimePeriod = 1;
    /**
     * debezium incremental migration verification statistics incremental change record count threshold.
     * the threshold must be greater than 100.
     */
    private int debeziumNumPeriod = 1000;
    private int debeziumNumDefaultPeriod = 1000;
}
