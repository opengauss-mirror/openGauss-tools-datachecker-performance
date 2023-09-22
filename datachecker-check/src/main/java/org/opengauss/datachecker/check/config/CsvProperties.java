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
import org.opengauss.datachecker.common.entry.csv.CsvPathConfig;
import org.springframework.beans.BeanUtils;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.stereotype.Component;

/**
 * CSV DataSourceConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/23
 * @since ：11
 */
@Data
@Component
@ConfigurationProperties(prefix = "spring.csv")
@JSONType(
    orders = {"sync", "schema", "path", "data", "reader", "writer", "schemaTables", "schemaColumns", "sleepInterval"})
public class CsvProperties {
    private boolean sync;
    private String schema;
    private String path;
    private String data;
    private String reader;
    private String writer;
    private String schemaTables;
    private String schemaColumns;
    private long sleepInterval = 100;

    /**
     * translate properties to csv path config
     *
     * @return csv path config
     */
    public CsvPathConfig translate() {
        CsvPathConfig config = new CsvPathConfig();
        BeanUtils.copyProperties(this, config);
        return config;
    }
}