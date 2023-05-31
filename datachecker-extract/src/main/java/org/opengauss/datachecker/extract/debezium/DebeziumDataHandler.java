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

package org.opengauss.datachecker.extract.debezium;

import org.opengauss.datachecker.extract.service.MetaDataService;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DebeziumDataHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
public interface DebeziumDataHandler<E> {
    /**
     * Debezium message parsing and adding the parsing result to the {@code DebeziumDataLogs.class} result set
     *
     * @param offset  offset
     * @param message message
     * @param queue   debeziumDataLogs
     */
    void handler(long offset, @NotEmpty E message, @NotNull LinkedBlockingQueue<DebeziumDataBean> queue);

    /**
     * set schema to debezium data handler
     *
     * @param schema schema
     */
    void setSchema(String schema);

    /**
     * inject MetaDataService Instance
     *
     * @param metaDataService metaDataService
     */
    void injectMetaDataServiceInstanceToHandler(MetaDataService metaDataService);
}
