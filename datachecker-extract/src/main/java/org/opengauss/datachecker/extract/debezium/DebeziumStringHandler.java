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

import com.alibaba.fastjson.JSONObject;
import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.debezium.DebeziumData;
import org.opengauss.datachecker.common.entry.debezium.DebeziumPayload;
import org.opengauss.datachecker.common.entry.debezium.PayloadSource;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.service.MetaDataService;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.Map;
import java.util.concurrent.LinkedBlockingQueue;

/**
 * DebeziumStringHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
public class DebeziumStringHandler implements DebeziumDataHandler<String> {
    private static final Logger log = LogUtils.getLogger();
    private String destSchema;
    private MetaDataService metaDataService;
    private boolean isDisplayRow;

    /**
     * Debezium message parsing and adding the parsing result to the {@code DebeziumDataLogs.class} result set
     *
     * @param offset  offset
     * @param message message
     * @param queue   debeziumDataLogs
     */
    @Override
    public void handler(long offset, @NotEmpty String message, @NotNull LinkedBlockingQueue<DebeziumDataBean> queue) {
        try {
            final DebeziumData debeziumData = JSONObject.parseObject(message, DebeziumData.class);
            final DebeziumPayload payload = debeziumData.getPayload();
            final Map<String, String> before = payload.getBefore();
            final Map<String, String> after = payload.getAfter();
            final PayloadSource source = payload.getSource();
            queue.put(new DebeziumDataBean(source.getTable(), offset, after != null ? after : before));
        } catch (InterruptedException ex) {
            log.error("put message at the tail of this queue, waiting if necessary for space to become available.");
        }
    }

    @Override
    public void setSchema(String schema) {
        this.destSchema = schema;
    }

    @Override
    public void setDebeziumRowDisplay(boolean isDisplayRow) {
        this.isDisplayRow = isDisplayRow;
    }

    @Override
    public void injectMetaDataServiceInstanceToHandler(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }
}
