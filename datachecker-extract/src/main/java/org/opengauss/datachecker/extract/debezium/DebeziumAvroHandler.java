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
import com.alibaba.fastjson.TypeReference;
import lombok.extern.slf4j.Slf4j;
import org.apache.avro.generic.GenericData;
import org.apache.avro.generic.GenericData.Record;
import org.apache.commons.lang3.StringUtils;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.MetaDataUtil;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;
import java.util.HashMap;
import java.util.Map;
import java.util.Objects;
import java.util.concurrent.LinkedBlockingQueue;

import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.AVRO_FIELD_BEFORE;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.AVRO_FIELD_AFTER;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.AVRO_FIELD_SOURCE;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.AVRO_FIELD_TABLE;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.AVRO_FIELD_DB;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.DDL;
import static org.opengauss.datachecker.extract.debezium.DebeziumAvroHandler.MessageConstants.TRANSACTION_STATUS;

/**
 * DebeziumAvroHandler
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/24
 * @since ：11
 */
@Slf4j
public class DebeziumAvroHandler implements DebeziumDataHandler<GenericData.Record> {
    private String destSchema;
    private MetaDataService metaDataService;

    /**
     * Debezium message parsing and adding the parsing result to the {@code DebeziumDataLogs.class} result set
     *
     * @param offset  offset
     * @param message message
     * @param queue   debeziumDataLogs
     */
    @Override
    public void handler(long offset, @NotEmpty GenericData.Record message,
        @NotNull LinkedBlockingQueue<DebeziumDataBean> queue) {
        try {
            if (isTransactionMessage(message)) {
                log.warn("transaction Message is ignored :  {}", message.toString());
                return;
            }
            final Map<String, String> source = parseRecordData(message, AVRO_FIELD_SOURCE);
            final String table = source.get(AVRO_FIELD_TABLE);
            if (isDdlMessage(message)) {
                log.warn("ddl Message is ignored :  {}", message.toString());
                if (StringUtils.isNotEmpty(table)) {
                    refreshMetadataCache(table);
                }
                return;
            }
            if (StringUtils.isEmpty(table)) {
                log.warn("table is empty, Message is ignored :  {}", message.toString());
                return;
            }
            TableMetadata tableMetadata = metaDataService.getMetaDataOfSchemaByCache(table);
            if (tableHasNoPrimary(tableMetadata)) {
                log.warn("table no primary ,Message is ignored :  {}", message.toString());
                return;
            }
            final Map<String, String> before = parseRecordData(message, AVRO_FIELD_BEFORE);
            final Map<String, String> after = parseRecordData(message, AVRO_FIELD_AFTER);
            final String schema = source.get(AVRO_FIELD_DB);
            if ((isMatchSchema(schema)) && source.containsKey(AVRO_FIELD_TABLE)) {
                final DebeziumDataBean dataBean = new DebeziumDataBean(table, offset, after.isEmpty() ? before : after);
                queue.put(dataBean);
                log.debug(dataBean.toString());
            } else {
                log.trace("message schema=[{}] is not match , ignored :  {}", schema, message.toString());
            }
        } catch (InterruptedException ex) {
            log.error("put message at the tail of this queue, waiting if necessary for space to become available.");
        }
    }

    private boolean tableHasNoPrimary(TableMetadata tableMetadata) {
        return MetaDataUtil.hasNoPrimary(tableMetadata);
    }

    private TableMetadata refreshMetadataCache(String table) {
        TableMetadata tableMetadata = metaDataService.queryIncrementMetaData(table);
        metaDataService.updateTableMetadata(tableMetadata);
        return tableMetadata;
    }

    @Override
    public void setSchema(String schema) {
        this.destSchema = schema;
    }

    @Override
    public void injectMetaDataServiceInstanceToHandler(MetaDataService metaDataService) {
        this.metaDataService = metaDataService;
    }

    private boolean isMatchSchema(String matchSchema) {
        return StringUtils.equalsIgnoreCase(destSchema, matchSchema);
    }

    private boolean isTransactionMessage(Record message) {
        return message.hasField(TRANSACTION_STATUS);
    }

    private boolean isDdlMessage(Record message) {
        return message.hasField(DDL);
    }

    interface MessageConstants {
        String TRANSACTION_STATUS = "status";
        String DDL = "ddl";
        String AVRO_FIELD_BEFORE = "before";
        String AVRO_FIELD_AFTER = "after";
        String AVRO_FIELD_SOURCE = "source";
        String AVRO_FIELD_TABLE = "table";
        String AVRO_FIELD_DB = "db";
    }

    private Map<String, String> parseRecordData(Record message, String key) {
        final Object object = message.get(key);
        if (Objects.nonNull(object)) {
            return JSONObject.parseObject(object.toString(), new TypeReference<>() {});
        } else {
            return new HashMap<>(0);
        }
    }
}
