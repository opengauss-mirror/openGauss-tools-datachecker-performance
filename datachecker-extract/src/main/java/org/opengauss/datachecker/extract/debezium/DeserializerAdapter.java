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

package org.opengauss.datachecker.extract.debezium;

import io.confluent.kafka.serializers.KafkaAvroDeserializer;
import org.apache.kafka.common.serialization.Deserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

import java.util.HashMap;
import java.util.Map;

/**
 * DeserializerAdapter
 *
 * @author ：wangchao
 * @date ：Created in 2023/2/8
 * @since ：11
 */
public class DeserializerAdapter {
    private static final String AVRO_SERIALIZER = "AvroSerializer";
    private static final String STRING_SERIALIZER = "StringSerializer";
    private static final String SCHEMA_REGISTRY_URL = "schema.registry.url";
    private static final Map<String, DebeziumDataHandler> HANDLERS = new HashMap<>();
    private static final Map<String, Class<? extends Deserializer>> DESERIALIZER = new HashMap<>();

    static {
        HANDLERS.put(AVRO_SERIALIZER, new DebeziumAvroHandler());
        HANDLERS.put(STRING_SERIALIZER, new DebeziumStringHandler());
        DESERIALIZER.put(STRING_SERIALIZER, StringDeserializer.class);
        DESERIALIZER.put(AVRO_SERIALIZER, KafkaAvroDeserializer.class);
    }

    /**
     * get debezium data handler by serializer type name
     *
     * @param name serializer type name
     * @return return debezium data handler
     */
    public DebeziumDataHandler getHandler(String name) {
        return HANDLERS.getOrDefault(name, HANDLERS.get(STRING_SERIALIZER));
    }

    /**
     * get String or avro deserializer
     *
     * @param name serializer type name
     * @return string or avro deserializer
     */
    public Class<? extends Deserializer> getDeserializer(String name) {
        return DESERIALIZER.getOrDefault(name, DESERIALIZER.get(STRING_SERIALIZER));
    }

    /**
     * get avro schema registry url key
     *
     * @return return avro schema registry url key
     */
    public String getAvroSchemaRegistryUrlKey() {
        return SCHEMA_REGISTRY_URL;
    }

    /**
     * check current serializer type is avro
     *
     * @param name serializer type name
     * @return current type is avro
     */
    public boolean isAvro(String name) {
        return AVRO_SERIALIZER.equalsIgnoreCase(name);
    }
}
