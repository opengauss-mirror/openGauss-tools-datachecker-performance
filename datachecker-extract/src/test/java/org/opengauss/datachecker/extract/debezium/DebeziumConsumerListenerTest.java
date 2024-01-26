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

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.util.ReflectUtil;
import org.opengauss.datachecker.extract.config.ExtractProperties;
import org.opengauss.datachecker.extract.service.MetaDataService;
import org.opengauss.datachecker.extract.util.TestJsonUtil;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;

@ExtendWith(MockitoExtension.class)
class DebeziumConsumerListenerTest {

    private DebeziumConsumerListener debeziumConsumerListenerUnderTest;
    @Mock
    private MetaDataService metaDataService;
    @Mock
    private ExtractProperties extractProperties;

    @BeforeEach
    void setUp() {
        debeziumConsumerListenerUnderTest = new DebeziumConsumerListener();
        ReflectUtil.setField(DebeziumConsumerListener.class, debeziumConsumerListenerUnderTest, "extractProperties",
            extractProperties);
        ReflectUtil.setField(DebeziumConsumerListener.class, debeziumConsumerListenerUnderTest, "metaDataService",
            metaDataService);
        debeziumConsumerListenerUnderTest.initDebeziumDataHandler();
    }

    @DisplayName("listen record value empty")
    @Test
    void test_listen_record_value_empty() {
        // Setup
        final ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0L, "key", "");
        // Verify the results
        assertThatThrownBy(() -> debeziumConsumerListenerUnderTest.listen(record)).isInstanceOf(NullPointerException.class);
    }

    @DisplayName("listen record value parse error")
    @Test
    void test_listen_record_value_parse_error() {
        // Setup
        final ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0L, "key", "value");
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        // Verify the results
    }

    @DisplayName("listen record value parse success")
    @Test
    void test_listen_record_value_parse_success() {
        // Setup
        String value = TestJsonUtil.getJsonText(TestJsonUtil.KEY_DEBEZIUM_ONE_TABLE_RECORD);
        final ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0L, "key", value);
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        // Verify the results
    }

    @DisplayName("listen record value parse empty ")
    @Test
    void test_listen_record_value_parse_success_empty_size() {
        assertThat(debeziumConsumerListenerUnderTest.size()).isEqualTo(0);
    }

    @DisplayName("listen record value parse success ,poll")
    @Test
    void test_listen_record_value_parse_success_poll() {
        String value = TestJsonUtil.getJsonText(TestJsonUtil.KEY_DEBEZIUM_ONE_TABLE_RECORD);
        final ConsumerRecord<String, Object> record = new ConsumerRecord<>("topic", 0, 0L, "key", value);
        // Run the test
        debeziumConsumerListenerUnderTest.listen(record);
        final DebeziumDataBean result = debeziumConsumerListenerUnderTest.poll();
        assertThat(result.getTable()).isEqualTo("t_test_1_30000_1");
        assertThat(result.getOffset()).isEqualTo(0);
        assertThat(result.getData()
                         .get("id")).isEqualTo("29836");
        assertThat(result.getData()
                         .get("sizex")).isEqualTo("2251");
        assertThat(result.getData()
                         .get("sizey")).isEqualTo("777");
        assertThat(result.getData()
                         .get("width")).isEqualTo("403");
        assertThat(result.getData()
                         .get("height")).isEqualTo("2492");
        assertThat(result.getData()
                         .get("last_upd_user")).isEqualTo("auto_user");
        assertThat(result.getData()
                         .get("last_upd_time")).isEqualTo("1671132074000");
        assertThat(result.getData()
                         .get("func_id")).isEqualTo(":Ensures that all data changes ar");
        assertThat(result.getData()
                         .get("portal_id")).isEqualTo("nge data capture (CDC). Unlike other approac");
        assertThat(result.getData()
                         .get("show_order")).isEqualTo("136");
    }
}
