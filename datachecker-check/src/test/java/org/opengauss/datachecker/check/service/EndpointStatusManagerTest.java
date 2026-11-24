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

package org.opengauss.datachecker.check.service;

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.mockito.Mock;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.util.ReflectUtil;

import static org.assertj.core.api.Assertions.assertThat;

class EndpointStatusManagerTest {

    @Mock
    private EndpointStatusManager endpointStatusManagerUnderTest;

    @BeforeEach
    void setUp() {
        endpointStatusManagerUnderTest = new EndpointStatusManager();
    }

    @DisplayName("reset source status false")
    @Test
    void testResetStatus_source_false() {
        // Run the test
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SOURCE, false);
        // Verify the results
        assertThat(endpointStatusManagerUnderTest.isEndpointHealth()).isFalse();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SOURCE)).isFalse();
        resetStatus(endpointStatusManagerUnderTest);
    }

    void resetStatus(EndpointStatusManager endpointStatusManagerUnderTest) {
        ReflectUtil.invoke(EndpointStatusManager.class, endpointStatusManagerUnderTest, "reset");
    }

    @DisplayName("reset source status true")
    @Test
    void testResetStatus_source_true() {
        // Run the test
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SOURCE, true);

        // Verify the results
        assertThat(endpointStatusManagerUnderTest.isEndpointHealth()).isFalse();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SOURCE)).isTrue();
        resetStatus(endpointStatusManagerUnderTest);
    }

    @DisplayName("reset sink status false")
    @Test
    void testResetStatus_sink_false() {
        // Run the test
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SINK, false);

        // Verify the results
        assertThat(endpointStatusManagerUnderTest.isEndpointHealth()).isFalse();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SINK)).isFalse();
        resetStatus(endpointStatusManagerUnderTest);
    }

    @DisplayName("reset sink status true")
    @Test
    void testResetStatus_sink_true() {
        // Run the test
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SINK, true);
        // Verify the results
        assertThat(endpointStatusManagerUnderTest.isEndpointHealth()).isFalse();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SINK)).isTrue();
        resetStatus(endpointStatusManagerUnderTest);
    }

    @DisplayName("reset source sink status true")
    @Test
    void testResetStatus_source_sink_true() {
        // Run the test
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SOURCE, true);
        endpointStatusManagerUnderTest.resetStatus(Endpoint.SINK, true);
        // Verify the results
        assertThat(endpointStatusManagerUnderTest.isEndpointHealth()).isTrue();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SOURCE)).isTrue();
        assertThat(endpointStatusManagerUnderTest.getHealthStatus(Endpoint.SINK)).isTrue();
        resetStatus(endpointStatusManagerUnderTest);
    }
}
