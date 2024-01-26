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

import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.Mock;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.common.service.ShutdownService;
import org.opengauss.datachecker.common.util.ReflectUtil;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.config.ExtractProperties;

import java.util.concurrent.ScheduledExecutorService;

import static org.assertj.core.api.Assertions.assertThat;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class IncrementDataAnalysisServiceTest {

    @Mock
    private ExtractProperties mockExtractProperties;
    @Mock
    private DataConsolidationService mockConsolidationService;
    @Mock
    private CheckingFeignClient mockCheckingFeignClient;
    @Mock
    private ShutdownService shutdownService;

    private IncrementDataAnalysisService incrementDataAnalysisServiceUnderTest;

    @BeforeEach
    void setUp() {
        incrementDataAnalysisServiceUnderTest =
            new IncrementDataAnalysisService(mockExtractProperties, mockConsolidationService, mockCheckingFeignClient);
        ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        if (scheduled_executor != null) {
            scheduled_executor.shutdownNow();
            ReflectUtil.setField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                "SCHEDULED_EXECUTOR", null);
        }
        ReflectUtil.setField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
            "shutdownService", shutdownService);
    }

    @DisplayName("when config debezium is disable ,do nothing")
    @Test
    void testStartIncrDataAnalysis_disable_and_not_source_endpoint_do_nothing() {
        // Setup
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(false);
        // Run the test
        incrementDataAnalysisServiceUnderTest.startIncrDataAnalysis();

        final ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        assertThat(scheduled_executor).isNull();
    }

    @DisplayName("when config debezium is enable but not source endpoint,do nothing")
    @Test
    void testStartIncrDataAnalysis_enable_and_not_source_endpoint_do_nothing() {
        // Setup
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(true);
        when(mockConsolidationService.isSourceEndpoint()).thenReturn(false);
        // Run the test
        incrementDataAnalysisServiceUnderTest.startIncrDataAnalysis();
        final ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        assertThat(scheduled_executor).isNull();
    }

    @DisplayName("start incr data analysis, num period smaller than default, throw  IllegalArgumentException")
    @Test
    void testStartIncrDataAnalysis_num_period_1_10_throw_exception() {
        // Setup
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(true);
        when(mockConsolidationService.isSourceEndpoint()).thenReturn(true);
        when(mockExtractProperties.getDebeziumTimePeriod()).thenReturn(1);
        when(mockExtractProperties.getDebeziumNumPeriod()).thenReturn(1);
        when(mockExtractProperties.getDebeziumNumDefaultPeriod()).thenReturn(10);
        // Run the test
        assertThatThrownBy(() -> incrementDataAnalysisServiceUnderTest.startIncrDataAnalysis()).isInstanceOf(
            IllegalArgumentException.class);
        ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        assertThat(scheduled_executor).isNotNull();
        scheduled_executor.shutdownNow();
    }

    @DisplayName("start incr data analysis, time period(0) throw  IllegalArgumentException")
    @Test
    void testStartIncrDataAnalysis_time_period_0_throw_exception() {
        // Setup
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(true);
        when(mockConsolidationService.isSourceEndpoint()).thenReturn(true);
        when(mockExtractProperties.getDebeziumTimePeriod()).thenReturn(0);
        when(mockExtractProperties.getDebeziumNumPeriod()).thenReturn(1);
        when(mockExtractProperties.getDebeziumNumDefaultPeriod()).thenReturn(10);
        // Run the test
        assertThatThrownBy(() -> incrementDataAnalysisServiceUnderTest.startIncrDataAnalysis()).isInstanceOf(
            IllegalArgumentException.class);
        ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        assertThat(scheduled_executor).isNotNull();
        scheduled_executor.shutdownNow();
    }

    @DisplayName("start incr data analysis, time period(1) , num period(10,10) success")
    @Test
    void testStartIncrDataAnalysis_time_period_1_throw_exception() {
        // Setup
        when(mockExtractProperties.isDebeziumEnable()).thenReturn(true);
        when(mockConsolidationService.isSourceEndpoint()).thenReturn(true);
        when(mockExtractProperties.getDebeziumTimePeriod()).thenReturn(1);
        when(mockExtractProperties.getDebeziumNumPeriod()).thenReturn(10);
        when(mockExtractProperties.getDebeziumNumDefaultPeriod()).thenReturn(10);
        // Run the test
        incrementDataAnalysisServiceUnderTest.startIncrDataAnalysis();

        ScheduledExecutorService scheduled_executor =
            ReflectUtil.getField(IncrementDataAnalysisService.class, incrementDataAnalysisServiceUnderTest,
                ScheduledExecutorService.class, "SCHEDULED_EXECUTOR");
        assertThat(scheduled_executor).isNotNull();
        scheduled_executor.shutdownNow();
    }
}
