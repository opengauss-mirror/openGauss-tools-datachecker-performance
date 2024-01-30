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

package org.opengauss.datachecker.check.load;

import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Test;
import org.junit.jupiter.api.extension.ExtendWith;
import org.mockito.InjectMocks;
import org.mockito.Mock;
import org.mockito.Spy;
import org.mockito.junit.jupiter.MockitoExtension;
import org.opengauss.datachecker.check.service.EndpointManagerService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;

import static org.mockito.Mockito.doNothing;
import static org.mockito.Mockito.when;

@ExtendWith(MockitoExtension.class)
class HeartBeatStartLoaderTest {

    @Mock
    private EndpointManagerService mockEndpointManagerService;
    @InjectMocks
    @Spy
    private HeartBeatStartLoader heartBeatStartLoaderUnderTest;

    @DisplayName("source & sink health true")
    @Test
    void testLoad_source_sink_true() {
        ConfigCache.put(ConfigConstants.ENABLE_HEART_BEAT_HEATH, true);
        // Setup
        final CheckEnvironment checkEnvironment = new CheckEnvironment();
        doNothing().when(mockEndpointManagerService)
                   .heartBeat();
        when(mockEndpointManagerService.isEndpointHealth()).thenReturn(true);
        // Run the test
        heartBeatStartLoaderUnderTest.load(checkEnvironment);
    }
}
