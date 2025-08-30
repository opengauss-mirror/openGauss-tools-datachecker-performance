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

import org.opengauss.datachecker.check.client.FeignClientService;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.Endpoint;
import org.opengauss.datachecker.common.entry.enums.ErrorCode;
import org.opengauss.datachecker.common.entry.enums.LowerCaseTableNames;
import org.opengauss.datachecker.common.entry.extract.Database;
import org.opengauss.datachecker.common.entry.extract.ExtractConfig;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.common.util.ThreadUtil;
import org.springframework.core.annotation.Order;
import org.springframework.stereotype.Service;
import org.springframework.util.Assert;

import jakarta.annotation.Resource;
import java.util.Objects;

/**
 * CheckDatabaseLoader
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Order(97)
@Service
public class CheckDatabaseLoader extends AbstractCheckLoader {
    @Resource
    private FeignClientService feignClient;

    /**
     * Initialize the verification result environment
     */
    @Override
    public void load(CheckEnvironment checkEnvironment) {
        int retry = 1;
        ExtractConfig sourceConfig = feignClient.getEndpointConfig(Endpoint.SOURCE);
        ExtractConfig sinkConfig = feignClient.getEndpointConfig(Endpoint.SINK);
        while (retry <= maxRetryTimes && (sourceConfig == null || sinkConfig == null)) {
            sourceConfig = feignClient.getEndpointConfig(Endpoint.SOURCE);
            sinkConfig = feignClient.getEndpointConfig(Endpoint.SINK);
            LogUtils.error(log, "{}load database configuration ,retry={}", ErrorCode.LOAD_DATABASE_CONFIG, retry);
            ThreadUtil.sleepOneSecond();
            retry++;
        }
        if (sourceConfig == null) {
            shutdown("source endpoint server has error");
            return;
        }
        if (sinkConfig == null) {
            shutdown("sink endpoint server has error");
            return;
        }
        checkDatabaseLowerCaseTableNames(sourceConfig.getDatabase(), sinkConfig.getDatabase());
        checkEnvironment.addExtractDatabase(Endpoint.SOURCE, sourceConfig.getDatabase());
        checkEnvironment.addExtractDatabase(Endpoint.SINK, sinkConfig.getDatabase());
        ConfigCache.put(ConfigConstants.DATA_CHECK_SOURCE_DATABASE, sourceConfig.getDatabase());
        ConfigCache.put(ConfigConstants.DATA_CHECK_SINK_DATABASE, sinkConfig.getDatabase());
        ConfigCache.put(ConfigConstants.LOWER_CASE_TABLE_NAMES, sinkConfig.getDatabase().getLowercaseTableNames());
        ConfigCache.put(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE, getMaxTableSliceConfig(sourceConfig, sinkConfig));
        LogUtils.info(log, "check service load database configuration success.");
    }

    private static int getMaxTableSliceConfig(ExtractConfig sourceConfig, ExtractConfig sinkConfig) {
        int max = Math.max(sourceConfig.getMaxSliceSize(), sinkConfig.getMaxSliceSize());
        return max == 0 ? ConfigConstants.MAXIMUM_TABLE_SLICE_DEFAULT_VALUE : max;
    }

    private void checkDatabaseLowerCaseTableNames(Database source, Database sink) {
        Assert.notNull(source, "source database config can't be null");
        Assert.notNull(sink, "sink database config can't be null");
        Assert.notNull(source.getLowercaseTableNames(), "source database lower_case_table_name fetch error");
        Assert.notNull(sink.getLowercaseTableNames(), "sink database lower_case_table_name fetch error");
        Assert.isTrue(!Objects.equals(source.getLowercaseTableNames(), LowerCaseTableNames.UNKNOWN),
                "sink database lower_case_table_name fetch unknown");
        Assert.isTrue(!Objects.equals(sink.getLowercaseTableNames(), LowerCaseTableNames.UNKNOWN),
                "sink database lower_case_table_name fetch unknown");
        Assert.isTrue(Objects.equals(source.getLowercaseTableNames(), sink.getLowercaseTableNames()),
                "source and sink database lower_case_table_name must be the same");
        LogUtils.info(log, "check database lower_case_table_name is {}", source.getLowercaseTableNames());
    }
}
