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

package org.opengauss.datachecker.extract.config;

import lombok.Getter;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.DataSourceType;
import org.springframework.context.annotation.Configuration;

/**
 * ExtractConfig
 *
 * @author ：wangchao
 * @date ：Created in 2022/7/1
 * @since ：11
 */
@Configuration
@Getter
public class ExtractConfig {
    private final String schema = "test";
    private final DataSourceType dataSourceType = DataSourceType.Source;
    private final DataBaseType databaseType = DataBaseType.MS;
}
