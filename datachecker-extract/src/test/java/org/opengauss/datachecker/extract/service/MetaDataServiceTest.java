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

package org.opengauss.datachecker.extract.service;

import org.apache.logging.log4j.Logger;
import org.junit.jupiter.api.Test;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.test.context.SpringBootTest;

import java.sql.SQLException;
import java.util.Map;

/**
 * MetaDataServiceTest
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/14
 * @since ：11
 */
@SpringBootTest
public class MetaDataServiceTest {
    private static final Logger log = LogUtils.getLogger();
    @Autowired
    private MetaDataService metaDataService;

    @Test
    void queryMetadataOfSourceDBSchema() throws SQLException {
        Map<String, TableMetadata> stringTableMetadataMap = metaDataService.queryMetaDataOfSchema();
        for (TableMetadata metadata : stringTableMetadataMap.values()) {
            log.info(metadata.toString());
        }
    }
}
