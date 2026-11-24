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

package org.opengauss.datachecker.extract.task.sql;

import com.alibaba.fastjson.JSONObject;
import org.apache.commons.io.IOUtils;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;

import java.io.IOException;
import java.io.InputStream;
import java.nio.charset.StandardCharsets;
import java.util.Map;
import java.util.Objects;

/**
 * MockTableMeta
 *
 * @author ：wangchao
 * @date ：Created in 2022/9/21
 * @since ：11
 */
public class MockTableMeta {
    private String metaJsonResource = "data/update_dml_builder_test/metadata.json";
    private String valueJsonResource = "data/update_dml_builder_test/values.json";

    public String getSchema() {
        return "test";
    }

    public Map<String, String> getValues() {
        return JSONObject.parseObject(mockMetadataJson(valueJsonResource), Map.class);
    }

    public TableMetadata mockSingleTablePrimaryMetadata() {
        return JSONObject.parseObject(mockMetadataJson(metaJsonResource), TableMetadata.class);
    }

    public String mockMetadataJson(String resource) {
        try (InputStream inputStream = MockTableMeta.class.getClassLoader().getResourceAsStream(resource)) {
            if (Objects.nonNull(inputStream)) {
                return IOUtils.toString(inputStream, String.valueOf(StandardCharsets.UTF_8));
            } else {
                return null;
            }
        } catch (IOException ex) {
            return null;
        }
    }
}
