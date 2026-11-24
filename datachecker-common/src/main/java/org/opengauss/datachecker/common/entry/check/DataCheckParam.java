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

package org.opengauss.datachecker.common.entry.check;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.springframework.boot.autoconfigure.kafka.KafkaProperties;

/**
 * Data verification thread parameters
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/10
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class DataCheckParam {
    private String process;
    private String tableName;
    private int bucketCapacity;
    private int partitions;
    private int errorRate;
    private int tablePartitionRowCount;
    private String schema;
    private KafkaProperties properties;
    private TableMetadata sourceMetadata;
}
