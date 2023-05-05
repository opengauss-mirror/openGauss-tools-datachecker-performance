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

package org.opengauss.datachecker.common.constant;

/**
 * Dynamic Thread Pool Constant
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/24
 * @since ：11
 */
public interface DynamicTpConstant {
    /**
     * default core-pool-size = 2
     */
    int DEFAULT_CORE_POOL_SIZE = 2;

    /**
     * default maximum-pool-size = 20
     */
    int DEFAULT_MAXIMUM_POOL_SIZE = 20;

    /**
     * min-cpu-free = 20%
     */
    int MIN_CPU_FREE = 20;

    /**
     * monitoring period is 100 millis
     */
    int MONITORING_PERIOD_MILLIS = 100;

    /**
     * check-executor
     */
    String CHECK_EXECUTOR = "check-dtp";

    /**
     * extract-executor
     */
    String EXTRACT_EXECUTOR = "extract-dtp";

    /**
     * table-parallel-executor
     */
    String TABLE_PARALLEL_EXECUTOR = "parallel-dtp";
}
