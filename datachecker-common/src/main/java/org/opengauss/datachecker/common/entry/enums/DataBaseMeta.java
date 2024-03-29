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

package org.opengauss.datachecker.common.entry.enums;

import lombok.Getter;

/**
 * DataBaseMeta
 *
 * @author ：wangchao
 * @date ：Created in 2022/5/29
 * @since ：11
 */
@Getter
public enum DataBaseMeta implements IEnum {
    /**
     * DataBaseHealth
     */
    HEALTH("HealthMeta", "DataBaseHealth"),
    /**
     * COUNT
     */
    COUNT("COUNT", "COUNT"),
    
    /**
     *  Use max(id) to get max count.
     */
    MAX_ID_COUNT("MaxIdCount", "MaxIdCount"),
    /**
     * TableMetaData
     */
    TABLE("TableMetaData", "TableMetaData"),
    /**
     * TablesColumnMetaData
     */
    COLUMN("TablesColumnMetaData", "TablesColumnMetaData");

    private final String code;
    private final String description;

    DataBaseMeta(String code, String description) {
        this.code = code;
        this.description = description;
    }

}
