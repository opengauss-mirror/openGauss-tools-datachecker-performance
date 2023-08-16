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

package org.opengauss.datachecker.common.entry.memory;

import java.util.LinkedList;
import java.util.List;

/**
 * GcInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class GcInfo extends BaseMonitor implements MonitorFormatter {
    private String name;
    private long collectionTime;
    private long collectionCount;

    /**
     * GcInfo
     *
     * @param name            gc name
     * @param collectionTime  collectionTime
     * @param collectionCount collectionCount
     */
    public GcInfo(String name, long collectionTime, long collectionCount) {
        this.name = name;
        this.collectionTime = collectionTime;
        this.collectionCount = collectionCount;
    }

    @Override
    public String toString() {
        return format();
    }

    @Override
    public String getTitle() {
        return "GC Information";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> format = new LinkedList<>();
        format.add(Field.of("name", name));
        format.add(Field.of("collectionCount", collectionCount + " times"));
        format.add(Field.of("collectionTime", collectionTime + " milli"));
        return format;
    }
}
