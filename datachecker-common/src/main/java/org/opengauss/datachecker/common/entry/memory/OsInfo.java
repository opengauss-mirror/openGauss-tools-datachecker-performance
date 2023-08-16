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
 * OsInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class OsInfo extends BaseMonitor implements MonitorFormatter {
    private String name;
    private String version;
    private int processors;
    private double average;
    private String arch;

    /**
     * OsInfo
     *
     * @param name       osName
     * @param version    version
     * @param processors availableProcessors
     * @param average    systemLoadAverage
     * @param arch       arch
     */
    public OsInfo(String name, String version, int processors, double average, String arch) {
        this.name = name;
        this.version = version;
        this.processors = processors;
        this.average = average;
        this.arch = arch;
    }

    @Override
    public String toString() {
        return format();
    }

    @Override
    public String getTitle() {
        return "Operating System";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> osFields = new LinkedList<>();
        osFields.add(Field.of("name", name));
        osFields.add(Field.of("version", version));
        osFields.add(Field.of("processors", processors));
        osFields.add(Field.of("average", average));
        osFields.add(Field.of("arch", arch));
        return osFields;
    }
}
