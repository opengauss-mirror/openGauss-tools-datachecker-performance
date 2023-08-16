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

import lombok.Setter;

import java.util.LinkedList;
import java.util.List;

/**
 * ThreadInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Setter
public class ThreadInfo extends BaseMonitor implements MonitorFormatter {
    private int threadCount;
    private int peakThreadCount;
    private int daemonThreadCount;

    @Override
    public String toString() {
        return format();
    }

    @Override
    public String getTitle() {
        return "JVM Thread";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> threadFields = new LinkedList<>();
        threadFields.add(Field.of("threads", threadCount));
        threadFields.add(Field.of("max", peakThreadCount));
        threadFields.add(Field.of("daemon", daemonThreadCount));
        return threadFields;
    }
}
