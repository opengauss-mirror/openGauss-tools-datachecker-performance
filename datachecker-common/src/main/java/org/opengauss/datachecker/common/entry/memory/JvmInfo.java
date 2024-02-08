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
import org.opengauss.datachecker.common.util.DateUtils;
import org.opengauss.datachecker.common.util.MathUtils;

import java.lang.management.ManagementFactory;
import java.util.LinkedList;
import java.util.List;

/**
 * Jvm Info
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
@Setter
public class JvmInfo implements MonitorFormatter {
    private long total;
    private long max;
    private long free;
    private String version;
    private String home;

    /**
     * The total amount of memory currently occupied by the JVM (byte)
     *
     * @return total
     */
    public double getTotal() {
        return total;
    }

    /**
     * JVM Maximum Total Free Memory (byte)
     *
     * @return JVM Maximum Total Free Memory (byte)
     */
    public double getMax() {
        return max;
    }

    /**
     * JVM Free Memory (byte)
     *
     * @return JVM Free Memory (byte)
     */
    public double getFree() {
        return free;
    }

    /**
     * JVM Used Memory (M)
     *
     * @return Used
     */
    public double getUsed() {
        return MathUtils.sub(total, free);
    }

    /**
     * JVM  Memory Usage (%)
     *
     * @return Usage
     */
    public double getUsage() {
        return MathUtils.mul(MathUtils.divRound(MathUtils.sub(total, free), total), 100);
    }

    /**
     * JVM name
     *
     * @return name
     */
    public String getName() {
        return ManagementFactory.getRuntimeMXBean().getVmName();
    }

    /**
     * JDK version
     *
     * @return JDK version
     */
    public String getVersion() {
        return version;
    }

    /**
     * JDK path
     *
     * @return JDK path
     */
    public String getHome() {
        return home;
    }

    /**
     * JDK ServerStartDate
     *
     * @return jdk start time
     */
    public String getStartTime() {
        return DateUtils.parseDateToStr(DateUtils.getServerStartDate());
    }

    /**
     * JDK Run Time
     *
     * @return jdk run time
     */
    public String getRunTime() {
        return DateUtils.getDatePoor(DateUtils.getNowDate(), DateUtils.getServerStartDate());
    }

    /**
     * free memory is available
     *
     * @param freeSize free memory
     * @return isAvailable
     */
    public boolean isAvailable(long freeSize) {
        return free > freeSize;
    }

    @Override
    public String toString() {
        return format();
    }

    @Override
    public String getTitle() {
        return "JVM Information";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> jvm = new LinkedList<>();
        jvm.add(Field.of("total", getTotal()));
        jvm.add(Field.of("max", getMax()));
        jvm.add(Field.of("free", getFree()));
        jvm.add(Field.of("usage", getUsage()));
        jvm.add(Field.of("version", getVersion()));
        jvm.add(Field.of("home", getHome()));
        return jvm;
    }
}
