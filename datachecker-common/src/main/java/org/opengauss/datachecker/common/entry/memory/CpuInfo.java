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
import org.opengauss.datachecker.common.util.MathUtils;

import java.util.LinkedList;
import java.util.List;

/**
 * CpuInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/4/23
 * @since ：11
 */
@Setter
public class CpuInfo extends BaseMonitor implements MonitorFormatter {
    private int cpuNum;
    private double total;
    private double sys;
    private double used;
    private double wait;
    private double free;

    /**
     * Number of CPU cores
     *
     * @return Number of CPU cores
     */
    public double getCpuNum() {
        return cpuNum;
    }

    /**
     * Total CPU usage
     *
     * @return Total CPU usage
     */
    public double getTotal() {
        return MathUtils.round100(total);
    }

    /**
     * CPU system usage rate
     *
     * @return CPU system usage rate
     */
    public double getSys() {
        return MathUtils.mul(MathUtils.divRound(sys, total), MathUtils.CONVERT_PERCENTAGE);
    }

    /**
     * CPU user usage rate
     *
     * @return CPU user usage rate
     */
    public double getUsed() {
        return MathUtils.mul(MathUtils.divRound(used, total), MathUtils.CONVERT_PERCENTAGE);
    }

    /**
     * CPU current wait rate
     *
     * @return CPU current wait rate
     */
    public double getWait() {
        return MathUtils.mul(MathUtils.divRound(wait, total), MathUtils.CONVERT_PERCENTAGE);
    }

    /**
     * CPU current idle rate
     *
     * @return CPU current idle rate
     */
    public double getFree() {
        return MathUtils.mul(MathUtils.divRound(free, total), MathUtils.CONVERT_PERCENTAGE);
    }

    @Override
    public String toString() {
        return format();
    }

    @Override
    public String getTitle() {
        return "Cpu Information";
    }

    @Override
    public List<Field> getFormatFields() {
        List<Field> cpu = new LinkedList<>();
        cpu.add(Field.of("cpuNum", getCpuNum()));
        cpu.add(Field.of("total", getTotal()));
        cpu.add(Field.of("sys", getSys()));
        cpu.add(Field.of("used", getUsed()));
        cpu.add(Field.of("wait", getWait()));
        cpu.add(Field.of("free", getFree()));
        return cpu;
    }
}
