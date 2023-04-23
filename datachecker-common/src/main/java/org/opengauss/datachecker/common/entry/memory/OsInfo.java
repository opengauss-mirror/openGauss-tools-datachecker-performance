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

/**
 * OsInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/29
 * @since ：11
 */
public class OsInfo {
    private String osName;
    private String version;
    private int availableProcessors;
    private double systemLoadAverage;
    private String arch;

    /**
     * OsInfo
     *
     * @param osName              osName
     * @param version             version
     * @param availableProcessors availableProcessors
     * @param systemLoadAverage   systemLoadAverage
     * @param arch                arch
     */
    public OsInfo(String osName, String version, int availableProcessors, double systemLoadAverage, String arch) {
        this.osName = osName;
        this.version = version;
        this.availableProcessors = availableProcessors;
        this.systemLoadAverage = systemLoadAverage;
        this.arch = arch;
    }

    /**
     * format template
     * <p>
     * -------------------- Operating System Information --------------------
     * Operating System Name       : osName
     * Operating System Version    : version
     * Operating System Processors : availableProcessors
     * Operating System Average    : systemLoadAverage
     * Operating System arch       : arch
     * </p>
     *
     * @return format
     */
    @Override
    public String toString() {
        StringBuffer message = new StringBuffer();
        message.append("-------------------- Operating System Information --------------------");
        message.append(System.lineSeparator());
        message.append("| Operating System Name       : ").append(osName);
        message.append(System.lineSeparator());
        message.append("| Operating System Version    : ").append(version);
        message.append(System.lineSeparator());
        message.append("| Operating System Processors : ").append(availableProcessors);
        message.append(System.lineSeparator());
        message.append("| Operating System Average    : ").append(systemLoadAverage);
        message.append(System.lineSeparator());
        message.append("| Operating System arch       : ").append(arch);
        message.append(System.lineSeparator());
        return message.toString();
    }
}
