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

package org.opengauss.datachecker.common.entry.common;

import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.check.Difference;
import org.opengauss.datachecker.common.entry.enums.CheckMode;
import org.opengauss.datachecker.common.entry.enums.DML;

import java.util.ArrayList;
import java.util.LinkedList;
import java.util.List;
import java.util.Set;
import java.util.TreeSet;

/**
 * RepairEntry
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/25
 * @since ：11
 */
@Data
@Accessors(chain = true)
public class RepairEntry {
    private DML type;
    private String schema;
    private String table;
    private String fileName;
    private int sno;
    private CheckMode checkMode;
    private boolean ogCompatibility;
    private Set<String> diffSet = new TreeSet<>();
    private List<Difference> diffList = new LinkedList<>();

    public int diffSize() {
        return Math.max(diffSet.size(), diffList.size());
    }

    public List<String> diffSetToList() {
        return new ArrayList<>(diffSet);
    }
}
