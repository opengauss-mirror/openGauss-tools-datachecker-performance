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

package org.opengauss.datachecker.common.entry.check;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;

import java.util.LinkedList;
import java.util.List;

/**
 * CheckTableInfo
 *
 * @author ：wangchao
 * @date ：Created in 2023/11/10
 * @since ：11
 */
@Data
@JSONType(orders = {"sinkLoss", "sinkExcess", "miss", "structure", "lossTables", "excessTables", "structureTables"})
public class CheckTableInfo {
    private int sourceTableTotalSize;
    private int sinkTableTotalSize;
    private int sinkLoss;
    private int sinkExcess;
    private int miss;
    private int structure;
    private List<String> lossTables = new LinkedList<>();
    private List<String> excessTables = new LinkedList<>();
    private List<String> structureTables = new LinkedList<>();

    /**
     * add source loss table
     *
     * @param tableName table
     */
    public void addSource(String tableName) {
        lossTables.add(tableName);
        sinkLoss++;
    }

    /**
     * add sink loss table
     *
     * @param tableName table
     */
    public void addSink(String tableName) {
        excessTables.add(tableName);
        sinkExcess++;
    }

    /**
     * add table structure checked failed
     *
     * @param tableName table
     */
    public void addStructure(String tableName) {
        structureTables.add(tableName);
        structure++;
    }

    /**
     * get check table count
     *
     * @return count
     */
    public int fetchCheckedTableCount() {
        return (sourceTableTotalSize + sinkTableTotalSize - sinkLoss - sinkExcess) / 2 - structure;
    }
}
