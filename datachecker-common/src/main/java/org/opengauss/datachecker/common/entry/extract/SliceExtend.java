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

package org.opengauss.datachecker.common.entry.extract;

import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

import java.nio.file.Path;
import java.util.List;

/**
 * SliceExtend
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@Data
public class SliceExtend {
    private Endpoint endpoint;
    private String name;
    private List<Path> tableFilePaths;
    private int no;
    private long startOffset;
    private long endOffset;
    private long count;
    private int status;
    private long tableHash;

    @Override
    public String toString() {
        return "{ name=" + name + ", " + endpoint.getDescription() + ", no=" + no + " , offset=[" + startOffset + ", "
            + endOffset + "] , count=" + count + " , tableHash=" + tableHash + '}';
    }

    public String toSimpleString() {
        return name + ", " + endpoint.getDescription() + ", no=" + no + " , offset=[" + startOffset + ", " + endOffset
            + "] , count=" + count;
    }
}
