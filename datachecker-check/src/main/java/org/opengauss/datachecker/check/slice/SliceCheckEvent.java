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

package org.opengauss.datachecker.check.slice;

import lombok.Data;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.SliceVo;

import java.util.Objects;

/**
 * SliceCheckEvent
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/2
 * @since ：11
 */
@Data
public class SliceCheckEvent {
    private SliceVo slice;
    private SliceExtend source;
    private SliceExtend sink;

    /**
     * slice check event
     *
     * @param slice slice
     * @param source source extend of extract
     * @param sink sink extend of extract
     */
    public SliceCheckEvent(SliceVo slice, SliceExtend source, SliceExtend sink) {
        this.slice = slice;
        this.source = source;
        this.sink = sink;
    }

    public boolean isTableLevel() {
        return slice.isWholeTable();
    }

    public String getCheckName() {
        return slice.getName();
    }

    @Override
    public String toString() {
        return "checked event : " + slice.getName() + (Objects.nonNull(source) ? source.toString() : " source is null")
            + (Objects.nonNull(sink) ? sink.toString() : " sink is null");
    }
}
