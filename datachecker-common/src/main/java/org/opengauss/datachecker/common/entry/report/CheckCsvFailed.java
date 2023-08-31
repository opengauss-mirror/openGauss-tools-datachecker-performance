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

package org.opengauss.datachecker.common.entry.report;

import com.alibaba.fastjson.annotation.JSONType;
import lombok.Data;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.entry.check.Difference;

import javax.validation.constraints.NotNull;
import java.util.List;

/**
 * @author ：wangchao
 * @date ：Created in 2023/2/24
 * @since ：11
 */
@Data
@Accessors(chain = true)
@JSONType(orders = {"table", "fileFailed"})
public class CheckCsvFailed {
    private String table;
    private CsvFileFailed fileFailed;

    public CheckCsvFailed build(String fileName, List<Difference> keyInsert, List<Difference> keyUpdate,
        List<Difference> keyDelete) {
        this.fileFailed = new CsvFileFailed(fileName, keyInsert, keyUpdate, keyDelete);
        return this;
    }

    public boolean isNotEmpty() {
        return fileFailed.getSize() > 0;
    }
}

@Data
@JSONType(orders = {"fileName", "keyInsert", "keyUpdate", "keyDelete"})
class CsvFileFailed {
    private String fileName;
    private int size;
    private List<Difference> keyInsert;
    private List<Difference> keyUpdate;
    private List<Difference> keyDelete;

    public CsvFileFailed(String fileName, @NotNull List<Difference> keyInsert, @NotNull List<Difference> keyUpdate,
        @NotNull List<Difference> keyDelete) {
        this.fileName = fileName;
        this.keyInsert = keyInsert;
        this.keyUpdate = keyUpdate;
        this.keyDelete = keyDelete;
        this.size = keyInsert.size() + keyUpdate.size() + keyDelete.size();
    }
}
