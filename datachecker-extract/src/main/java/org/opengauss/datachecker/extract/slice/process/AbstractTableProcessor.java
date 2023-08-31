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

package org.opengauss.datachecker.extract.slice.process;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;
import org.opengauss.datachecker.common.entry.enums.SliceStatus;
import org.opengauss.datachecker.common.entry.extract.SliceExtend;
import org.opengauss.datachecker.common.entry.extract.TableMetadata;
import org.opengauss.datachecker.common.util.LogUtils;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;

import java.util.Objects;

/**
 * AbstractTableProcessor
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/27
 * @since ：11
 */
public abstract class AbstractTableProcessor extends AbstractProcessor {
    protected static final Logger log = LogUtils.getBusinessLogger();
    protected String table;
    protected TableMetadata tableMetadata;

    public AbstractTableProcessor(String table, SliceProcessorContext context) {
        super(context);
        this.table = table;
        this.tableMetadata = context.getTableMetaData(table);
    }

    protected SliceExtend createTableSliceExtend() {
        SliceExtend tableSliceExtend = new SliceExtend();
        tableSliceExtend.setName(table);
        tableSliceExtend.setEndpoint(ConfigCache.getEndPoint());
        tableSliceExtend.setTableHash(tableMetadata.getTableHash());
        tableSliceExtend.setStatus(SliceStatus.codeOf(ConfigCache.getEndPoint()));
        return tableSliceExtend;
    }
    protected void initTableMetadata() {
        this.tableMetadata = context.getTableMetaData(table);
        Objects.requireNonNull(tableMetadata, "table metadata " + table + " must not be null");
    }

    protected int getFetchSize() {
        return ConfigCache.getIntValue(ConfigConstants.FETCH_SIZE);
    }

    protected int getMaximumTableSliceSize() {
        return ConfigCache.getIntValue(ConfigConstants.MAXIMUM_TABLE_SLICE_SIZE);
    }

    protected int getQueryDop() {
        return ConfigCache.getIntValue(ConfigConstants.QUERY_DOP);
    }

    public boolean noTableSlice() {
        return tableMetadata.getTableRows() < getMaximumTableSliceSize() || getQueryDop() == 1;
    }
}
