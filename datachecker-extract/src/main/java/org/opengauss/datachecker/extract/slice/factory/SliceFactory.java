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

package org.opengauss.datachecker.extract.slice.factory;

import com.alibaba.druid.pool.DruidDataSource;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.SpringUtil;
import org.opengauss.datachecker.extract.slice.process.CsvTableProcessor;
import org.opengauss.datachecker.extract.slice.process.JdbcTableProcessor;
import org.opengauss.datachecker.extract.slice.process.SliceProcessor;
import org.opengauss.datachecker.extract.slice.SliceProcessorContext;
import org.opengauss.datachecker.extract.slice.process.CsvSliceProcessor;
import org.opengauss.datachecker.extract.slice.process.JdbcSliceProcessor;

import java.nio.file.Path;
import java.util.List;
import java.util.Objects;

/**
 * SliceFactory
 *
 * @author ：wangchao
 * @date ：Created in 2023/8/8
 * @since ：11
 */
public class SliceFactory {
    private DruidDataSource datasource;

    /**
     * slice factory
     *
     * @param datasource datasource
     */
    public SliceFactory(DruidDataSource datasource) {
        this.datasource = datasource;
    }

    /**
     * create slice processor<br>
     * if datasource is null, so default create csv slice processor,else create jdbc slice processor
     *
     * @param sliceVo slice
     * @return slice processor
     */
    public SliceProcessor createSliceProcessor(SliceVo sliceVo) {
        SliceProcessorContext processorContext = SpringUtil.getBean(SliceProcessorContext.class);
        if (Objects.isNull(datasource)) {
            return new CsvSliceProcessor(sliceVo, processorContext);
        }
        return new JdbcSliceProcessor(sliceVo, processorContext, datasource);
    }

    /**
     * create slice processor<br>
     *
     * @param table          table
     * @param tableFilePaths tableFilePaths
     * @return SliceProcessor
     */
    public SliceProcessor createTableProcessor(String table, List<Path> tableFilePaths) {
        SliceProcessorContext processorContext = SpringUtil.getBean(SliceProcessorContext.class);
        if (Objects.isNull(datasource)) {
            return new CsvTableProcessor(table, tableFilePaths, processorContext);
        }
        return new JdbcTableProcessor(table, processorContext, datasource);
    }
}
