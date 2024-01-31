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

package org.opengauss.datachecker.extract.data.csv;

import net.openhft.hashing.LongHashFunction;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.extract.client.CheckingFeignClient;
import org.opengauss.datachecker.extract.constants.ExtConstants;

import java.util.List;

/**
 * CsvListener
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/17
 * @since ：11
 */
public interface CsvListener {
    /**
     * xx3 seed
     */
    long XX3_SEED = 199972221018L;

    /**
     * hashing algorithm
     */
    LongHashFunction XX_3_HASH = LongHashFunction.xx3(XX3_SEED);

    /**
     * calc line content hash value
     *
     * @param content content
     * @return hash
     */
    default long lineContentHash(String content) {
        return XX_3_HASH.hashChars(content);
    }

    /**
     * init csv listener
     *
     * @param checkingClient checkingClient
     */
    void initCsvListener(CheckingFeignClient checkingClient);

    /**
     * fetch table's all slices
     *
     * @param table table
     * @return slices
     */
    List<SliceVo> fetchTableSliceList(String table);

    /**
     * stop tailer listener
     */
    void stop();

    /**
     * check slice ptn num, if ptn num is invalid, set min ptn num
     *
     * @param slice slice
     */
    default void checkSlicePtnNum(SliceVo slice) {
        if (slice.getPtnNum() < ExtConstants.MIN_TOPIC_PTN_NUM) {
            slice.setPtnNum(ExtConstants.MIN_TOPIC_PTN_NUM);
        }
    }

    /**
     * listener is finished
     *
     * @return true | false
     */
    boolean isFinished();

    /**
     * release slice cache of table
     *
     * @param table table
     */
    void releaseSliceCache(String table);
}
