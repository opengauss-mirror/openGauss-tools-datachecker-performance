/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2026-2026. All rights reserved.
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

package org.opengauss.datachecker.extract.slice;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.entry.extract.SliceVo;
import org.opengauss.datachecker.common.util.LogUtils;

import java.math.BigDecimal;
import java.math.RoundingMode;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.stream.Collectors;
import java.util.stream.IntStream;

/**
 * Sampling check selector: picks evenly spaced slices from a table's slice list by ratio,
 * so tables with many slices can check only a subset and cut extraction/check overhead
 *
 * @author : xujintao
 * @date : Created in 2026/9/7
 * @since : 11
 */
public class SampleSelector {
    private static final Logger log = LogUtils.getLogger(SampleSelector.class);

    /** Minimum sampling ratio */
    private static final double MIN_RATIO = 0.0;

    /** Maximum sampling ratio; >= 1.0 means full check */
    private static final double MAX_RATIO = 1.0;

    /** Sampling ratio, range [0.0, 1.0] */
    private final double ratio;

    /** Minimum slice count that triggers sampling; 0 means no threshold */
    private final int threshold;

    /**
     * Construct the sampling selector
     *
     * @param ratio     sampling ratio, range [0.0, 1.0]; >= 1.0 means full check
     * @param threshold minimum slice count that triggers sampling; 0 means no threshold
     */
    public SampleSelector(double ratio, int threshold) {
        this.ratio = Math.max(MIN_RATIO, ratio);
        this.threshold = Math.max(0, threshold);
    }

    /**
     * Whether the slice list needs sampling. Requires all of: ratio < 1.0, slice count > 1,
     * and slice count above the threshold (if configured)
     *
     * @param tableSliceList slice list
     * @return true if sampling is needed
     */
    public boolean shouldSample(List<SliceVo> tableSliceList) {
        if (ratio >= MAX_RATIO) {
            return false;
        }
        if (tableSliceList == null || tableSliceList.size() <= 1) {
            return false;
        }
        if (threshold > 0 && tableSliceList.size() <= threshold) {
            LogUtils.info(log, "sample check skipped: slice count {} <= threshold {}",
                    tableSliceList.size(), threshold);
            return false;
        }
        return true;
    }

    /**
     * Pick slices by even spacing: sample count = ceil(total x ratio), at least 1;
     * first and last always included, rest evenly distributed
     *
     * @param tableSliceList full slice list sorted by slice number ascending
     * @return the selected slice subset (original objects, not copies), sorted by slice number ascending
     */
    public List<SliceVo> selectSlices(List<SliceVo> tableSliceList) {
        int totalSlices = tableSliceList.size();
        int sampleCount = calculateSampleCount(totalSlices);
        List<Integer> indices = sample(totalSlices, sampleCount);
        List<SliceVo> sampled = new ArrayList<>(sampleCount);
        for (Integer idx : indices) {
            sampled.add(tableSliceList.get(idx - 1));
        }

        LogUtils.info(log, "sample check: selected {} of {} slices (ratio={}), indices={}",
                sampleCount, totalSlices, ratio, indices);
        return sampled;
    }

    /**
     * Build registration copies from the sampled slices: total is rewritten to the sample count
     * for the check side's counting, while name keeps the original value so the slice name
     * is not regenerated from the rewritten total
     *
     * @param sampledSlices the sampled slice list
     * @return copies for registration
     */
    public List<SliceVo> createRegisterCopies(List<SliceVo> sampledSlices) {
        int sampleCount = sampledSlices.size();
        List<SliceVo> copies = new ArrayList<>(sampleCount);
        for (SliceVo original : sampledSlices) {
            SliceVo copy = copySliceVo(original);
            // Lock the slice name so it is not regenerated from the rewritten total
            copy.setName(original.getName());
            // Rewrite total to the sample count for the check side's counting
            copy.setTotal(sampleCount);
            copies.add(copy);
        }
        return copies;
    }

    /**
     * Calculate the sample count: ceil(total x ratio), at least 1 and at most the total
     *
     * @param totalSlices total slice count
     * @return sample count
     */
    private int calculateSampleCount(int totalSlices) {
        int count = BigDecimal.valueOf(totalSlices).multiply(BigDecimal.valueOf(ratio))
            .setScale(0, RoundingMode.CEILING).intValue();
        return Math.max(1, Math.min(count, totalSlices));
    }

    /**
     * Deep-copy a SliceVo (covering all fields of both BaseSlice and SliceVo)
     *
     * @param original the source slice
     * @return the copied slice
     */
    private SliceVo copySliceVo(SliceVo original) {
        SliceVo copy = new SliceVo();
        copy.setSchema(original.getSchema());
        copy.setTable(original.getTable());
        copy.setName(original.getName());
        copy.setType(original.getType());
        copy.setWholeTable(original.isWholeTable());
        copy.setEndpoint(original.getEndpoint());
        copy.setNo(original.getNo());
        copy.setTotal(original.getTotal());
        copy.setBeginIdx(original.getBeginIdx());
        copy.setEndIdx(original.getEndIdx());
        copy.setInIds(original.getInIds());
        copy.setRowCountOfInIds(original.getRowCountOfInIds());
        copy.setFetchSize(original.getFetchSize());
        // SliceVo's own fields (the ones above belong to the parent BaseSlice)
        copy.setStatus(original.getStatus());
        copy.setExistTableRows(original.isExistTableRows());
        copy.setTableHash(original.getTableHash());
        copy.setPtn(original.getPtn());
        copy.setPtnNum(original.getPtnNum());
        return copy;
    }

    /**
     * Evenly spaced sampling: pick sampleCount evenly distributed indices from [1, totalCount],
     * always including the first and the last
     *
     * @param totalCount  total count
     * @param sampleCount sample count
     * @return ascending 1-based index list
     */
    public static List<Integer> sample(int totalCount, int sampleCount) {
        if (totalCount < 1) {
            return Collections.emptyList();
        }
        if (sampleCount == 1) {
            return Collections.singletonList(totalCount);
        }
        if (sampleCount < 1 || sampleCount >= totalCount) {
            return IntStream.rangeClosed(1, totalCount).boxed().collect(Collectors.toList());
        }

        // Step = (totalCount - 1) / (sampleCount - 1); index = round(1 + i * step),
        // which guarantees the first and the last are always included and the rest evenly distributed
        double step = 1.0 * (totalCount - 1) / (sampleCount - 1);
        return IntStream.range(0, sampleCount)
                .mapToObj(i -> Math.toIntExact(Math.round(1.0 + i * step)))
                .collect(Collectors.toList());
    }
}
