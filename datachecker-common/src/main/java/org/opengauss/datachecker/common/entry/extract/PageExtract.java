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
import lombok.ToString;
import lombok.experimental.Accessors;
import org.opengauss.datachecker.common.config.ConfigCache;
import org.opengauss.datachecker.common.constant.ConfigConstants;

/**
 * PageExtract
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@ToString
@Data
@Accessors(chain = true)
public class PageExtract {
    /**
     * 任务总数
     */
    private int size;

    /**
     * 当前页号
     */
    private int pageNo;

    /**
     * 分页大小 默认1000
     */
    private int pageSize;

    /**
     * 创建分页对象
     *
     * @param size     任务总数
     * @param pageNo   页号
     * @param pageSize 分页大小
     */
    public PageExtract(int size, int pageNo, int pageSize) {
        this.size = size;
        this.pageNo = pageNo;
        this.pageSize = pageSize;
    }

    /**
     * 创建分页对象
     * <p>
     * 默认任务总数=0;  页号=0,分页大小为默认值
     *
     * @return 分页对象
     */
    public static PageExtract build() {
        return new PageExtract(0, 0, ConfigCache.getIntValue(ConfigConstants.REST_API_PAGE_SIZE));
    }

    /**
     * 创建分页对象
     * <p>
     * 默认任务总数=0;  页号=0,分页大小为默认值
     *
     * @param size 分页总记录大小
     * @return 分页对象
     */
    public static PageExtract buildInitPage(int size) {
        return new PageExtract(size, 0, ConfigCache.getIntValue(ConfigConstants.REST_API_PAGE_SIZE));
    }

    /**
     * 计算当前任务页数
     *
     * @return 页数
     */
    public int getPageCount() {
        return size / pageSize + (size % pageSize == 0 ? 0 : 1);
    }

    /**
     * 计算当前页开始下标
     *
     * @return 开始下标
     */
    public int getPageStartIdx() {
        return pageNo * pageSize;
    }

    /**
     * 计算当前页截止下标
     *
     * @return 截止下标
     */
    public int getPageEndIdx() {
        return Math.min((pageNo + 1) * pageSize, size);
    }

    /**
     * 翻页并返回当前页号
     *
     * @return 页号
     */
    public int incrementAndGetPageNo() {
        pageNo++;
        return pageNo;
    }
}
