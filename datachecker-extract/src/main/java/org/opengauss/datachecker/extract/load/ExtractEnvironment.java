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

package org.opengauss.datachecker.extract.load;

import org.springframework.stereotype.Service;
import java.util.concurrent.ExecutorService;

/**
 * ExtractEnvironment
 *
 * @author ：wangchao
 * @date ：Created in 2022/10/31
 * @since ：11
 */
@Service
public class ExtractEnvironment {
    private boolean loadSuccess = false;
    private ExecutorService threadPoolExecutor = null;

    public void setLoadSuccess(boolean loadSuccess) {
        this.loadSuccess = loadSuccess;
    }

    public void setExtractThreadPool(ExecutorService threadPoolExecutor) {
        this.threadPoolExecutor = threadPoolExecutor;
    }

    public ExecutorService getExtractThreadPool() {
        return threadPoolExecutor;
    }
}