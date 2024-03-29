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

package org.opengauss.datachecker.check.event;

import org.springframework.stereotype.Component;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * CustomEventHistory
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@Component
public class CustomEventHistory {
    private static volatile Map<CustomApplicationEvent, Boolean> events = new ConcurrentHashMap<>();

    public void addEvent(CustomApplicationEvent customEvent) {
        events.put(customEvent, false);
    }

    public void completedEvent(CustomApplicationEvent customEvent) {
        events.remove(customEvent);
    }

    public boolean checkAllEventCompleted() {
        return events.values().stream().distinct().allMatch(complete -> complete);
    }
}
