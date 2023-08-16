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

import lombok.EqualsAndHashCode;
import lombok.Getter;
import org.opengauss.datachecker.common.util.IdGenerator;
import org.springframework.context.ApplicationEvent;

/**
 * CustomApplicationEvent
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/7
 * @since ：11
 */
@EqualsAndHashCode(of = "eventId", callSuper = false)
@Getter
public class CustomApplicationEvent extends ApplicationEvent {
    private String eventId;
    private String message;

    public CustomApplicationEvent(Object source, String message) {
        super(source);
        this.eventId = IdGenerator.nextId36();
        this.message = message;
    }
}
