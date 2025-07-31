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

package org.opengauss.datachecker.check.config.validator;

import org.opengauss.datachecker.common.entry.common.Rule;

import jakarta.validation.ConstraintValidator;
import java.lang.annotation.Annotation;
import java.util.List;
import java.util.regex.Pattern;

/**
 * RuleConfigValidator
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/7
 * @since ：11
 */
public interface RuleConfigValidator<A extends Annotation> extends ConstraintValidator<A, List<Rule>> {
    Pattern ROW_PATTERN = Pattern.compile("^\\d+(\\,\\d+)");
}
