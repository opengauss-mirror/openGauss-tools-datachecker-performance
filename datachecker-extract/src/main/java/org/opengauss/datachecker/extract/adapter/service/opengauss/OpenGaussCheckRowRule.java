package org.opengauss.datachecker.extract.adapter.service.opengauss;

import org.opengauss.datachecker.extract.adapter.service.CheckRowRuleService;
import org.springframework.stereotype.Service;

/**
 * OpenGaussCheckRowRule
 *
 * @author ：wangchao
 * @date ：Created in 2022/12/2
 * @since ：11
 */
@Service
public class OpenGaussCheckRowRule extends CheckRowRuleService {

    @Override
    protected String convert(String text) {
        return "\"" + text + "\"";
    }

    @Override
    protected String convertCondition(String text) {
        return text;
    }
}
