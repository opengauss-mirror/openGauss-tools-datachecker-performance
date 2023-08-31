package org.opengauss.datachecker.extract.service;

import org.opengauss.datachecker.extract.data.BaseDataService;
import org.springframework.stereotype.Service;

import javax.annotation.Resource;
import java.util.List;

/**
 * BaseManagementService
 *
 * @author ：wangchao
 * @date ：Created in 2023/7/18
 * @since ：11
 */
@Service
public class BaseManagementService {
    @Resource
    private BaseDataService baseDataService;

    public Integer fetchCheckTableCount() {
        List<String> nameList = baseDataService.queryTableNameList();
        return nameList.size();
    }
}
