package org.opengauss.datachecker.common.entry.common;

import lombok.Data;
import org.opengauss.datachecker.common.entry.enums.DataBaseType;
import org.opengauss.datachecker.common.entry.enums.Endpoint;

/**
 * @author ：wangchao
 * @date ：Created in 2023/6/11
 * @since ：11
 */
@Data
public class ExtractContext {
    private  Endpoint endpoint;
    private  DataBaseType databaseType;
    private  String schema;
    private  int maximumTableSliceSize;
}
