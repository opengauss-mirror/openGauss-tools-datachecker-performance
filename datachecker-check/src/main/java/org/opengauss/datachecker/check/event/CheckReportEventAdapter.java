package org.opengauss.datachecker.check.event;

import org.apache.logging.log4j.Logger;
import org.opengauss.datachecker.common.util.LogUtils;

import java.io.File;
import java.time.Duration;
import java.time.LocalDateTime;
import java.util.Objects;

/**
 * CheckReportEventAdapter
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/8
 * @since ：11
 */
public class CheckReportEventAdapter {
    protected static final Logger log = LogUtils.getLogger();

    /**
     * Construction verification report result root path
     *
     * @param exportCheckPath exportCheckPath
     * @return
     */
    protected String getLogRootPath(String exportCheckPath) {
        return exportCheckPath + File.separatorChar + "result" + File.separatorChar;
    }

    /**
     * Calculation and verification time
     *
     * @param start start
     * @param end   end
     * @return
     */
    protected long calcCheckTaskCost(LocalDateTime start, LocalDateTime end) {
        if (Objects.nonNull(start) && Objects.nonNull(end)) {
            return Duration.between(start, end)
                           .toMillis();
        }
        return 0;
    }
}
