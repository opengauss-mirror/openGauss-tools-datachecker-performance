/*
 * Copyright (c) Huawei Technologies Co., Ltd. 2024-2024. All rights reserved.
 */

package org.opengauss.datachecker.common.entry.enums;

import org.opengauss.datachecker.common.load.AlertLogCollectionManager;

import java.util.Locale;

/**
 * error code
 *
 * @since 2024/12/11
 */
public enum ErrorCode {
    UNKNOWN(5000, "未知异常", "Unknown error"),
    CHECK_START_ERROR(5001, "校验进程启动异常", "check process start error"),
    CHECK_SLICE_EXCEPTION(5002, "分片校验异常", "slice check exception"),
    REFRESH_SLICE_STATUS_EXCEPTION(5003, "更新分片校验异状态常", "refresh slice check status exception"),
    RULE_CONFIG_ERROR(5004, "过滤规则配置错误", "filter rule configuration error"),
    LOAD_DATABASE_CONFIG(5005, "加载数据库配置信息失败", "load database configuration information failed"),
    INCREMENT_CHECK_ERROR(5006, "增量校验任务执行异常", "increment check thread error"),
    SUMMARY_CHECK_RESULT(5007, "汇总校验结果异常", "summary check result exception"),
    BUILD_DIFF_STATEMENT(5008, "构建修复语句异常", "build diff statement exception"),
    DISPATCH_SLICE_POINT(5009, "下发分片分割点异常", "dispatch slice point exception"),
    SEND_SLICE_POINT_TIMEOUT(5010, "kafka client发送分片分割点超时", "kafka client send slice point timeout"),
    POLL_SLICE_POINT(5011, "kafka consumer拉取分片分割点异常", "kafka consumer poll slice point exception"),
    KAFKA_INIT_CONFIG(5012, "kafka初始化配置异常", "kafka init config exception"),
    KAFKA_CREATE_TOPIC(5013, "kafka创建主题失败", "kafka create topic failed"),
    CHECK_TABLE_EXCEPTION(5014, "表校验任务异常", "table check task exception"),
    PROCESS_LOG(5015, "progress日志记录异常", "process log record exception"),
    CSV_METADATA_NOT_EXIST(5016, "CSV元数据文件不存在", "csv metadata file not exist"),
    CSV_LOAD_METADATA_ERROR(5017, "CSV加载表元数据异常", "csv load metadata error"),
    REGISTER_SLICE_POINT(5018, "注册分片分割点异常", "register slice point exception"),
    CSV_TABLE_DISPATCHER(5019, "CSV表分发异常", "csv table dispatcher exception"),
    DEBEZIUM_WORKER(5020, "debezium监听异常", "debezium worker exception"),
    EXECUTE_SLICE_QUERY(5021, "执行分片查询异常", "execute slice query exception"),
    EXECUTE_SLICE_PAGE_QUERY(5022, "执行分片分页查询异常", "execute slice page query exception"),
    EXECUTE_SLICE_PROCESSOR(5023, "分片抽取异常", "execute slice processor exception"),
    TABLE_COL_NULL(5024, "查询列元数据为空", "query column metadata is null"),
    EXECUTE_QUERY_SQL(5025, "查询SQL异常", "execute query sql exception"),
    CSV_READER_LISTENER(5026, "CSV reader 监听异常", "csv reader listener exception"),
    CSV_WRITER_LISTENER(5027, "CSV writer 监听异常", "csv writer listener exception"),
    DISPATCH_CONFIG(5028, "下发配置信息异常", "dispatch config exception"),
    INCREMENT_LISTENER(5029, "增量监听异常", "increment listener exception"),
    CSV_TABLE_PROCESSOR(5030, "CSV 表数据抽取异常", "csv table processor exception"),
    KAFKA_LOG_CONFIG(5031, "kafka日志配置异常", "kafka log config exception"),
    BUILD_SLICE_POINT(5032, "生成表分割点异常", "build slice point exception"),
    ASYNC_EXTRACT_TABLE(5033, "同步抽取表信息异常", "async extract table info exception"),
    FEEDBACK_SLICE_STATUS(5034, "反馈分片抽取状态异常", "feedback slice status exception");

    private final int code;
    private final String causeCn;
    private final String causeEn;

    ErrorCode(int code, String causeCn, String causeEn) {
        this.code = code;
        this.causeCn = causeCn;
        this.causeEn = causeEn;
    }

    public int getCode() {
        return code;
    }

    public String getCauseCn() {
        return causeCn;
    }

    public String getCauseEn() {
        return causeEn;
    }

    @Override
    public String toString() {
        return getErrorPrefix();
    }

    /**
     * get error prefix
     *
     * @return String error prefix
     */
    public String getErrorPrefix() {
        AlertLogCollectionManager.handle();
        return String.format(Locale.ENGLISH, "<CODE:%d> ", code);
    }
}
