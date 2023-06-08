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
import lombok.EqualsAndHashCode;
import lombok.experimental.Accessors;
import org.apache.commons.lang3.StringUtils;

import static org.opengauss.datachecker.common.util.EncodeUtil.format;
import static org.opengauss.datachecker.common.util.EncodeUtil.parseInt;
import static org.opengauss.datachecker.common.util.EncodeUtil.parseLong;
import static org.opengauss.datachecker.common.util.EncodeUtil.parse;

/**
 * RowDataHash
 *
 * @author ：wangchao
 * @date ：Created in 2022/6/1
 * @since ：11
 */
@Data
@EqualsAndHashCode
@Accessors(chain = true)
public class RowDataHash {
    public RowDataHash() {
    }

    /**
     * According to the line record encoding string, reverse construct the line record object
     *
     * @param content Line record encoding string
     */
    public RowDataHash(String content) {
        decode(content);
    }

    public RowDataHash(String primaryKey, String content) {
        decode(primaryKey,content);
    }

    /**
     * <pre>
     * If the primary key is a numeric type, it will be converted to a string.
     * If the table primary key is a joint primary key, the current attribute will be a table primary key,
     * and the corresponding values of the joint fields will be spliced. String splicing will be underlined
     * </pre>
     */
    private String primaryKey;

    /**
     * Hash value of the corresponding value of the primary key
     */
    private long primaryKeyHash;
    /**
     * Total hash value of the current record
     */
    private long rowHash;

    private int partition;

    /**
     * This method implements the serialization encoding of the current object，The encoding format is [head][content]
     * Head is a string with a fixed length of 8，Each 2 characters of the head represents the string length of an attribute。
     * The encoding order of head is[partition,primaryKeyHash,rowHash,primaryKey]
     * content is the value of four attributes of the current object,
     * The encoding order of content is[partition,primaryKeyHash,rowHash,primaryKey]
     *
     * @return Returns the object code string
     */
    public String toEncode() {
        return encode();
    }

    private String getHeader(String[] content) {
        String header = "";
        for (int i = 0; i < content.length; i++) {
            header += format(content[i].length());
        }
        return header;
    }

    private String encode() {
        String[] content = new String[] {partition + "", primaryKeyHash + "", rowHash + "", primaryKey};
        String header = getHeader(content);
        return header + StringUtils.join(content, "");
    }
    private void decode(String key ,String content) {
        final char[] chars = content.toCharArray();
        if (chars.length < 8) {
            return;
        }
        int pos1 = parseInt(chars, 0, 2);
        int pos2 = parseInt(chars, 2, 4);
        int pos3 = parseInt(chars, 4, 6);
        int pos4 = parseInt(chars, 6, 8);
        if (chars.length != (8 + pos1 + pos2 + pos3 + pos4)) {
            return;
        }
        this.setPartition(parseInt(chars, 8, 8 + pos1));
        this.setPrimaryKeyHash(parseLong(chars, 8 + pos1, 8 + pos1 + pos2));
        this.setRowHash(parseLong(chars, 8 + pos1 + pos2, 8 + pos1 + pos2 + pos3));
        this.setPrimaryKey(key);
    }
    private void decode(String content) {
        final char[] chars = content.toCharArray();
        if (chars.length < 8) {
            return;
        }
        int pos1 = parseInt(chars, 0, 2);
        int pos2 = parseInt(chars, 2, 4);
        int pos3 = parseInt(chars, 4, 6);
        int pos4 = parseInt(chars, 6, 8);
        if (chars.length != (8 + pos1 + pos2 + pos3 + pos4)) {
            return;
        }
        this.setPartition(parseInt(chars, 8, 8 + pos1));
        this.setPrimaryKeyHash(parseLong(chars, 8 + pos1, 8 + pos1 + pos2));
        this.setRowHash(parseLong(chars, 8 + pos1 + pos2, 8 + pos1 + pos2 + pos3));
        this.setPrimaryKey(parse(chars, 8 + pos1 + pos2 + pos3, 8 + pos1 + pos2 + pos3 + pos4));
    }
}