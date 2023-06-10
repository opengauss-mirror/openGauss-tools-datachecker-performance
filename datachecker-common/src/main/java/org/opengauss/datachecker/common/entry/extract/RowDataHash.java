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

import static org.opengauss.datachecker.common.util.EncodeUtil.format;
import static org.opengauss.datachecker.common.util.EncodeUtil.parseInt;
import static org.opengauss.datachecker.common.util.EncodeUtil.parseLong;

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
    private static final int HEADER_LENGTH = 6;

    public RowDataHash() {
    }

    public RowDataHash(String primaryKey, String content) {
        decode(primaryKey, content);
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
     * This method implements the serialization encoding of the current object，The
     * encoding format is [head][content]
     * Head is a string with a fixed length of 8，Each 2 characters of the head
     * represents the string length of an attribute。
     * The encoding order of head is[partition,primaryKeyHash,rowHash,primaryKey]
     * content is the value of four attributes of the current object,
     * The encoding order of content is[partition,primaryKeyHash,rowHash,primaryKey]
     *
     * @return Returns the object code string
     */
    public String toEncode() {
        String[] content = new String[]{partition + "", primaryKeyHash + "", rowHash + ""};
        return format(content[0].length()) + format(content[1].length()) + format(content[2].length()) + content[0]
                + content[1] + content[2];
    }

    private void decode(String key, String content) {
        final char[] chars = content.toCharArray();
        if (chars.length < HEADER_LENGTH) {
            return;
        }
        int pos1 = HEADER_LENGTH + parseInt(chars, 0, 2);
        int pos2 = pos1 + parseInt(chars, 2, 4);
        int pos3 = pos2 + parseInt(chars, 4, 6);
        if (chars.length != pos3) {
            return;
        }
        this.setPartition(parseInt(chars, HEADER_LENGTH, pos1));
        this.setPrimaryKeyHash(parseLong(chars, pos1, pos2));
        this.setRowHash(parseLong(chars, pos2, pos3));
        this.setPrimaryKey(key);
    }
}