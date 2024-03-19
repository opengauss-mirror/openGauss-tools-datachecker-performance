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

package org.opengauss.datachecker.common.util;

import java.math.BigInteger;

/**
 * HexUtil
 *
 * @author ：wangchao
 * @date ：Created in 2023/3/10
 * @since ：11
 */
public class HexUtil {
    private static final char[] CHARS = "0123456789ABCDEF".toCharArray();
    public static final String HEX_ZERO_PREFIX = "0x";
    public static final String HEX_PREFIX = "\\x";

    /**
     * hex openGauss prefix
     */
    public static final String HEX_OG_PREFIX = "\\";
    private static final String HEX_NO_PREFIX = "";

    /**
     * Convert text string to hexadecimal string
     *
     * @param str str
     * @return str
     */
    public static String toHex(String str) {
        StringBuilder sb = new StringBuilder("");
        byte[] bs = str.getBytes();
        int bit;
        for (int i = 0; i < bs.length; i++) {
            bit = (bs[i] & 0x0f0) >> 4;
            sb.append(CHARS[bit]);
            bit = bs[i] & 0x0f;
            sb.append(CHARS[bit]);
        }
        return sb.toString()
                 .trim();
    }

    /**
     * Convert byte array to hexadecimal string
     *
     * @param data data
     * @return data
     */
    public static String byteToHex(byte[] data) {
        StringBuilder result = new StringBuilder();
        for (byte datum : data) {
            result.append(Integer.toHexString((datum & 0xFF) | 0x100)
                                 .toUpperCase()
                                 .substring(1, 3));
        }
        return result.toString();
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 0x02AA
     *
     * @param data data
     * @return data
     */
    public static String byteToHexTrimBackslash(byte[] data) {
        return byteToHexTrim(data, HEX_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 02AA
     *
     * @param data data
     * @return data
     */
    public static String byteToHexTrim(byte[] data) {
        return byteToHexTrim(data, HEX_NO_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 0x02AA
     *
     * @param data data
     * @return data
     */
    public static String byteToHexTrimZero(byte[] data) {
        return byteToHexTrim(data, HEX_ZERO_PREFIX);
    }

    private static String byteToHexTrim(byte[] data, String prefix) {
        StringBuilder result = new StringBuilder(prefix);
        int fast = 0;
        int slow = 0;
        final int end = data.length;
        while (fast < end) {
            if (data[fast] != 0) {
                while (slow < fast) {
                    result.append(Integer.toHexString((data[slow++] & 0xFF) | 0x100)
                                         .toUpperCase()
                                         .substring(1, 3));
                }
                slow = fast;
            }
            fast++;
        }
        result.append(Integer.toHexString((data[slow] & 0xFF) | 0x100)
                             .toUpperCase()
                             .substring(1, 3));
        return result.toString();
    }

    /**
     * binary string translate to hex string
     *
     * @param binary binary
     * @return hex
     */
    public static String binaryToHex(String binary) {
        if (!isValidBinary(binary)) {
            return "";
        }
        String paddedBinary = padZeroes(binary);
        StringBuilder hexBuilder = new StringBuilder();
        for (int i = 0; i < paddedBinary.length(); i += 4) {
            char group = getBinaryToHexChar(paddedBinary.substring(i, i + 4));
            hexBuilder.append(group);
        }
        return hexBuilder.toString();
    }

    /**
     * 16进制字符串转换为二进制位串
     *
     * @param hexStream hexStream
     * @return binary
     */
    public static String hexToBinary(String hexStream) {
        byte[] bytes = new BigInteger(hexStream, 16).toByteArray();
        StringBuilder binaryStringBuilder = new StringBuilder();
        for (byte b : bytes) {
            for (int i = 7; i >= 0; i--) {
                binaryStringBuilder.append((b & (1 << i)) != 0 ? '1' : '0');
            }
        }
        return binaryStringBuilder.toString();
    }

    private static boolean isValidBinary(String binary) {
        return binary != null && !binary.isEmpty() && binary.matches("[0-1]+");
    }

    private static String padZeroes(String binary) {
        while (binary.length() % 4 != 0) {
            binary = "0" + binary;
        }
        return binary;
    }

    private static char getBinaryToHexChar(String binaryGroup) {
        return BinaryHex.hexOf(binaryGroup);
    }

    /**
     * binary hex mapping
     */
    public enum BinaryHex {
        B0000("0000", '0'), B0001("0001", '1'), B0010("0010", '2'), B0011("0011", '3'), B0100("0100", '4'), B0101(
            "0101", '5'), B0110("0110", '6'), B0111("0111", '7'), B1000("1000", '8'), B1001("1001", '9'), B1010("1010",
            'A'), B1011("1011", 'B'), B1100("1100", 'C'), B1101("1101", 'D'), B1110("1110", 'E'), B1111("1111", 'F');
        private final String name;
        private final char hexChar;

        /**
         * BinaryHex
         *
         * @param name    name
         * @param hexChar hexChar
         */
        BinaryHex(String name, char hexChar) {
            this.name = name;
            this.hexChar = hexChar;
        }

        /**
         * hexOf
         *
         * @param name name
         * @return char
         */
        public static char hexOf(String name) {
            return valueOf("B" + name).hexChar;
        }
    }
}
