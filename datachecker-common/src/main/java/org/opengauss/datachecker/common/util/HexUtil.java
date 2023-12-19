package org.opengauss.datachecker.common.util;

/**
 * @author ：wangchao
 * @date ：Created in 2023/3/10
 * @since ：11
 */
public class HexUtil {
    private static final char[] CHARS = "0123456789ABCDEF".toCharArray();
    public static final String HEX_ZERO_PREFIX = "0x";
    public static final String HEX_PREFIX = "\\x";
    private static final String HEX_NO_PREFIX = "";

    /**
     * Convert text string to hexadecimal string
     *
     * @param str str
     * @return
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
     * @return
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
     * @return
     */
    public static String byteToHexTrimBackslash(byte[] data) {
        return byteToHexTrim(data, HEX_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 02AA
     *
     * @param data data
     * @return
     */
    public static String byteToHexTrim(byte[] data) {
        return byteToHexTrim(data, HEX_NO_PREFIX);
    }

    /**
     * Clear the 0 at the end of the byte array and convert valid values to hexadecimal strings
     * 02AA -> 0x02AA
     *
     * @param data data
     * @return
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
        return BinaryHex.valueOf(binaryGroup).hexChar;
    }

    /**
     * binary hex mapping
     */
    enum BinaryHex {
        B0000("0000", '0'), B0001("0001", '1'), B0002("0010", '2'), B0003("0011", '3'), B0004("0100", '4'), B0005(
            "0101", '5'), B0006("0110", '6'), B0007("1000", '7'), B0008("0001", '8'), B0009("1001", '9'), B00010("1010",
            'A'), B00011("1011", 'B'), B00012("1100", 'C'), B00013("1101", 'D'), B00014("1110", 'E'), B00015("1111",
            'F');
        private final String code;
        private final char hexChar;

        BinaryHex(String code, char hexChar) {
            this.code = code;
            this.hexChar = hexChar;
        }
    }
}
