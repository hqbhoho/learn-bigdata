package com.hqbhoho.bigdata.guava.io;

import com.google.common.base.Preconditions;
import com.google.common.io.BaseEncoding;

/**
 * describe:
 * <p>
 * Base64 encode and decode
 *
 * @author hqbhoho
 * @version [v1.0]
 * @date 2019/09/26
 */
public class Base64 {

    private final static String BASE64CHARS = "ABCDEFGHIJKLMNOPQRSTUVWXYZabcdefghijklmnopqrstuvwxyz0123456789+/";

    public static String encode(String input) {
        String binaryString = toBinaryString(input);
        StringBuilder sb = new StringBuilder();
        // 按照每6位截取一段字符串，最后一个不足6位的补0
        int remainder = binaryString.length() % 6;
        for (int i = 0; i < 6 - remainder; i++) {
            binaryString += "0";
        }
        for (int i = 0; i < binaryString.length(); i += 6) {
            String temp = binaryString.substring(i, i + 6);
            // 将6位二进制字符串转化成十进制数
            Integer index = Integer.valueOf(temp, 2);
            sb.append(BASE64CHARS.charAt(index));
        }

        // 每添加2个0  就加一个=
        for (int i = 0; i < (6 - remainder) / 2; i++) {
            sb.append('=');
        }
        return sb.toString();
    }

    /**
     * 将一个字符串翻译转化成二进制字符串(char : 8bit)
     * eg:
     * "ab" ---> "01100001 01100010"
     *
     * @param input
     * @return
     */
    private static String toBinaryString(String input) {

        Preconditions.checkNotNull(input, "input can't be null!");
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < input.length(); i++) {
            char ch = input.charAt(i);
            String tempBinaryString = Integer.toBinaryString(ch);
            for (int j = 0; j < 8 - tempBinaryString.length(); j++) {
                sb.append("0");
            }
            sb.append(tempBinaryString);
        }
        return sb.toString();
    }

    public static String decode(String input) {
        String base64String = toBase64String(input);
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < base64String.length() - 8; i += 8) {
            String temp = base64String.substring(i, i + 8);
            char ch = (char) Integer.valueOf(temp, 2).intValue();
            sb.append(ch);
        }
        return sb.toString();

    }

    /**
     * Base64加密后的字符串穿换成二进制字符串（6bit）
     * eg：
     * Y ---> 011000
     *
     * @param input
     * @return
     */
    private static String toBase64String(String input) {
        Preconditions.checkNotNull(input, "input can't be null!");
        StringBuilder sb = new StringBuilder();
        String temp = input;
        if(input.contains("=")){
             temp = input.substring(0, input.indexOf("="));
        }
        for (int i = 0; i < temp.length(); i++) {
            char ch = temp.charAt(i);
            int index = BASE64CHARS.indexOf(ch);
            String tempBase64String = Integer.toBinaryString(index);
            for (int j = 0; j < 6 - tempBase64String.length(); j++) {
                sb.append("0");
            }
            sb.append(tempBase64String);
        }
        return sb.toString();
    }

    public static void main(String[] args) {

        String s = "asdf";
        String encode = Base64.encode(s);
        System.out.println(encode);
        System.out.println(Base64.decode(encode));

        // Base64 encode and decode with guava

        System.out.println(BaseEncoding.base64().encode(s.getBytes()));
        System.out.println(new String(BaseEncoding.base64().decode(encode)));


    }
}
