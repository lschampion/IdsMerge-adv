package com.test.utils;

import java.security.MessageDigest;
import java.security.NoSuchAlgorithmException;

public class MD5Util {
	public final static String getMD5(byte[] source) {
		String s = null;
		char hexDigits[] = { '0', '1', '2', '3', '4', '5', '6', '7', '8', '9',
				'A', 'B', 'C', 'D', 'E', 'F' };
		try {
			MessageDigest md = MessageDigest.getInstance("MD5");
			md.update(source);
			byte tmp[] = md.digest();// MD5 的计算结果是一个 128 位的长整数，
			// 用字节表示就是 16 个字节
			char str[] = new char[16 * 2];// 每个字节用 16 进制表示的话，使用两个字符， 所以表示成 16
			// 进制需要 32 个字符
			int k = 0;// 表示转换结果中对应的字符位置
			for (int i = 0; i < 16; i++) {// 从第一个字节开始，对 MD5 的每一个字节// 转换成 16
				// 进制字符的转换
				byte byte0 = tmp[i];// 取第 i 个字节
				str[k++] = hexDigits[byte0 >>> 4 & 0xf];// 取字节中高 4 位的数字转换,// >>>
				// 为逻辑右移，将符号位一起右移
				str[k++] = hexDigits[byte0 & 0xf];// 取字节中低 4 位的数字转换
			}
			s = new String(str);// 换后的结果转换为字符串

		} catch (NoSuchAlgorithmException e) {
			e.printStackTrace();
		}
		return s;
	}
	
	public static String getMD5(String strsource) {
		byte[] source=strsource.getBytes();
		String s=getMD5(source);
		return s;
	}

	public static void main(String[] args) {
		
//		long st=System.currentTimeMillis();
//		for(int i=0;i<1000000;i++){
//			String uid_a=MD5Util.getMD5("software_8198372_1194021440_YY").substring(8, 24)+"_A";
//		}
//		System.out.println(System.currentTimeMillis()-st);
//		System.out.println(MD5Util.getMD5("software_8198372_1194021440_YY").toLowerCase());
//		System.out.println(MD5Util.getMD5(""));
		System.out.println(MD5Util.getMD5("苏M5G083"));
	}
}
