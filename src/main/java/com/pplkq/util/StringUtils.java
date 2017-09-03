package com.pplkq.util;

public class StringUtils {

	public static String join(final char separator, Object...objects) {
		if(null == objects) {
			return null;
		}
		StringBuffer tmp = new StringBuffer();
		for(Object obj : objects) {
			tmp.append(obj).append(separator);
		}
		tmp.setLength(tmp.length()-1);
		return tmp.toString();
	}
	
}
