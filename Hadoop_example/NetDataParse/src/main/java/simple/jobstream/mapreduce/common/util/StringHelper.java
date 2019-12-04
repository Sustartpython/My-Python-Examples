package simple.jobstream.mapreduce.common.util;

import java.util.HashMap;
import java.util.Map;

public class StringHelper {
	/**
	 * @author liuqingxin 2018年10月31日 下午5:04:28
	 * @description 根据相应字符映射规则转换字符串
	 * @param strx   字符串映射表 原始 abc
	 * @param stry   字符串映射表 转换 123
	 * @param strRaw 需要转换的字符串 adcgb
	 * @return 转换后的字符串 1d3g2
	 */
	public static String makeTrans(String strx, String stry, String strRaw) {
		if (strx.length() != stry.length() || strx.length() == 0 || stry.length() == 0) {
			return strRaw;
		}
		String result = "";
		char[] charx = strx.toCharArray();
		char[] chary = stry.toCharArray();
		Map<String, String> maptrans = new HashMap<String, String>();
		for (int i = 0; i < chary.length; i++) {
			maptrans.put(String.valueOf(charx[i]), String.valueOf(chary[i]));
		}
		for (int i = 0; i < strRaw.length(); i++) {
			result = result + maptrans.get(String.valueOf(strRaw.charAt(i)));
		}
		return result;
	}

	/**
	 * <p>
	 * Description: 将一行页码信息解析为起始页、结束页、跳转页
	 * </p>
	 * 
	 * @author qiuhongyang 2019年5月31日 下午2:03:47
	 * @param line，3-4, 7
	 * @return {3, 4, 7}
	 */
	public static String[] parsePageInfo(String line) {
		String beginpage = "";
		String endpage = "";
		String jumppage = "";

		int idx = line.indexOf(',');
		if (idx > 0) {
			jumppage = line.substring(idx + 1).trim();
			line = line.substring(0, idx).trim(); // 去掉逗号及以后部分
		}
		idx = line.indexOf('-');
		if (idx > 0) {
			endpage = line.substring(idx + 1).trim();
			line = line.substring(0, idx).trim(); // 去掉减号及以后部分
		}
		beginpage = line.trim();
		if (endpage.length() < 1) {
			endpage = beginpage;
		}

		String[] vec = { beginpage, endpage, jumppage };
		return vec;
	}

	/**
	 * <p>
	 * Description: 清理分号。 全角分号转半角分号； 去掉分号前后的空白； 去掉最前面的分号； 去掉最后面的分号
	 * </p>
	 * 
	 * @author qiuhongyang 2018年11月8日 上午11:25:07
	 * @param text
	 * @return
	 */
	public static String cleanSemicolon(String text) {
		text = text.replace('；', ';'); // 全角分号转半角分号
		text = text.replaceAll("\\s+;", ";"); // 去掉分号前的空白
		text = text.replaceAll(";\\s+", ";"); // 去掉分号后的空白
		text = text.replaceAll(";+", ";"); // 多个分号转一个分号
		text = text.replaceAll("^;", ""); // 去掉最前面的分号
		text = text.replaceAll(";$", ""); // 去掉最后面的分号
		text = text.trim();

		return text;
	}

	/**
	 * 判断字符串是否全为数字
	 * 
	 * @author xujiang 2018年11月16日 上午11:25:07
	 * @param 传入需要判断的string
	 * @return
	 */
	public static boolean isNumeric(String str) {
		for (int i = 0; i < str.length(); i++) {
//			System.out.println(str.charAt(i));
			if (!Character.isDigit(str.charAt(i))) {
				return false;
			}
		}
		return true;
	}
}
