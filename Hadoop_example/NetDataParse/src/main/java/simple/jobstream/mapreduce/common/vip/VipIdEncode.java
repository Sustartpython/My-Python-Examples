package simple.jobstream.mapreduce.common.vip;

import java.io.UnsupportedEncodingException;
import org.apache.commons.codec.binary.Base32;
import simple.jobstream.mapreduce.common.util.StringHelper;
import org.apache.commons.codec.digest.DigestUtils;

public class VipIdEncode {

	public static String encodeID(String strRaw) {
		Base32 base32 = new Base32();
		String strEncode = "";
		try {
			strEncode = base32.encodeAsString(strRaw.getBytes("utf8"));
			if (strEncode.endsWith("======")) {
				strEncode = strEncode.substring(0, strEncode.length() - 6) + "0";
			} else if (strEncode.endsWith("====")) {
				strEncode = strEncode.substring(0, strEncode.length() - 4) + "1";
			} else if (strEncode.endsWith("===")) {
				strEncode = strEncode.substring(0, strEncode.length() - 3) + "8";
			} else if (strEncode.endsWith("=")) {
				strEncode = strEncode.substring(0, strEncode.length() - 1) + "9";
			}
			strEncode = StringHelper.makeTrans("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
					"ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210", strEncode);
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		return strEncode;
	}

	public static String decodeID(String strRaw) {
		String strEncode = strRaw;
		strEncode = StringHelper.makeTrans("ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210",
				"0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ", strEncode);
		if (strEncode.endsWith("0")) {
			strEncode = strEncode + "======";
		} else if (strEncode.endsWith("1")) {
			strEncode = strEncode + "====";
		} else if (strEncode.endsWith("8")) {
			strEncode = strEncode + "===";
		} else if (strEncode.endsWith("9")) {
			strEncode = strEncode + "=";
		}

		Base32 base32 = new Base32();

		try {
			strEncode = new String(base32.decode(strEncode), "utf8");
		} catch (UnsupportedEncodingException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}

		return strEncode;
	}

	/**
	 * @author liuqingxin 2018年12月11日 下午4:35:26
	 * @param sub_db_id（5位）
	 * @param rawid
	 * @param case_insensitive 源网站rawid是否区分大小写
	 * @return
	 */
	public static String getLngid(String sub_db_id, String rawid, boolean case_insensitive) {
		String uppercase_rawid = ""; // 大写版 rawid
		if (case_insensitive) { // 源网站的 rawid 区分大小写
			char[] rawlist = rawid.toCharArray();
			for (char ch : rawlist) {
				if (Character.toUpperCase(ch) == ch) {
					uppercase_rawid += ch;
				} else {
					uppercase_rawid += Character.toUpperCase(ch) + "_";
				}
			}
		} else {
			uppercase_rawid = rawid.toUpperCase();
		}
		String limited_id = uppercase_rawid;
		if (limited_id.length() > 20) {
			limited_id = DigestUtils.md5Hex(uppercase_rawid).toUpperCase();
		} else {
			limited_id = encodeID(uppercase_rawid);
		}
		String lngid = sub_db_id + limited_id;
		return lngid;
	}

	public static void main(String[] args) {
		System.out.println(getLngid("000002", "JJYJ201809002", false));
		System.out.println(getLngid("000021", "10.1007/s35148-018-0081-9", false));
//		System.out.println(getLngid("cnki", "CJFD", "你好吗%A的ac的BC",false));
//		System.out.println(getLngid("cnki", "CJFD", "wanfangjournal_10.1515/kl-2017-frontmatter1-2",true));
//		System.out.println(getLngid("cnki", "CJFD", "gxbookanjournal@4s5lvz4ut7slrdxexs2ojpvdyk36lanf4w5lpz4ut7tljo7hrgedgnrngmydk999",false));
//		System.out.println(decodeID(encodeID("CXSTAR_TS_1b8f62c4000001XXXX")));
	}

}
