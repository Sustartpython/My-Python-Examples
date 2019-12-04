package simple.jobstream.mapreduce.common.util;

import java.net.URI;
import java.net.URISyntaxException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.List;


import org.apache.commons.httpclient.URIException;
import org.apache.http.NameValuePair;
import org.apache.http.client.utils.URLEncodedUtils;

/**
 * <p>Description: 处理 url 的工具类 </p>  
 * @author qiuhongyang 2018年11月9日 上午9:26:28
 */
public class URLHelper {
	/**
	 * <p>Description: 根据 key 获取 url 参数的 value 列表 </p>  
	 * @author qiuhongyang 2018年11月9日 上午9:35:43
	 * @param url 举例：http://www.example.com/something.html?one=1&two=2&three=3&three=3a
	 * @param key 举例：three
	 * @return 有值的列表或空列表 举例：[3, 3a]
	 */
	public static ArrayList<String> getParamValueList(String  url, String key) {
		ArrayList<String> arrList = new ArrayList<String>(); 
		List<NameValuePair> params = URLEncodedUtils.parse(url, Charset.forName("UTF-8"));
		for (NameValuePair param : params) {
//			System.out.println(param.getName() + " : " + param.getValue());
			if (param.getName().equals(key)) {
				arrList.add(param.getValue());
			}
		}
		
		return arrList;
	}
	
	/**
	 * <p>Description: 根据 key 获取 url 参数的 value 的第一个值。 </p>  
	 * @author qiuhongyang 2018年11月9日 上午9:35:43
	 * @param url 举例：http://www.example.com/something.html?one=1&two=2&three=3&three=3a 或 one=1&two=2&three=3&three=3a
	 * @param key 举例：three
	 * @return 返回第一个值，不存在此键时返回空字符串 (to avoid nulls). 举例：3
	 */
	public static String getParamValueFirst(String  url, String key) {
		String value = "";
		int idx = url.indexOf('?');
		if ((idx > -1) && (url.length() > idx+1)) {
			url = url.substring(idx+1);
		}
		/*
		 * URLEncodedUtils.parse(String s, Charset charset) 参数为url参数部分，
		 * URLEncodedUtils.parse(URI uri, Charset charset) 参数为全url
		 */
		List<NameValuePair> params = URLEncodedUtils.parse(url, Charset.forName("UTF-8"));
		for (NameValuePair param : params) {
			System.out.println(param.getName() + " : " + param.getValue());
			if (param.getName().equals(key)) {
				value = param.getValue();
				break;
			}
		}
		
		return value;
	}
}
