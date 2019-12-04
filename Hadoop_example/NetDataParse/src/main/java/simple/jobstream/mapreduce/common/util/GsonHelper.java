package simple.jobstream.mapreduce.common.util;

import java.util.ArrayList;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class GsonHelper {
	/**
	 * <p>Description: 获取 json 值，可能为一个或多个或没有此键</p>  
	 * @author qiuhongyang 2019年2月13日 上午10:26:17
	 * @param articleJsonObject com.google.gson.JsonObject对象
	 * @param jsonKey 想要取值的 key
	 * @return 若没有此key或者值为null时返回空字符串；
	 * 			若值为字符串，则返回该字符串；
	 * 			若值为字符串数组，则返回以分号分割的字符串
	 */
	public static String getJsonValue(JsonObject jsonObject, String jsonKey) {
		String line = "";

		JsonElement jsonValueElement = jsonObject.get(jsonKey);
		if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
			line = "";
		} else if (jsonValueElement.isJsonArray()) {
			for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
				line += jEle.getAsString().trim() + ";";
			}
		} else {
			line = jsonValueElement.getAsString().trim();
		}
		line = line.replaceAll(";$", ""); // 去掉末尾的分号
		line = line.trim();

		return line;
	}
	
	/**
	 * <p>Description: 将 JsonElement 转为字符串列表 </p>  
	 * @author qiuhongyang 2019年2月15日 下午5:24:47
	 * @param jsonElement
	 * @return
	 */
	public static ArrayList<String> jsonElement2Array(JsonElement jsonElement) {
		ArrayList<String> strList = new ArrayList<String>();
		if ((null == jsonElement) || jsonElement.isJsonNull()) {
			// do nothing
		} else if (jsonElement.isJsonArray()) {
			for (JsonElement jEle : jsonElement.getAsJsonArray()) {
				strList.add(jEle.getAsString());
			}
		} else {
			strList.add(jsonElement.getAsString());
		}		
		
		return strList;
	}
}
