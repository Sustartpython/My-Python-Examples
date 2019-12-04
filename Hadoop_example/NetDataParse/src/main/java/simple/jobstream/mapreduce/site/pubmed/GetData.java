package simple.jobstream.mapreduce.site.pubmed;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.JsonElement;
import com.google.gson.JsonObject;

public class GetData {
	// 获取 json 值，可能为一个或多个或没有此键
	static String getJsonValue(JsonObject articleJsonObject, String jsonKey, String dictkey) {
		String line = "";

		JsonElement jsonValueElement = articleJsonObject.get(jsonKey);
		if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
			line = "";
		} else if (jsonValueElement.isJsonArray()) {
			for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {

				if (jEle.getAsJsonObject().has(dictkey)) {
					line += jEle.getAsJsonObject().get(dictkey).getAsString().trim() + ";";
				}

			}
		} else {
			if (jsonValueElement.getAsJsonObject().has(dictkey)) {
				line = jsonValueElement.getAsJsonObject().get(dictkey).getAsString().trim();
			}

		}
		line = line.replaceAll(";$", ""); // 去掉末尾的分号
		line = line.trim();

		return line;
	}

	// 获取journal相关数据,issn,title等信息
	static String getData(JsonObject articleObjt, String jsonKey, String dictkey, String textkey) {
		String result = "";
		if (articleObjt.has(jsonKey)) {
			// 创建journal相关信息object
			if (dictkey.length() != 0) {
				JsonObject objc = articleObjt.get(jsonKey).getAsJsonObject();
				if (textkey.length() != 0) { // 获取issn
					if (objc.has(dictkey)) {
						if (objc.get(dictkey).getAsJsonObject().has(textkey)) {
							result = objc.get(dictkey).getAsJsonObject().get(textkey).getAsString();
						}
					}
				} else {
					if (objc.has(dictkey)) {
						// 判断是否是列表,获取dictkey对象
						JsonElement dicEle = articleObjt.get(jsonKey).getAsJsonObject().get(dictkey);
						if (!dicEle.isJsonNull()) {

							if (dicEle.isJsonArray()) {
								for (JsonElement jEle : dicEle.getAsJsonArray()) {
									String Abstract_before = "";
									String Abstract_Text = "";
									if (!jEle.isJsonNull()) {
										if (jEle.isJsonPrimitive()) {
											result += jEle.getAsString() + ";";
											continue;
										}
										if (jEle.getAsJsonObject().has("@NlmCategory")) {
											Abstract_before = jEle.getAsJsonObject().get("@NlmCategory").getAsString();
										}
										if (jEle.getAsJsonObject().has("#text")) {
											Abstract_Text = jEle.getAsJsonObject().get("#text").getAsString();
										}
										if (Abstract_before.length() != 0) {
											result += Abstract_before + ":" + Abstract_Text + ";";
										} else {
											result += Abstract_Text + ";";
										}
									}

								}
							} else {
								if (dicEle.isJsonPrimitive()) {
									result = dicEle.getAsString();

								} else {
									if (dicEle.getAsJsonObject().has("#text")) {

										result = dicEle.getAsJsonObject().get("#text").getAsString();
									}
								}
							}
						}
					}
				}
			} else {
				if (!articleObjt.get(jsonKey).isJsonNull()) {

					if (articleObjt.get(jsonKey).isJsonPrimitive()) {
						result = articleObjt.get(jsonKey).getAsString();

					}
					if (!articleObjt.get(jsonKey).isJsonPrimitive() && !articleObjt.get(jsonKey).isJsonArray()) {

						if (articleObjt.get(jsonKey).getAsJsonObject().has("#text")) {
							result = articleObjt.get(jsonKey).getAsJsonObject().get("#text").getAsString();

						}

					}

				}
			}
		}
		return result.replaceAll(";$", "");
	}

	// 获取作者与机构相关数据
	static LinkedHashMap getAuthorOrgan(JsonElement jsonAuthorElement, String jsonKey, String dictkey, String textkey) {

		LinkedHashMap params = new LinkedHashMap();

		if (jsonAuthorElement.isJsonArray()) {

			for (JsonElement jEle : jsonAuthorElement.getAsJsonArray()) {
				String organ_str = "";
				String author_str = "";
				String xing_str = "";
				String ming_str = "";

				ming_str = getData(jEle.getAsJsonObject(), "LastName", "", "").trim().replace("[", "").replace("]", "")
						.replace("/", "");

				xing_str = getData(jEle.getAsJsonObject(), "ForeName", "", "").trim().replace("[", "").replace("]", "")
						.replace("/", "").replace("-", "");
				author_str = xing_str + " " + ming_str;

				if (jEle.getAsJsonObject().has("AffiliationInfo")) {
					// 获取机构对象
					JsonElement organElement = jEle.getAsJsonObject().get("AffiliationInfo");
					// 判断是否含有多个机构
					if (organElement.isJsonArray()) {
						// 循环取出机构
						for (JsonElement OrganElt : organElement.getAsJsonArray()) {
							// 取出其中一个机构值
							String OrganOne = getData(OrganElt.getAsJsonObject(), "Affiliation", "", "").trim()
									.replace(";", "");
							organ_str += OrganOne.trim() + ";";
						}
					} else {
						// 一个作者只对应一个机构时
						String OrganOne = getData(organElement.getAsJsonObject(), "Affiliation", "", "").trim()
								.replace(";", "");
						organ_str = OrganOne.trim() + ";";
					}

					// 将作者与机构放入map中去
					if (author_str.length() != 0) {
						params.put(author_str.replaceAll(";$", ""), organ_str.replaceAll(";$", ""));

					}

				} else {
					// 无机构标签的时候
					params.put(author_str.replaceAll(";$", ""), organ_str);
				}
			}

		} else {
			// 当author标签不是列表的时候
			String organ_str = "";
			String author_str = "";
			String xing_str = "";
			String ming_str = "";

			ming_str = getData(jsonAuthorElement.getAsJsonObject(), "LastName", "", "").trim().replace("[", "")
					.replace("]", "").replace("/", "");
			xing_str = getData(jsonAuthorElement.getAsJsonObject(), "ForeName", "", "").trim().replace("[", "")
					.replace("]", "").replace("/", "");

			author_str = xing_str + " " + ming_str;

			if (jsonAuthorElement.getAsJsonObject().has("AffiliationInfo")) {
				// 获取机构对象
				JsonElement organElement = jsonAuthorElement.getAsJsonObject().get("AffiliationInfo");
				// 判断是否含有多个机构
				if (organElement.isJsonArray()) {
					// 循环取出机构
					for (JsonElement OrganElt : organElement.getAsJsonArray()) {
						//取出机构的具体某一值
						String OrganOne = getData(OrganElt.getAsJsonObject(), "Affiliation", "", "").trim().replace(";",
								"");
						organ_str += OrganOne.trim() + ";";
					}
				} else {
					String OrganOne = getData(organElement.getAsJsonObject(), "Affiliation", "", "").trim().replace(";",
							"");
					organ_str = OrganOne.trim() + ";";
				}

				// 将作者与机构放入map中去
				if (author_str.length() != 0) {
					params.put(author_str.replaceAll(";$", ""), organ_str.replaceAll(";$", ""));
				}
			} else {
				// 无机构标签的时候
				params.put(author_str.replaceAll(";$", ""), organ_str);
			}
		}
		return params;
	}

	// 获取journal日期相关数据
	static String getPubmedData(JsonObject currentobjc) {
		String curr_year = "";
		String curr_month = "";
		String curr_day = "";
		String date = "";
		{
			if (currentobjc.has("Year")) {
				curr_year = currentobjc.get("Year").getAsString();
			}
			if (currentobjc.has("Month")) {
				curr_month = currentobjc.get("Month").getAsString();
			}
			if (currentobjc.has("Day")) {
				curr_day = currentobjc.get("Day").getAsString();
			}
			if (curr_year.equals("")) {
				curr_year = "1900";
			}
			if (curr_month.equals("")) {

				curr_month = "00";
			}
			if (curr_day.equals("")) {

				curr_day = "00";
			}
			if (curr_month.length() == 1) {
				curr_month = "0" + curr_month;
			}
			if (curr_day.length() == 1) {
				curr_day = "0" + curr_day;
			}

			date = curr_year + curr_month + curr_day;
		}
		return date;
	}

	// 获取文章相关id,doi,pmc相关数据
	static String getPubmedId(JsonObject Idobjc, String key) {
		String Id = "";
		Id = getData(Idobjc, key, "", "");
		return Id;
	}

	// 获取orcid
	static String getorcId(JsonElement jsonAuthorinfoElement, String jsonKey, String dictkey, String textkey) {

		LinkedHashMap params = new LinkedHashMap();
		String lines = "";

		if (jsonAuthorinfoElement.isJsonArray()) {

			for (JsonElement jEle : jsonAuthorinfoElement.getAsJsonArray()) {
				String orcid = "";
				String xing_str = "";
				String ming_str = "";
				String author_str = "";
				String orc = "";

				if (jEle.getAsJsonObject().has("Identifier")) {
					// 获取orcid
					JsonElement orcidElement = jEle.getAsJsonObject().get("Identifier");
					if (orcidElement.isJsonArray()) {

						for (JsonElement orcElement : orcidElement.getAsJsonArray()) {

							if (orcElement.getAsJsonObject().has("#text")) {

								orcid = getData(orcElement.getAsJsonObject(), "#text", "", "").trim()
										.replace("http://orcid.org/", "").replace("https://orcid.org/", "");
								xing_str = getData(jEle.getAsJsonObject(), "ForeName", "", "").trim().replace("[", "")
										.replace("]", "").replace("/", "");
								ming_str = getData(jEle.getAsJsonObject(), "LastName", "", "").trim().replace("[", "")
										.replace("]", "").replace("/", "").replace("-", "");
								author_str = xing_str + " " + ming_str;

								if (orcid.length() > 0 && author_str.length() > 0) {

									orc = orcid + "@" + author_str;

									lines += orc + ";";
								}
							}
						}

					} else {
						orcid = getData(jEle.getAsJsonObject(), "Identifier", "#text", "").trim()
								.replace("http://orcid.org/", "").replace("https://orcid.org/", "");
						xing_str = getData(jEle.getAsJsonObject(), "ForeName", "", "").trim().replace("[", "")
								.replace("]", "").replace("/", "");
						ming_str = getData(jEle.getAsJsonObject(), "LastName", "", "").trim().replace("[", "")
								.replace("]", "").replace("/", "").replace("-", "");
						author_str = xing_str + " " + ming_str;

						if (orcid.length() > 0 && author_str.length() > 0) {

							orc = orcid + "@" + author_str;

							lines += orc + ";";

						}
					}
				}
			}
		} else {
			String orcid = "";
			String xing_str = "";
			String ming_str = "";
			String author_str = "";
			String orc = "";
			String organ_str = "";
			if (jsonAuthorinfoElement.getAsJsonObject().has("Identifier")) {
				// 获取orcid
				JsonElement orcidElement = jsonAuthorinfoElement.getAsJsonObject().get("Identifier");
				if (orcidElement.isJsonArray())
					for (JsonElement OrcEle : orcidElement.getAsJsonArray()) {
						if (OrcEle.getAsJsonObject().has("#text")) {

							orcid = getData(OrcEle.getAsJsonObject(), "#text", "", "").trim()
									.replace("http://orcid.org/", "").replace("https://orcid.org/", "");
						}
					}
				else {
					if (jsonAuthorinfoElement.getAsJsonObject().get("Identifier").getAsJsonObject().has("#text")) {
						orcid = getData(jsonAuthorinfoElement.getAsJsonObject(), "Identifier", "#text", "").trim()
								.replace("http://orcid.org/", "").replace("https://orcid.org/", "");
					}
				}
				if (jsonAuthorinfoElement.getAsJsonObject().has("ForeName")) {
					xing_str = getData(jsonAuthorinfoElement.getAsJsonObject(), "ForeName", "", "").trim()
							.replace("[", "").replace("]", "").replace("/", "");
				}
				if (jsonAuthorinfoElement.getAsJsonObject().has("LastName")) {
					ming_str = getData(jsonAuthorinfoElement.getAsJsonObject(), "LastName", "", "").trim()
							.replace("[", "").replace("]", "").replace("/", "").replace("-", "");
				}

				author_str = xing_str + " " + ming_str;
				author_str = getData(jsonAuthorinfoElement.getAsJsonObject(), "LastName", "", "").trim();
				if (orcid.length() > 0 && author_str.length() > 0) {
					orc = orcid + "@" + author_str;
					lines += orc + ";";
				}
			}
		}
		return lines;
	}

}