package simple.jobstream.mapreduce.user.walker.sd_qk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.PrintWriter;
import java.io.StringWriter;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Mapper;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.GsonHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;

public abstract class SciencedirectMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

	int cnt = 0;
	int sourceNullCnt = 0;
	int issnNullCnt = 0;
	String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
	String journalInfoFile = "";
	Map<String, String> mapMonth = new HashMap<String, String>();
	// 期刊信息
	HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();
	Context m_context = null;

	String lngid = "";
	String rawid = "";
	String sub_db_id = "";
	String product = "";
	String sub_db = "";
	String provider = "";
	String down_date = "";
	String batch = "";
	String translator = "";
	String translator_intro = "";
	String ori_src = "";
	String doi = "";
	String source_type = "";
	String provider_url = "";
	String title = "";
	String title_alt = "";
	String title_sub = "";
	String title_series = "";
	String keyword = "";
	String keyword_alt = "";
	String keyword_machine = "";
	String clc_no_1st = "";
	String clc_no = "";
	String clc_machine = "";
	String subject_word = "";
	String subject_edu = "";
	String subject = "";
	String abstract_ = "";
	String abstract_alt = "";
	String abstract_type = "";
	String abstract_alt_type = "";
	String page_info = "";
	String begin_page = "";
	String end_page = "";
	String jump_page = "";
	String doc_code = "";
	String doc_no = "";
	String raw_type = "";
	String recv_date = "";
	String accept_date = "";
	String revision_date = "";
	String pub_date = "";
	String pub_date_alt = "";
	String pub_place = "";
	String coden = "";
	String page_cnt = "";
	String pdf_size = "";
	String fulltext_txt = "";
	String fulltext_addr = "";
	String fulltext_type = "";
	String column_info = "";
	String fund = "";
	String fund_id = "";
	String fund_alt = "";
	String author_id = "";
	String author_1st = "";
	String author = "";
	String author_raw = "";
	String author_alt = "";
	String corr_author = "";
	String corr_author_id = "";
	String email = "";
	String subject_major = "";
	String research_field = "";
	String contributor = "";
	String contributor_id = "";
	String contributor_alt = "";
	String author_intro = "";
	String organ_id = "";
	String organ_1st = "";
	String organ = "";
	String organ_alt = "";
	String preferred_organ = "";
	String host_organ_id = "";
	String organ_area = "";
	String journal_raw_id = "";
	String journal_name = "";
	String journal_name_alt = "";
	String pub_year = "";
	String vol = "";
	String num = "";
	String is_suppl = "";
	String issn = "";
	String eissn = "";
	String cnno = "";
	String isbn = "";
	String publisher = "";
	String cover_path = "";
	String is_oa = "";
	String country = "";
	String language = "";
	String ref_cnt = "";
	String ref_id = "";
	String cited_id = "";
	String cited_cnt = "";
	String down_cnt = "";
	String orc_id = "";
	String researcher_id = "";
	String is_topcited = "";
	String is_hotpaper = "";
	String pubmed_id = "";
	String pmc_id = "";

	final int curYear = Calendar.getInstance().get(Calendar.YEAR);

	protected final void setup(Context context) throws IOException, InterruptedException {
		m_context = context;

		initMapMonth(); // 初始化月份map
		setJournalInfoFile();
		initJournalInfo(context);

		batch = context.getConfiguration().get("batch");
	}

	// 设置期刊信息文件路径
	abstract void setJournalInfoFile();
	
	// 初始化期刊信息
	final void initJournalInfo(Context context) throws IOException {
		// 获取HDFS文件系统
		FileSystem fs = FileSystem.get(context.getConfiguration());

		FSDataInputStream fin = fs.open(new Path(journalInfoFile));
		BufferedReader in = null;
		String line;
		try {
			in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			while ((line = in.readLine()) != null) {
				line = line.trim();
				if (line.length() < 1) {
					continue;
				}
				Gson gson = new Gson();
				Type type = new TypeToken<HashMap<String, String>>() {
				}.getType();
				HashMap<String, String> mapField = gson.fromJson(line, type);
				journalInfo.put(mapField.get("jid"), mapField);
			}
		} finally {
			if (in != null) {
				in.close();
			}
		}
		System.out.println("journalInfo size: " + journalInfo.size());
	}

	public final void initMapMonth() {
		mapMonth.put("january", "01");
		mapMonth.put("february", "02");
		mapMonth.put("march", "03");
		mapMonth.put("april", "04");
		mapMonth.put("may", "05");
		mapMonth.put("june", "06");
		mapMonth.put("july", "07");
		mapMonth.put("august", "08");
		mapMonth.put("september", "09");
		mapMonth.put("october", "10");
		mapMonth.put("november", "11");
		mapMonth.put("december", "12");

		mapMonth.put("spring", "03");
		mapMonth.put("summer", "06");
		mapMonth.put("autumn", "09");
		mapMonth.put("winter", "12"); // http://www.sciencedirect.com/science/article/pii/S0251108883800370

		mapMonth.put("1st quarter", "03"); // http://www.sciencedirect.com/science/article/pii/S1084856801000451
		mapMonth.put("2nd quarter", "06");
		mapMonth.put("3rd quarter", "09");
		mapMonth.put("4th quarter", "12"); // http://www.sciencedirect.com/science/article/pii/S0251108883800370
	}

	final boolean isNumeric(String str) {
		Pattern pattern = Pattern.compile("[0-9]*");
		Matcher isNum = pattern.matcher(str);
		if (!isNum.matches()) {
			return false;
		}
		return true;
	}

	// 获取 pub_year，pub_date
	final void getPubDate(String line) {
		// System.out.println("getPubDate line:" + line);
		for (String item : line.split(",")) {
			item = item.trim();
			// S0007153612800064: 1912–1913
			// S2542568417000101: 2017
			// S0368174295800706: 1895
			Pattern pattern = Pattern.compile("^((18|19|20)\\d{2})(–(18|19|20)\\d{2})?$");
			Matcher matcher = pattern.matcher(item);
			if (matcher.find()) { // 只有年
				pub_year = item.replaceAll("^((18|19|20)\\d{2})(–(18|19|20)\\d{2})?$", "$1");
				pub_date = pub_year + "0000";
				break;
			}

			boolean flag = false;
			for (String month : mapMonth.keySet()) { // 找月
				if (item.toLowerCase().indexOf(month) > -1) {
					flag = true;
					break;
				}
			}

			if (!flag) {
				continue;
			}

			String year = "1900";
			String month = "00";
			String day = "00";
			for (Map.Entry<String, String> entry : mapMonth.entrySet()) {
				line = item.toLowerCase().trim(); // 29 April 1961
				int idx = line.indexOf(entry.getKey());
				if (idx > -1) {
					month = entry.getValue();
					if (idx > 0) {
						String tmp = line.substring(0, idx).trim();
						if (isNumeric(tmp)) {
							if (Integer.parseInt(tmp) <= 31) { // 是数字并且小等于31
								// 0 代表前面补充0
								// 4 代表长度为4
								// d 代表参数为正数型
								day = String.format("%02d", Integer.parseInt(tmp));
							}
						}
					}
					break;
				}
			}
			pattern = Pattern.compile("^.*(\\d{4})$");
			matcher = pattern.matcher(item);
			if (matcher.find()) {
				year = matcher.group(1).trim();
			}

			pub_year = year;
			pub_date = year + month + day;
			break;
		}

//		System.out.println("pub_year:" + pub_year);
//		System.out.println("pub_date:" + pub_date);
	}

	public final boolean parseArticleTitle(String rawid, Document doc) {

		// json 字符串
		String jsonString = "";
		JsonObject jsonObj = null;
		Element eleJson = doc.select("script[type=application/json]").first();
		if (eleJson != null) {
			jsonString = eleJson.html().trim();
			jsonObj = new JsonParser().parse(jsonString).getAsJsonObject();
		}

		Element eleArticleContent = doc.select("div.article-wrapper").first();
		if (eleArticleContent == null) {
			// System.out.println("eleArticleContent == null");
			return false;
		}

		/**************************** begin title ****************************/
		{
			title = title_alt = "";

			Element eleTitle = eleArticleContent.select("h1.article-title").first();
			Element eleDochead = eleArticleContent.select("div.Head > div.article-dochead").first();
			if (eleTitle != null) {
				title = eleTitle.text().trim();
			} else if (eleDochead != null) {
				title = eleDochead.text().trim();
			}

			if (title.length() < 1) {
				title = doc.title().trim();
				if (title.endsWith("- ScienceDirect")) {
					title = title.substring(0, title.length() - "- ScienceDirect".length()).trim();
				}
			}

			title = title.replaceAll("[ ☆]*$", ""); // 删除末尾的☆
			if (title.length() < 1) {
				return false;
			}

			Element eleTitle_alt = eleArticleContent.select("div.article-title-alt").first();
			if (eleTitle_alt != null) {
				title_alt = eleTitle_alt.text().trim();
			}

//			System.out.println("title:" + title);
//			System.out.println("title_alt:" + title_alt);	
		}
		/**************************** end title ****************************/

		/******************************
		 * begin author organ
		 ********************************/
		{
			author = "";
			organ = "";
			Element eleAuthorGroup = eleArticleContent.select("div.AuthorGroups > div.author-group").first();
			if (eleAuthorGroup != null) {
				for (Element eleAuthor : eleAuthorGroup.select("a > span.content")) {
					Element spanGivenName = eleAuthor.select("span.text.given-name").first();
					Element spanSurName = eleAuthor.select("span.text.surname").first();

//					System.out.println("spanGivenName:" + spanGivenName.toString());
//					System.out.println("spanSurName:" + spanSurName.toString());

					String auItem = "";
					if (spanGivenName != null) {
						auItem = spanGivenName.text().trim();
					}
					if (spanSurName != null) {
						auItem += " " + spanSurName.text().trim();
					}

					String sup = "";
//					for (Element eleSup : eleAuthor.select("span.author-ref > sup")) {
					for (Element eleSup : eleAuthor.select("span.author-ref")) {
//						System.out.println("eleSup:" + eleSup.text());
						sup += eleSup.text().replace(';', '☆').trim() + ","; // 替换掉标号中的分号
					}
					sup = sup.replaceAll(",+?$", ""); // 去掉尾部多余逗号

					if (sup.length() > 0) {
						auItem += "[" + sup + "]";
					}

					author += auItem + ";";
				}

				author = author.replaceAll("[ ;]+?$", ""); // 去掉尾部多余的分号
				author = author.replaceAll("\\[\\]$", ""); // 去掉尾部空的中括号

				if (jsonObj != null) {
					try {
						JsonObject organObj = jsonObj.get("authors").getAsJsonObject().get("affiliations")
								.getAsJsonObject();

						Type type = new TypeToken<Map<String, JsonObject>>() {
						}.getType();
						Gson gson = new Gson();
						Map<String, JsonObject> mapJson = gson.fromJson(organObj, type);
						for (JsonObject obj : mapJson.values()) {
							JsonArray array = obj.get("$$").getAsJsonArray();
							// System.out.println(rawid + " array:" + array);
							if (array.get(0).getAsJsonObject().get("#name").getAsString().equals("label")) { // 多个机构
								String orgItem = "[" + array.get(0).getAsJsonObject().get("_").getAsString() + "]"
										+ array.get(1).getAsJsonObject().get("_").getAsString();
								organ += orgItem + ";";
							} else if (array.get(0).getAsJsonObject().get("#name").getAsString().equals("textfn")) { // 单个机构
								organ = array.get(0).getAsJsonObject().get("_").getAsString().trim();
							}

						}
					} catch (Exception e) {
						StringWriter sw = new StringWriter();
						e.printStackTrace(new PrintWriter(sw, true));
						String text = "Errror:" + rawid + "\t" + sw.toString();
						LogMR.log2HDFS4Mapper(m_context, logHDFSFile, text);
					}
				}
				organ = organ.replaceAll(";+?$", ""); // 去掉尾部多余分号
			}

			String[] vec = author.split(";");
			if (vec.length > 0) {
				author_1st = vec[0].trim();
			}
			vec = organ.split(";");
			if (vec.length > 0) {
				organ_1st = vec[0].trim();
			}

//			System.out.println("***author:" + author);
//			System.out.println("***organ:" + organ);

			// 去掉第一作者后面的编号
			author_1st = author_1st.replaceAll("(.+)\\[[^\\]]+\\]", "$1");
			// 去掉第一机构前面的编号
			organ_1st = organ_1st.replaceAll("\\[[^\\]]+\\](.+)", "$1");
			// 将作者机构以数字编号
			try {
				vec = AuthorOrgan.renumber(author, organ);
				author = vec[0];
				organ = vec[1];
			} catch (Exception e) {
				LogMR.log2HDFS4Mapper(m_context, logHDFSFile, "except renuber" + rawid + "\t" + author + "\t" + organ);
				m_context.getCounter("map", "except renuber").increment(1);
			}

//			System.out.println("author:" + author);
//			System.out.println("author_1st:" + author_1st);
//			System.out.println("organ:" + organ);
//			System.out.println("organ_1st:" + organ_1st);
		}
		/*************************** end author organ **************************/

		/*************************** begin keyword **************************/
		{
			keyword = "";
			Element eleKeywords = eleArticleContent.select("div.Keywords").first();
			if (eleKeywords != null) {
				for (Element divKeyord : eleKeywords.select("div.keyword")) {
					keyword += divKeyord.text().trim() + ";";
				}
			}
			keyword = keyword.replaceAll(";+?$", ""); // 去掉尾部多余分号
//			System.out.println("keyword:" + keyword);
		}
		/*************************** end keyword **************************/

		/****************************
		 * begin abstract_ abstract_alt
		 **************************/
		{
			// 多摘要如：0001616053900030
			abstract_ = abstract_alt = "";
			Element eleAbs = eleArticleContent.select("div.Abstracts").first();
			// System.out.println("div.Abstracts:" + eleAbs.text());
			if (eleAbs != null) {
				int cnt = 0;
				for (Element ele : eleAbs.select("div > div[id^=aep-abstract-sec]")) {
					++cnt;
					if (1 == cnt) {
						abstract_ = ele.text().trim();
					} else if (2 == cnt) {
						abstract_alt = ele.text().trim();
						break;
					}
				}

				if (abstract_.length() < 1) {
					Element eleH2 = eleAbs.select("h2:contains(Abstract)").first();
					if (eleH2 != null) {
						eleH2.remove();
					}
					abstract_ = eleAbs.text().trim();
				}
			}

			// 清理版权申明
			if (abstract_.trim().toLowerCase().endsWith("© 2008 Elsevier B.V. All rights reserved.".toLowerCase())) {
				abstract_ = abstract_.substring(0,
						abstract_.length() - "© 2008 Elsevier B.V. All rights reserved.".length());
			} else if (abstract_.trim().toLowerCase().endsWith("All rights reserved.".toLowerCase())) {
				abstract_ = abstract_.substring(0, abstract_.length() - "All rights reserved.".length());
			}

//			System.out.println("abstract_:" + abstract_);
//			System.out.println("abstract_alt:" + abstract_alt);
		}
		/****************************
		 * end abstract_ abstract_alt
		 **************************/

		/******************* begin doi ************************/
		{
			doi = "";
			Element eleDoi = eleArticleContent.select("div.DoiLink > a.doi").first();
			if (eleDoi != null) {
				String url = eleDoi.attr("href").trim();

				final String regex = "doi\\.org/(.*)$";

				final Pattern pattern = Pattern.compile(regex);
				final Matcher matcher = pattern.matcher(url);

				if (matcher.find()) {
					doi = matcher.group(1);
				}
			}
			// System.out.println("doi:" + doi);
		}
		/******************* end doi ************************/

		/****************************
		 * begin journal_raw_id, issn
		 ******************************/
		{

			journal_raw_id = issn = "";
			Element eleAJournal = eleArticleContent.select("div.Publication > div.publication-brand > a").first();
			if (eleAJournal == null) {
				eleAJournal = eleArticleContent.select("h2#publication-title > a").first();
			}
			if (eleAJournal != null) {
				String url = eleAJournal.attr("href").trim();
				Pattern pattern = Pattern.compile("science/journal/([0-9a-zA-z]{8})$"); // issn的字符不全是数字
				Matcher matcher = pattern.matcher(url);
				if (matcher.find()) {
					journal_raw_id = matcher.group(1).trim();
				}
			}
			issn = journal_raw_id.substring(0, 4) + "-" + journal_raw_id.substring(4);
		}
//		System.out.println("issn:" + issn);
		/**************************** end issn ******************************/

		/**************************** begin issn 年卷期页 ******************************/
		// S0892059189706212: #react-root > div > div > div > div > section > div >
		// div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right >
		// div.Publication > div.publication-vol > h2 > span > a
		// S0140673600025459: #react-root > div > div > div > div > section > div >
		// div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right >
		// div.Publication > div.publication-brand > a
		{// 卷期处无链接：S2314728816300034
//			pub_year = "1900";		// 已从采集直接传入（20190608）
			pub_date = "19000000";
			vol = num = page_info = "";
			is_suppl = "0";

			// #publication > div.publication-brand > div
			// #publication > div.publication-vol.u-text-center > div.text-xs
			// Element eleVolIssue = eleArticleContent.select("div#publication > div >
			// span.size-m").first();
			Element eleVolIssue = eleArticleContent.select("div#publication > div > div.text-xs").first();
			if (eleVolIssue != null) {
				String line = eleVolIssue.text();

//				System.out.println(eleArticleContent.select("div#publication > div").first().html());
//				System.out.println("********************");
//				System.out.println(eleVolIssue.html());
//				System.out.println("line:" + line);
				getPubDate(line); // 解析出版日期等

				if (issn.length() < 1) { // 如果前面没获取到issn
					Element aElement = eleVolIssue.select("a[href*=science/journal/]").first();
					if (aElement != null) {
						String url = aElement.attr("href").trim();
						Pattern pattern = Pattern.compile("science/journal/([0-9a-zA-z]{8})/"); // issn的字符不全是数字
						Matcher matcher = pattern.matcher(url);
						if (matcher.find()) {
							issn = matcher.group(1).trim();
						}
					}
				}

				// 从 json 获取卷期
				if ((jsonObj != null) && (jsonObj.has("article"))
						&& (jsonObj.getAsJsonObject("article").has("vol-iss-suppl-text"))) {
					String volIssLine = jsonObj.getAsJsonObject("article").getAsJsonPrimitive("vol-iss-suppl-text")
							.getAsString().trim();
//					System.out.println("line:" + line);					
					// S0001209216307554, Volume 104, Issue 6, Supplement
					// S0001209216307232, Volume 104, Issue 6
					// S0020751919300347, Volume 49, Issues 3–4
					// S0001457519305469, Volume 127

					vol = volIssLine.replaceAll("^Volumes?\\s+([^,]+),?.*$", "$1");
					if (volIssLine.indexOf("Issue") > -1) {
						num = volIssLine.replaceAll("^.*Issues?\\s(.+)$", "$1");
					}
				}

				for (String item : line.split(",")) {
					item = item.trim();
					if (item.startsWith("Volume")) {
						if (vol.length() < 1) {
							if (item.startsWith("Volumes")) {
								vol = item.substring("Volumes".length()).trim();
							} else {
								vol = item.substring("Volume".length()).trim();
							}
						}
					} else if (item.startsWith("Issue")) {
						if (num.length() < 1) {
							if (item.startsWith("Issues")) {
								num = item.substring("Issues".length()).trim(); // 多期
							} else {
								num = item.substring("Issue".length()).trim(); // 单期
							}
							num = num.replace('–', '-'); // 替换特殊的短横
						}
					} else if (item.startsWith("Page")) {
						if (item.startsWith("Pages")) {
							page_info = item.substring("Pages".length()).trim(); // 多页
						} else {
							page_info = item.substring("Page".length()).trim(); // 单页:S0008418216310754
						}
					}
				}
			}

			String[] vec = StringHelper.parsePageInfo(page_info);
			begin_page = vec[0].trim();
			end_page = vec[1].trim();
			jump_page = vec[2].trim();

			if (num.toLowerCase().indexOf("supplement") > -1) {
				is_suppl = "1";
			}

//			System.out.println("issn:" + issn);
//			System.out.println("pub_year:" + pub_year);
//			System.out.println("pub_date:" + pub_date);
//			System.out.println("vol:" + vol);
//			System.out.println("num:" + num);
//			System.out.println("page_info:" + page_info);
//			System.out.println("begin_page:" + begin_page);
//			System.out.println("end_page:" + end_page);
//			System.out.println("jump_page:" + jump_page);
		}
		/**************************** end issn 年卷期页 ******************************/

		/*********************
		 * begin recv_date, accept_date, revision_date, pub_date_alt
		 ******************/
		{
			recv_date = accept_date = revision_date = pub_date_alt = "";
			if (jsonObj != null) {
				JsonObject datesObj = jsonObj.getAsJsonObject("article").getAsJsonObject("dates");
//				System.out.println("datesObj:" + datesObj.toString());					

				String recvLine = GsonHelper.getJsonValue(datesObj, "Received");
				recv_date = DateTimeHelper.stdDate(recvLine);
//				System.out.println("recvLine:" + recvLine);					

				String acceptLine = GsonHelper.getJsonValue(datesObj, "Accepted");
				accept_date = DateTimeHelper.stdDate(acceptLine);
//				System.out.println("acceptLine:" + acceptLine);					

				String pubLine = GsonHelper.getJsonValue(datesObj, "Publication date");
				pub_date = DateTimeHelper.stdDate(pubLine);
//				System.out.println("pubLine:" + pubLine);	

				String reviLine = GsonHelper.getJsonValue(datesObj, "Revised");
				revision_date = DateTimeHelper.stdDate(reviLine);

				String avaiLine = GsonHelper.getJsonValue(datesObj, "Available online");
				pub_date_alt = DateTimeHelper.stdDate(avaiLine);
//				System.out.println("avaiLine:" + avaiLine);			
			}
//			System.out.println("recv_date:" + recv_date);
//			System.out.println("accept_date:" + accept_date);
//			System.out.println("pub_date:" + pub_date);
//			System.out.println("revision_date:" + revision_date);
//			System.out.println("pub_date_alt:" + pub_date_alt);
		}
		/*********************
		 * end recv_date, accept_date, pub_date_alt
		 ******************/

		/******************* begin journal_name(刊名) ************************/
		{
			// 图片刊名：014067369093348S
			journal_name = "";
			Element eleJournal = eleArticleContent.select("a.publication-title-link").first();
			if (eleJournal != null) {
				journal_name = eleJournal.text().trim();
			}
//			System.out.println("journal_name:" + journal_name);				
		}
		/******************* end journal_name(刊名) ************************/

		// 打印所有字段
		boolean bPrint = false;
		if (bPrint) {
			System.out.println("lngid: " + lngid);
			System.out.println("rawid: " + rawid);
			System.out.println("sub_db_id: " + sub_db_id);
			System.out.println("product: " + product);
			System.out.println("sub_db: " + sub_db);
			System.out.println("provider: " + provider);
			System.out.println("down_date: " + down_date);
			System.out.println("batch: " + batch);
			System.out.println("translator: " + translator);
			System.out.println("translator_intro: " + translator_intro);
			System.out.println("ori_src: " + ori_src);
			System.out.println("doi: " + doi);
			System.out.println("source_type: " + source_type);
			System.out.println("provider_url: " + provider_url);
			System.out.println("title: " + title);
			System.out.println("title_alt: " + title_alt);
			System.out.println("title_sub: " + title_sub);
			System.out.println("title_series: " + title_series);
			System.out.println("keyword: " + keyword);
			System.out.println("keyword_alt: " + keyword_alt);
//			System.out.println("keyword_machine: " + keyword_machine);
//			System.out.println("clc_no_1st: " + clc_no_1st);
//			System.out.println("clc_no: " + clc_no);
//			System.out.println("clc_machine: " + clc_machine);
			System.out.println("subject_word: " + subject_word);
			System.out.println("subject_edu: " + subject_edu);
			System.out.println("subject: " + subject);
			System.out.println("abstract: " + abstract_);
			System.out.println("abstract_alt: " + abstract_alt);
			System.out.println("abstract_type: " + abstract_type);
			System.out.println("abstract_alt_type: " + abstract_alt_type);
			System.out.println("page_info: " + page_info);
			System.out.println("begin_page: " + begin_page);
			System.out.println("end_page: " + end_page);
			System.out.println("jump_page: " + jump_page);
			System.out.println("doc_code: " + doc_code);
			System.out.println("doc_no: " + doc_no);
			System.out.println("raw_type: " + raw_type);
			System.out.println("recv_date: " + recv_date);
			System.out.println("accept_date: " + accept_date);
			System.out.println("revision_date: " + revision_date);
			System.out.println("pub_date: " + pub_date);
			System.out.println("pub_date_alt: " + pub_date_alt);
			System.out.println("pub_place: " + pub_place);
			System.out.println("coden: " + coden);
			System.out.println("page_cnt: " + page_cnt);
			System.out.println("pdf_size: " + pdf_size);
			System.out.println("fulltext_txt: " + fulltext_txt);
			System.out.println("fulltext_addr: " + fulltext_addr);
			System.out.println("fulltext_type: " + fulltext_type);
			System.out.println("column_info: " + column_info);
			System.out.println("fund: " + fund);
			System.out.println("fund_id: " + fund_id);
			System.out.println("fund_alt: " + fund_alt);
			System.out.println("author_id: " + author_id);
			System.out.println("author_1st: " + author_1st);
			System.out.println("author: " + author);
			System.out.println("author_raw: " + author_raw);
			System.out.println("author_alt: " + author_alt);
//			System.out.println("corr_author: " + corr_author);
//			System.out.println("corr_author_id: " + corr_author_id);
			System.out.println("email: " + email);
			System.out.println("subject_major: " + subject_major);
//			System.out.println("research_field: " + research_field);
//			System.out.println("contributor: " + contributor);
//			System.out.println("contributor_id: " + contributor_id);
//			System.out.println("contributor_alt: " + contributor_alt);
//			System.out.println("author_intro: " + author_intro);
//			System.out.println("organ_id: " + organ_id);
			System.out.println("organ_1st: " + organ_1st);
			System.out.println("organ: " + organ);
			System.out.println("organ_alt: " + organ_alt);
//			System.out.println("preferred_organ: " + preferred_organ);
//			System.out.println("host_organ_id: " + host_organ_id);
			System.out.println("organ_area: " + organ_area);
			System.out.println("journal_raw_id: " + journal_raw_id);
			System.out.println("journal_name: " + journal_name);
			System.out.println("journal_name_alt: " + journal_name_alt);
			System.out.println("pub_year: " + pub_year);
			System.out.println("vol: " + vol);
			System.out.println("num: " + num);
			System.out.println("is_suppl: " + is_suppl);
			System.out.println("issn: " + issn);
			System.out.println("eissn: " + eissn);
			System.out.println("cnno: " + cnno);
			System.out.println("isbn: " + isbn);
			System.out.println("publisher: " + publisher);
			System.out.println("cover_path: " + cover_path);
			System.out.println("is_oa: " + is_oa);
			System.out.println("country: " + country);
			System.out.println("language: " + language);
			System.out.println("ref_cnt: " + ref_cnt);
			System.out.println("ref_id: " + ref_id);
			System.out.println("cited_id: " + cited_id);
			System.out.println("cited_cnt: " + cited_cnt);
			System.out.println("down_cnt: " + down_cnt);
//			System.out.println("orc_id: " + orc_id);
//			System.out.println("researcher_id: " + researcher_id);
//			System.out.println("is_topcited: " + is_topcited);
//			System.out.println("is_hotpaper: " + is_hotpaper);
//			System.out.println("pubmed_id: " + pubmed_id);
//			System.out.println("pmc_id: " + pmc_id);
		}

		return true;
	}

	protected abstract void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException;
}
