package simple.jobstream.mapreduce.site.mspjournal;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;
import java.util.regex.PatternSyntaxException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.http.auth.params.AuthPNames;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 100;

//	private static String batch = "";
	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
//		batch = job.getConfiguration().get("batch");
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	// ======================================处理逻辑=======================================
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		static String rawid = "";
		static String down_date = "";
		static String batch = "";
		static String doi = "";
		static String title = "";
		static String keyword = "";
		static String description = "";
		static String begin_page = "";
		static String end_page = "";
		static String recv_date = "";
		static String accept_date = "";
		static String revision_date = "";
		static String pub_date = "";
		static String author = "";
		static String organ = "";
		static String journal_raw_id = "";
		static String journal_name = "";
		static String pub_year = "";
		static String vol = "";
		static String num = "";
		static String publisher = "";
		static String provider_url = "";
		static String eissn = "";
		static String pissn = "";
		static String page_info = "";
		static String lngid = "";

		static String sub_db_id = "00033";
		static String product = "MSP";
		static String provider = "MSP";
		static String source_type = "3";
		static String country = "US";
		static String language = "EN";
		static String sub_db = "QK";

		static String candidate_year = "";
		static String fulltext_type = "pdf";
		static String is_oa = "0";
		static String author_1st = "";
		static String organ_1st = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public static boolean parseHtml(Document doc) {

			// title 文章标题
//			String title = "";
			Element titleElement = doc.select("meta[name=citation_title]").first();
			if (titleElement != null) {
				title = titleElement.attr("content").trim();
			}
//			System.out.println(title);

			// DOI
			Element doiElement = doc.select("meta[name=citation_doi]").first();
			if (doiElement != null) {
				doi = doiElement.attr("content").trim();
			}
//			System.out.println(doi);

			// 作者机构组成字典
			LinkedHashMap<String, String> authorMap = new LinkedHashMap<String, String>();

			// 作者机构 优先从页面取，没有则从头标签取
			Element authorAffElement = doc.select("table.author-contact").first();
			if (authorAffElement != null) { // 作者机构在页面不为空
				ArrayList<String> authors = new ArrayList<>(); 
				Elements trElements = authorAffElement.select("tr");
//				System.out.println(trElements.size());
				if(trElements.size()>0) {
					String tempAuth = "";
					for(int i=trElements.size()-1;i>=0;i--) {  //倒序累加机构和作者,每当取到作者时,累加的字符串传入到一个列表,同时置空
						Elements tdElements = trElements.get(i).select("td");
						String eleTemp = "";
						for(Element ele:tdElements) {		// 取tr的所有子元素td,拿到td的文本
							String tdText = "";
							
							for(TextNode eleChild:ele.textNodes()) {	// 取出单个td标签下的所有文本,且用逗号链接
								tdText += eleChild.text()+",";
							}
							tdText = tdText.replaceAll(",+$", "");		// 去除末尾的逗号
//							System.out.println(tdText);
							
							if(ele.className().equals("author-address")) {  //单个tr下面的所有文本,多个时用分号链接(主要是多个机构)
								if(tdText.startsWith("http://www.math")) {  // 机构地址带网址标签时不要
									tdText = "";
								}
								eleTemp += tdText + ";";
							}else if (ele.className().equals("author-name")) {  // 作者用@标识
								eleTemp += "@" + ele.text().trim();
							}else {
								eleTemp = "";
							}
						}
						eleTemp = eleTemp.replaceAll(";+$", "");
						
						// 从后往前累加机构和作者,到作者时,累加的字符串传入到一个列表,同时置空
						Element tdElement = trElements.get(i).select("td").first();
						tempAuth += eleTemp;
//						System.out.println("tempAuth:"+tempAuth);
						if(!tempAuth.equals("") && tempAuth.contains("@")) {   //不为空且包含作者时才进入列表
//							System.out.println("tempAuth:"+tempAuth);
							authors.add(tempAuth);
						}
						
						if(tdElement.hasClass("author-name")) {   // 遍历到作者标识后,重置为空
							tempAuth = "";
						}
					}
				}
//				System.out.println(authors);
				
				// 遍历作者机构组成的列表,传入到一个字典里, 注意顺序为列表倒序
				for(int i=authors.size() -1;i>=0;i--) {
					String[] auString = authors.get(i).split("@");
//					System.out.println("author:"+auString[1]);
//					System.out.println("address:"+auString[0]);
					authorMap.put(auString[1], auString[0]);
				}
				
			} else { // 页面没有作者机构；从头标签里面取
				Elements authorElments = doc.select("meta[name=citation_author]");
				Elements organElements = doc.select("meta[name=citation_author_institution]");
//				System.out.println(organElements.size());
				if (authorElments.size() > 0 && organElements.size() > 0) { // 头标签既有作者标签又有作者机构
					for (int i = 0; i < authorElments.size(); i++) {
						// 单个作者，为了与页面相匹配，切分逗号并倒序组合
						String one_auth = authorElments.get(i).attr("content").trim();
						String[] auth = one_auth.split(",");
						for (int j = auth.length - 1; j >= 0; j--) {
							one_auth += auth[j];
						}

						if (organElements.get(i) != null) { // 作者机构不为空
							authorMap.put(one_auth, organElements.get(i).attr("content").trim());
						} else {
							authorMap.put(one_auth, "");
						}
					}
				} else if (authorElments.size() > 0 && organElements.size() == 0) { // 头标签只有作者没有机构
					for (int i = 0; i < authorElments.size(); i++) {
						// 单个作者，为了与页面相匹配，切分逗号并倒序组合
						String one_auth = authorElments.get(i).attr("content").trim();
						String[] auth = one_auth.split(",");
						for (int j = auth.length - 1; j >= 0; j--) {
							one_auth += auth[j];
						}
						authorMap.put(one_auth, "");
					}
				}
			}

			String[] result = AuthorOrgan.numberByMap(authorMap);
			author = result[0];
			organ = result[1];
			author_1st = author.split(";")[0].replaceAll("\\[.*?\\]", "");
			organ_1st = organ.split(";")[0].replaceAll("\\[.*?\\]", "");

//			System.out.println("author: " + author + " and organ: "+ organ + ";");

			// journal_name
			Element jtElement = doc.select("meta[name=citation_journal_title]").first();
			if (jtElement != null) {
				journal_name = jtElement.attr("content").trim();
			}
//			System.out.println("journal_name:"+journal_name);

			// 卷
			Element volumeElement = doc.select("meta[name=citation_volume]").first();
			if (volumeElement != null) {
				vol = volumeElement.attr("content").trim();
			}
//			System.out.println("vol:"+vol);

			// 期
			Element issueElement = doc.select("meta[name=citation_issue]").first();
			if (issueElement != null) {
				num = issueElement.attr("content").trim();
			}
//			System.out.println(num);

			// 出版日期
			Element pubDateElement = doc.select("meta[name=citation_publication_date]").first();
			if (pubDateElement != null) {
				pub_date = pubDateElement.attr("content").trim().toLowerCase();
				if (!pub_date.contains("nan")) {
					// 出版年
					pub_year = pub_date.split("/")[0].split("-")[0];
					// 出版日期
					pub_date = pub_date.replace("/", "").replace("-", "");
				} else {
					pub_year = "";
					pub_date = "";
				}
			}

//			System.out.println(pub_year);
//			System.out.println(pub_date);

			// description 摘要
			Elements absElements = doc.select("table.article");
			for (Element ele : absElements) {
				String eleText = ele.text().trim();
				if (eleText.toLowerCase().startsWith("abstract")) {
					description = eleText;
					description = Jsoup.parse(description).text().replaceAll("^+Abstract", "").trim();
				}
			}
//			System.out.println(description);

			// 关键词 以分号;分隔
//			String keyword = "";
			Elements kwElements = doc.select("table.article");
			for (Element ele : kwElements) {
				String eleText = ele.text().trim();
				if (eleText.toLowerCase().startsWith("keywords")) {
					keyword = eleText;
					keyword = Jsoup.parse(keyword).text().replaceAll("^+Keywords", "").trim().replaceAll(",", ";");
					keyword = StringHelper.cleanSemicolon(keyword); //处理分号的空白内容
				}
			}
			
//			System.out.println(keyword);

			// begin page
			Element beginElement = doc.select("meta[name=citation_firstpage]").first();
			if (beginElement != null) {
				begin_page = beginElement.attr("content").trim();
			}
//			System.out.println(begin_page);

			// end page
			Element endElement = doc.select("meta[name=citation_lastpage]").first();
			if (endElement != null) {
				end_page = endElement.attr("content").trim();
			}
//			System.out.println(end_page);

			// page
			Element pageElement = doc.select("div.page-numbers-heading, div.page-numbers").first();
			if (pageElement != null) {
				String[] splitArray = pageElement.text().trim().split(" ");
				page_info = splitArray[splitArray.length - 1].trim();
				// 候补年
				candidate_year = pageElement.text().split("\\(")[1].split("\\)")[0].trim();
			}
//			System.out.println(page_info);

			// 收稿日期 接受日期 修订日期, 出版日期
//			recv_date = "";
//			accept_date = "";
//			revision_date = "";
			Elements keyElements = doc.select("div.keywords");
			for (Element ele : keyElements) {
				String wordString = ele.text().trim().toLowerCase();
				if (wordString.startsWith("received")) {
					recv_date = wordString.split(":")[1].trim();
					recv_date = DateTimeHelper.stdDate(recv_date);
				} else if (wordString.startsWith("revised")) {
					revision_date = wordString.split(":")[1].trim();
					revision_date = DateTimeHelper.stdDate(revision_date);
				} else if (wordString.startsWith("accepted")) {
					accept_date = wordString.split(":")[1].trim();
					accept_date = DateTimeHelper.stdDate(accept_date);
				} else if (wordString.startsWith("published") && pub_date.equals("")) { // 有publish且head无
					pub_date = wordString.split(":")[1].trim();
					pub_date = DateTimeHelper.stdDate(pub_date);
				}
			}
//			System.out.println(recv_date);
//			System.out.println(revision_date);
//			System.out.println(accept_date);
//			System.out.println("pub_date:"+pub_date);

			// publisher
			Element pubElement = doc.select("meta[name=citation_publisher]").first();
			if (pubElement != null) {
				publisher = pubElement.attr("content").trim();
			}
//			System.out.println(publisher);

			// eissn、pissn
			// 第一种页面格式
			Elements aboutElements = doc.select("td.about-area");
			for (Element ele : aboutElements) {
				String aboutText = ele.text().trim();
				if (aboutText.contains("ISSN")) {
					if (aboutText.contains("electronic") || aboutText.contains("e-only")) {
						eissn = aboutText.split(":")[1].trim().replace("electronic", "").replace("e-only", "")
								.replace("(", "").replace(")", "");
					} else {
						pissn = aboutText.split(":")[1].trim().replace("print", "").replace("(", "").replace(")", "");
					}
				}
			}
			// 第二种页面格式
			Elements issnElements = doc.select("td.issn");
			for (Element ele : issnElements) {
				String issnText = ele.text().trim().toLowerCase();
				if (issnText.contains("issn")) {
					if (issnText.contains("electronic") || issnText.contains("e-only")) {
						eissn = issnText.split(":")[1].trim().replace("electronic", "").replace("e-only", "")
								.replace("(", "").replace(")", "");
					} else {
						pissn = issnText.split(":")[1].trim().replace("print", "").replace("(", "").replace(")", "");
					}
				}
			}

			// 处理出版日期及出版年,年不足8位补齐
			if (pub_date.equals("") && candidate_year.equals("")) { // head page-numbers publish都没有
				pub_date = "19000000";
				pub_year = "1900";
			} else if (pub_date.equals("") && !candidate_year.equals("")) { // head publish没有 有page-numbers
				pub_date = candidate_year + "0000";
				pub_year = candidate_year;
			} else if (pub_date.length() < 8 && pub_date.length() > 0) { // head 和 publish任意有一个但是长度不足8位
				int miss_num = 8 - pub_date.length();
				for (int i = 0; i < miss_num; i++) {
					pub_date += "0";
				}
				pub_year = pub_date.substring(0, 4);
			}

			// is_oa
			Element oaElement = doc.select("img.oa").first();
			if (oaElement != null) {
				is_oa = "1";
			}

//			System.out.println(pub_date);
//			System.out.println(pub_year);

//			System.out.println(eissn);
//			System.out.println(pissn);
//			System.out.println("---------------------**********--------------------");

			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				rawid = "";
				down_date = "";
				doi = "";
				title = "";
				keyword = "";
				description = "";
				begin_page = "";
				end_page = "";
				recv_date = "";
				accept_date = "";
				revision_date = "";
				pub_date = "";
				author = "";
				organ = "";
				journal_raw_id = "";
				journal_name = "";
				pub_year = "";
				vol = "";
				num = "";
				publisher = "";
				provider_url = "";
				eissn = "";
				pissn = "";
				page_info = "";
				lngid = "";

				candidate_year = "";
				is_oa = "0";
				author_1st = "";
				organ_1st = "";
			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			down_date = mapJson.get("down_date").trim().replace("-", "");

			// rawid 文章唯一标识
			rawid = mapJson.get("rwaid").trim().replaceAll("^/", "");

			// lngID
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			// provider_url
			provider_url = "https://msp.org/" + rawid + ".xhtml";

			// journal_raw_id
			journal_raw_id = rawid.split("/")[0];

			String html = mapJson.get("html").trim();
			Document doc = Jsoup.parse(html);

			// 解析html
			parseHtml(doc);

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("rawid", rawid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("doi", doi);
			xObj.data.put("title", title);
			xObj.data.put("keyword", keyword);
			xObj.data.put("description", description);
			xObj.data.put("begin_page", begin_page);
			xObj.data.put("end_page", end_page);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("revision_date", revision_date);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("author", author);
			xObj.data.put("organ", organ);
			xObj.data.put("journal_raw_id", journal_raw_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("publisher", publisher);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("eissn", eissn);
			xObj.data.put("pissn", pissn);
			xObj.data.put("page_info", page_info);
			xObj.data.put("lngid", lngid);
			xObj.data.put("sub_db_id", sub_db_id);
			xObj.data.put("product", product);
			xObj.data.put("provider", provider);
			xObj.data.put("source_type", source_type);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("sub_db", sub_db);

			xObj.data.put("candidate_year", candidate_year);
			xObj.data.put("fulltext_type", fulltext_type);
			xObj.data.put("is_oa", is_oa);
			xObj.data.put("author_1st", author_1st); 	//第一作者
			xObj.data.put("organ_1st", organ_1st);		//第一机构

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}

	}
}