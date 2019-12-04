package simple.jobstream.mapreduce.site.plosjournal;

import java.io.IOException;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import org.mockito.internal.matchers.And;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo { 
	public static Logger logger = Logger.getLogger(Json2XXXXObject.class);
	static boolean testRun = false;
	static int testReduceNum = 5;
	static int reduceNum = 100;

//	batch = "";
	static String inputHdfsPath = "";
	static String outputHdfsPath = "";

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
		
		static String lngid = "";
		static String rawid = "";
		//static String sub_db_id = "";//自定义子库代
		//static String product = "";
		//static String sub_db = "";
		//static String provider = "";
		static String down_date = "";
		static String batch = "";
		static String doi = "";
		//static String source_type = "";
		static String provider_url = "";
		static String title = "";
		static String title_alt = "";
		static String title_sub = "";//副标题
		static String title_series = "";//所属丛书名称。与原书保持一致。
		static String keyword = "";
		static String keyword_alt = "";
		static String keyword_machine = "";//机标关键词
		static String clc_no_1st = "";//第一中图分类号
		static String clc_no = "";//中图分类号，以英文分号作分隔符。
		static String clc_machine = "";
		static String subject_word = "";//主题词。（多值）扩展主题词，分号分隔，机器分析生成。
		static String subject_edu = "";//教育部学科分类代码。
		static String subject = "";//文献学科分类名称。（多值）与采集原貌保持一致。英文分号分隔
		static String abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
		static String abstract_alt = "";
		static String abstract_type = "";//文摘类型。著者文摘、编者按、第一段
		static String abstract_alt_type = "";//交替文摘类型。著者文摘、编者按、第一段。
		static String page_info = "";//网页上展示的整体页码信息
		static String begin_page = "";
		static String end_page = "";//跳转页。与原文保持一致。（多值）以英文分号作分隔符。12;15
		static String jump_page = "";//	跳转页。与原文保持一致。（多值）以英文分号作分隔符。
		static String doc_code = "";//文献标识码，一般印刷版才有。
		static String doc_no = "";//文章编号，一般印刷版才有。
		static String raw_type = "";//文章类型：Article、Review、Letter、Editorial等。与原文保持一致。
		static String recv_date = "";//收稿（提交）日期。
		static String accept_date = "";//	接受日期
		static String revision_date = "";//	修订日期
		static String pub_date = "";//出版日期。优先填纸本出版日期。
		static String pub_date_alt = "";//出版日期。优先填在线出版日期。
		static String pub_place = "";//出版地。与原文保持一致。
		static String page_cnt = "";//文章页数。PDF全文的实际页数一致
		static String pdf_size = "";//PDF文件大小。全文容量，精确到字节。全文容量大小字节数。
		static String fulltext_txt = "";
		static String fulltext_addr = "";
		static String fulltext_type = "";
		static String column_info = "";//栏目信息。与原文保持一致。
		static String fund = "";//基金资助。与原文保持一致，未切分。
		static String fund_id = "";//基金ID
		static String fund_alt = "";
		static String author_id = "";//源网站作者ID。	zz233234@李磊;zz66536@韩梅梅
		static String author_1st = "";//	第一作者（责任者）。可多值，分号分隔。	杨小纯[1];田菲[2];于建春[2]
		static String author = "";
		static String author_raw = "";// 英文作者及机构(在一起的原始信息)
		static String author_alt = "";
		static String corr_author = "";//通讯作者。与原文保持一致。与原文通信作者一致，分号分隔。
		static String corr_author_id = "";
		static String email = "";
		static String subject_dsa = "";//作者学科专业。与原文保持一致。
		static String research_field = "";//学科领域、研究方向。在无明确领域时，可填入教育部学科分类汉字文本。
		static String contributor = "";//导师姓名。与原文保持一致。分号分隔，作者ID@导师姓名。
		static String contributor_id = "";
		static String contributor_alt = "";
		static String author_intro = "";//作者简介。独立切分入作者库。
		static String organ_id = "";//机构ID。
		static String organ_1st = "";//第一作者单位。（多值）与原文保持一致，分号分隔。	天津中医药大学,天津300193
		static String organ = "";//机构及对照关系。(多值)与原文保持一致，首填项。以英文分号作分隔符。	[1]天津中医药大学,天津300193;[2]天津中医药大学第一附属医院,天津300192
		static String organ_alt = "";
		static String preferred_organ = "";//增强组织信息的名称，主要针对 WOS。
		static String host_organ_id = "";//办方机构ID。以机构库中ID为准。来源机构库。	jg764355
		static String organ_area = "";//机构所属国家地区。与原文保持一致。来源机构库。	中国,天津市,天津市；
		static String journal_raw_id = "";//	期刊刊名ID。	ZGFX
		static String journal_name = "";//刊名（第一语言）
		static String journal_name_alt = "";
		static String pub_year = "";
		static String vol = "";//卷。以期刊版权页信息为准。来源期刊信息库。
		static String num = "";//	期。以期刊版权页信息为准，来源期刊信息库。	
		static String is_suppl = "";//是否为增刊。以期刊版权页信息为准。来源期刊信息库。
		static String issn = "";//International Standard Serial Number，国际标准连续出版物编号。
		static String eissn = "";//电子期刊的 issn。
		static String cnno = "";//	国内统一刊号（国内统一连续出版物号）	CN50-1074/TP
		static String publisher = "";//出版社（单位）。	天津大学出版社
		static String cover_path = "";//封面本地路径
		static String is_oa = "";//是否公开
		//static String country = "";
		//static String language = "";
		static String ref_cnt = "";//参考文献数量。由于采集时可能有空白的引文条目，所以这个值和ref_id的数量可能不一致。
		static String ref_id = "";//参考文献ID，原始网页未反证成功时不要@及后面内容。	编号后ID@原始ID
		static String cited_id = "";//引证文献ID，无原始ID时不要@及以后内容	编号后ID@原始ID
		static String cited_cnt = "";//被引次数（次数@采集日期）	333@20161231;444@20171231;555@20181231
		static String down_cnt = "";//下载次数（次数@采集日期）	333@20161231 
		static String orc_id = "";//开放研究者与贡献者身份识别码	0000-0002-6795-4760
		static String researcher_id = "";//学者的Researcher识别码	T-9631-2018 
		static String is_topcited = "";//	本字段有值即为高被引。cited_cnt@down_date	333@20161231
		static String is_hotpaper = "";//本字段有值即为热点论文，值为采集日期。down_date

		static String sub_db_id = "";
		static String sub_db = "";
		static String product = "";
		static String provider = "";
		static String source_type = "";
		static String country = ""; 
		static String language = ""; 
		
//		static String sub_db_id = "00044";
//		static String sub_db = "QK";
//		static String product = "PLOS";
//		static String provider = "PLOS";
//		static String source_type = "3";
//		static String country = "US"; 
//		static String language = "EN"; 
		 
//		智图provider: plosjournal
		
		//static String candidate_year = "";
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public static boolean parseHtml(Document doc) {  

			// DOI
			Element doiElement = doc.select("meta[name=citation_doi]").first();
			if (doiElement != null) {
				doi = doiElement.attr("content").trim();
			} 
			// title 文章标题 
			Element titleElement = doc.select("meta[name=citation_title]").first();
			if (titleElement != null) {
				title = titleElement.attr("content").trim();
			}
			Element keywordElement = doc.select("meta[name=keywords]").first();
			if (keywordElement != null) {
				keyword = keywordElement.attr("content").trim().replace(",", ";");
			} 
			Element abstractElement = doc.select("meta[name=citation_abstract]").first();
			if (abstractElement != null) {
				abstract_ = abstractElement.attr("content").trim();
			} 
			Element raw_typeElement = doc.select("meta[property=og:type]").first();
			if (raw_typeElement != null) {
				raw_type = raw_typeElement.attr("content").trim();
				raw_type = (new StringBuilder()).append(Character.toUpperCase(raw_type.charAt(0))).append(raw_type.substring(1)).toString();
			} 
			Element column_infoElement = doc.select("meta[name=citation_article_type]").first();
			if (column_infoElement != null) {
				column_info = column_infoElement.attr("content").trim();
			} 
			Element urlElement = doc.select("meta[property=og:url]").first();
			if (urlElement != null) { 
				String[] urls = urlElement.attr("content").trim().split("/"); 
				journal_raw_id = urls[3];
			}   
			Element journal_nameElement = doc.select("meta[name=citation_journal_title]").first();
			if (journal_nameElement != null) {
				journal_name = journal_nameElement.attr("content").trim(); 
			}   
			Elements articleInfoElement = doc.select("div[class=articleinfo] > p");
			for(Element element : articleInfoElement) {  
				String strong = element.select("strong").text(); 
				if(strong.indexOf("Funding") != -1 && element.ownText().indexOf("no specific funding") == -1) {   
					fund = element.ownText();
				}else {
					if(strong.indexOf("Received") != -1 || strong.indexOf("Accepted") != -1 || strong.indexOf("Published") != -1) { 
						String[] recAllInfo = element.text().split(";");//Received: June 27, 2018; Accepted: January 11, 2019; Published: February 15, 2019
						if(recAllInfo.length > 0) { 
							String[] recInfos = element.ownText().split(";"); 
							for(int i=0 ; i < recAllInfo.length ; i++) { 
								if(recAllInfo[i].indexOf("Received") != -1) {
									recv_date = DateTimeHelper.stdDate(recInfos[i]);					
								} else if(recAllInfo[i].indexOf("Accepted") != -1) {
									accept_date = DateTimeHelper.stdDate(recInfos[i]);
								}else if(recAllInfo[i].indexOf("Published") != -1) { 
									pub_date = DateTimeHelper.stdDate(recInfos[i]);
									pub_year = pub_date.substring(0,4); 
								}
							} 
						} 
					} 
				} 
			}  
			if(pub_date.equals("")) {
				pub_date = "19000000";
				pub_year = "1900";  
			}
			Element volElement = doc.select("meta[name=citation_volume]").first();
			if (volElement != null) {
				vol = volElement.attr("content").trim(); 
			}   
			Element numElement = doc.select("meta[name=citation_issue]").first();
			if (numElement != null) {
				num = numElement.attr("content").trim();
			}
			Element issnElement = doc.select("meta[name=citation_issn]").first();
			if (issnElement != null) {
				issn = issnElement.attr("content").trim();
			}
			Element publisherElement = doc.select("meta[name=citation_publisher]").first();
			if (publisherElement != null) {
				publisher = publisherElement.attr("content").trim();
			}
			Element is_oaElement = doc.select("p[id=licenseShort]").first(); 
			if (is_oaElement != null) {
				String isOAtemp = is_oaElement.text().trim(); 
				if(isOAtemp.equals("Open Access")){
					is_oa = "1";
				} 
			}  
			// 作者机构组成字典
			LinkedHashMap<String, String> authorMap = new LinkedHashMap<String, String>();
			// authors
			Elements authorElments = doc.select("meta[name^=citation_author]"); 
			//Collections.reverse(authorElments);
			String authorStr = null;
			String organStr = "";
			if(authorElments != null && authorElments.size() > 0) {
				int i = 1;
				int len = authorElments.size();
				for(Element element : authorElments) {  
					if(element.attr("name").trim().equals("citation_author")) { 
						if(authorStr != null) {  
							authorMap.put(authorStr,organStr.startsWith(";")?organStr.substring(1,organStr.length()):organStr); 
							organStr = "";
						} 
						authorStr = element.attr("content").trim(); 
						if(i == len) {
							authorMap.put(authorStr,organStr);
						} 
					}else {
						organStr = organStr + ";" + element.attr("content").trim();
						if(i == len) {
							authorMap.put(authorStr,organStr.substring(1,organStr.length()));
						} 
					}  
					i ++;
				} 
				String[] result = AuthorOrgan.numberByMap(authorMap);
				Map.Entry<String, String> fisrt = authorMap.entrySet().iterator().next();
				author_1st = fisrt.getKey();
				author = result[0];
				organ_1st = fisrt.getValue();
				organ = result[1]; 
			} 
			Elements authorInfoElments = doc.select("ul.author-list > li[data-js-tooltip=tooltip_trigger]");  
			if(authorInfoElments != null){
				StringBuilder sb = new StringBuilder();
				StringBuilder sbOcc = new StringBuilder();
				for(Element element : authorInfoElments){  
					String authorName = element.select("a[class=author-name]").text(); 
					if(authorName.charAt(authorName.length()-1) == ',') {
						authorName = authorName.substring(0, authorName.length()-1).trim();
					}
					sb.append(";").append(authorName).append(".");  
					Elements inforPlabel = element.select("div[class=author-info] > p");
					for(Element ele : inforPlabel) {
						if(ele.select("span").hasClass("contribute")) {  
							sb.append(ele.ownText()).append("."); 
						}else if(ele.hasClass("roles")) { 
							sb.append("Roles:" + ele.ownText()).append("."); 
						}else if(ele.select("span").hasClass("email")) { 
							Elements mailElements = ele.select("a");
							sb.append("E-mail:");   
							for(Element mail:mailElements) {
								sb.append(mail.ownText()).append(",");  
							} 
							sb.deleteCharAt(sb.length()-1).append(".");
						} 
					} 
					Elements orcidLabel = element.select("div[class=author-info] > div > p");
					if(orcidLabel != null && orcidLabel.hasClass("orcid")) {
						String orcid = orcidLabel.select("a").attr("href").split("/")[3]; 
						sbOcc.append(orcid).append("@").append(authorName).append(";"); 
					}
				}
				if(sb.length()>0) {
					author_intro = sb.deleteCharAt(0).deleteCharAt(sb.length()-1).toString();
				} 
				if(sbOcc.length()>0) {
					orc_id = sbOcc.deleteCharAt(sbOcc.length()-1).toString();
				} 	
			} 
			Elements refElememt = doc.select("ol.references > li"); 
			if(refElememt != null && refElememt.size() > 0) {
				String ref = refElememt.get(refElememt.size()-1).attr("id").trim();
				ref_cnt = ref.substring(3, ref.length());
			} 
			
			Elements dloadPdfElememt = doc.select("a[id=downloadPdf]"); 
			if(dloadPdfElememt != null && dloadPdfElememt.attr("href") != null) {
				fulltext_type = "pdf";
			} 
			Elements dloadXmlElememt = doc.select("a[id=downloadXml]"); 
			if(dloadXmlElememt != null && dloadXmlElememt.attr("href") != null) {
				if(!fulltext_type.equals("")) {
					fulltext_type = fulltext_type + ";xml";
				}else {
					fulltext_type ="xml";
				} 
			} 
			
			XXXXObject xObj = new XXXXObject(); 
			xObj.data.put("rawid", doi);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("doi", doi); 
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("title", title);
			xObj.data.put("keyword", keyword);
			xObj.data.put("abstract", abstract_); 
			xObj.data.put("raw_type", raw_type);   
			xObj.data.put("rawid", doi);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("accept_date", accept_date); 
			xObj.data.put("fulltext_type", fulltext_type); 
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("column_info", column_info); 
			xObj.data.put("fund", fund);
			xObj.data.put("author_1st", author_1st);
			xObj.data.put("author", author);
			xObj.data.put("author_intro", author_intro);
			xObj.data.put("organ_1st", organ_1st);
			xObj.data.put("organ", organ);
			xObj.data.put("journal_raw_id", journal_raw_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("issn", issn); 
			xObj.data.put("publisher", publisher); 
			xObj.data.put("is_oa", is_oa);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("down_cnt", down_cnt);
			xObj.data.put("orc_id", orc_id);
			
			
			for (Map.Entry<String, String> entry : xObj.data.entrySet()) { 
				System.out.println(entry.getKey() + ":" + entry.getValue()); 
			}
//			Elements countElememt = doc.select("ul[id=almSignposts] > li"); 
//			if(countElememt != null) { 
//				System.out.println(countElememt.attr("id"));
//				for(Element ele : countElememt) { 
//					if(ele.attr("id").equals("almSaves")) {
//						down_cnt = ele.ownText().trim();
//					}else if(ele.attr("id").equals("almCitations")) {
//						cited_cnt = ele.ownText().trim();
//					} 
//				} 
//			}    
			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				lngid = "";
				rawid = "";
				down_date = ""; 
				doi = "";
				provider_url = "";
				title = "";
				title_alt = "";
				title_sub = "";//副标题
				title_series = "";//所属丛书名称。与原书保持一致。
				keyword = "";
				keyword_alt = "";
				keyword_machine = "";//机标关键词
				clc_no_1st = "";//第一中图分类号
				clc_no = "";//中图分类号，以英文分号作分隔符。
				clc_machine = "";
				subject_word = "";//主题词。（多值）扩展主题词，分号分隔，机器分析生成。
				subject_edu = "";//教育部学科分类代码。
				subject = "";//文献学科分类名称。（多值）与采集原貌保持一致。英文分号分隔
				abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
				abstract_alt = "";
				abstract_type = "";//文摘类型。著者文摘、编者按、第一段
				abstract_alt_type = "";//交替文摘类型。著者文摘、编者按、第一段。
				page_info = "";//网页上展示的整体页码信息
				begin_page = "";
				end_page = "";//跳转页。与原文保持一致。（多值）以英文分号作分隔符。12;15
				jump_page = "";//	跳转页。与原文保持一致。（多值）以英文分号作分隔符。
				doc_code = "";//文献标识码，一般印刷版才有。
				doc_no = "";//文章编号，一般印刷版才有。
				raw_type = "";//文章类型：Article、Review、Letter、Editorial等。与原文保持一致。
				recv_date = "";//收稿（提交）日期。
				accept_date = "";//	接受日期
				revision_date = "";//	修订日期
				pub_date = "";//出版日期。优先填纸本出版日期。
				pub_date_alt = "";//出版日期。优先填在线出版日期。
				pub_place = "";//出版地。与原文保持一致。
				page_cnt = "";//文章页数。PDF全文的实际页数一致
				pdf_size = "";//PDF文件大小。全文容量，精确到字节。全文容量大小字节数。
				fulltext_txt = "";
				fulltext_addr = "";
				fulltext_type = "";
				column_info = "";//栏目信息。与原文保持一致。
				fund = "";//基金资助。与原文保持一致，未切分。
				fund_id = "";//基金ID
				fund_alt = "";
				author_id = "";//源网站作者ID。	zz233234@李磊;zz66536@韩梅梅
				author_1st = "";//	第一作者（责任者）。可多值，分号分隔。	杨小纯[1];田菲[2];于建春[2]
				author = "";
				author_raw = "";// 英文作者及机构(在一起的原始信息)
				author_alt = "";
				corr_author = "";//通讯作者。与原文保持一致。与原文通信作者一致，分号分隔。
				corr_author_id = "";
				email = "";
				subject_dsa = "";//作者学科专业。与原文保持一致。
				research_field = "";//学科领域、研究方向。在无明确领域时，可填入教育部学科分类汉字文本。
				contributor = "";//导师姓名。与原文保持一致。分号分隔，作者ID@导师姓名。
				contributor_id = "";
				contributor_alt = "";
				author_intro = "";//作者简介。独立切分入作者库。
				organ_id = "";//机构ID。
				organ_1st = "";//第一作者单位。（多值）与原文保持一致，分号分隔。	天津中医药大学,天津300193
				organ = "";//机构及对照关系。(多值)与原文保持一致，首填项。以英文分号作分隔符。	[1]天津中医药大学,天津300193;[2]天津中医药大学第一附属医院,天津300192
				organ_alt = "";
				preferred_organ = "";//增强组织信息的名称，主要针对 WOS。
				host_organ_id = "";//办方机构ID。以机构库中ID为准。来源机构库。	jg764355
				organ_area = "";//机构所属国家地区。与原文保持一致。来源机构库。	中国,天津市,天津市；
				journal_raw_id = "";//	期刊刊名ID。	ZGFX
				journal_name = "";//刊名（第一语言）
				journal_name_alt = "";
				pub_year = "";
				vol = "";//卷。以期刊版权页信息为准。来源期刊信息库。
				num = "";//	期。以期刊版权页信息为准，来源期刊信息库。	
				is_suppl = "";//是否为增刊。以期刊版权页信息为准。来源期刊信息库。
				issn = "";//International Standard Serial Number，国际标准连续出版物编号。
				eissn = "";//电子期刊的 issn。
				cnno = "";//	国内统一刊号（国内统一连续出版物号）	CN50-1074/TP
				publisher = "";//出版社（单位）。	天津大学出版社
				cover_path = "";//封面本地路径
				is_oa = "";//是否公开
				ref_cnt = "";//参考文献数量。由于采集时可能有空白的引文条目，所以这个值和ref_id的数量可能不一致。
				ref_id = "";//参考文献ID，原始网页未反证成功时不要@及后面内容。	编号后ID@原始ID
				cited_id = "";//引证文献ID，无原始ID时不要@及以后内容	编号后ID@原始ID
				cited_cnt = "";//被引次数（次数@采集日期）	333@20161231;444@20171231;555@20181231
				down_cnt = "";//下载次数（次数@采集日期）	333@20161231 
				orc_id = "";//开放研究者与贡献者身份识别码	0000-0002-6795-4760
				researcher_id = "";//学者的Researcher识别码	T-9631-2018 
				is_topcited = "";//	本字段有值即为高被引。cited_cnt@down_date	333@20161231
				is_hotpaper = "";//本字段有值即为热点论文，值为采集日期。down_date
				 
			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			down_date = mapJson.get("down_date").trim(); 
			
			//rawid = mapJson.get("url").trim();
			
			// lngID
//			lngid = VipIdEncode.getLngid(sub_db_id, rawid ,false);
			
			//provider_url
			provider_url = "https://journals.plos.org/" + mapJson.get("url").trim();
			
			//journal_raw_id
//			journal_raw_id = rawid.split("/")[1];
			
			String html = mapJson.get("html").trim();
			Document doc = Jsoup.parse(html);
			
			// 解析html
			parseHtml(doc);
			
			rawid = doi;

			XXXXObject xObj = new XXXXObject();   
			xObj.data.put("rawid", rawid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("doi", doi); 
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("title", title);
			xObj.data.put("keyword", keyword);
			xObj.data.put("abstract", abstract_); 
			xObj.data.put("raw_type", raw_type);   
			xObj.data.put("rawid", doi);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("fulltext_type", fulltext_type); 
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("column_info", column_info); 
			xObj.data.put("fund", fund);
			xObj.data.put("author_1st", author_1st);
			xObj.data.put("author", author);
			xObj.data.put("author_intro", author_intro);
			xObj.data.put("organ_1st", organ_1st);
			xObj.data.put("organ", organ);
			xObj.data.put("journal_raw_id", journal_raw_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("issn", issn); 
			xObj.data.put("publisher", publisher); 
			xObj.data.put("is_oa", is_oa);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("orc_id", orc_id);
			//xObj.data.put("cited_cnt", cited_cnt);
			//xObj.data.put("down_cnt", down_cnt);
			
//			xObj.data.put("sub_db_id", sub_db_id); 
//			xObj.data.put("sub_db", sub_db);
//			xObj.data.put("product", product); 
//			xObj.data.put("provider", provider);
//			xObj.data.put("source_type", source_type);
//			xObj.data.put("country", country);
//			xObj.data.put("language", language); 
 
			context.getCounter("map", "count").increment(1); 
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			
//			String logHDFSFile = "/user/qinym/log/" + DateTimeHelper.getNowDate() + "title.txt";
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "author:" + author);
		}

	}
}