package simple.jobstream.mapreduce.site.scopusjournal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Array;
import java.lang.reflect.Type;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.commons.validator.routines.ISSNValidator;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.google.gson.JsonElement;
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
		static String isbn = "";
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
		static Map<String,String> codelanguageMap = new HashMap<String,String>();
		static ISSNValidator issnValidator = ISSNValidator.getInstance();
		static String pubmed_id = "";
		
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
			initlanguageMap(context); 
		}
		
		private static void initlanguageMap(Context context) throws IOException {  
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/_rel_file/code_language.txt"));
//			File file = new File("E:\\temps\\scopus\\test\\code_language.txt");  
//	        FileInputStream fin = new FileInputStream(file);  
			BufferedReader in = null;
			String line;
			try {  
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 3) {
						continue;
					} 
					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					String language = vec[1].toUpperCase().trim();
	
					String code = vec[0].trim();
					String infodata = language + "_" + code;
	 		       	if (language.length() < 1) {
	 		    	   continue;
	 		       	}
	 		       	if (code.length() < 1) {
			           continue;
	 		       	} 
			       codelanguageMap.put(language, code);
				}
		   } finally {
		    if (in != null) {
		     in.close();
		    }
		   }
		//   LogMR.log2HDFS4Mapper(context, logHDFSFile, "languageMap: " +codelanguageMap ); 
			 
//			for (Map.Entry<String, String> entry :codelanguageMap.entrySet()) { 
//				System.out.println(entry.getKey() + ":" + entry.getValue()); 
//			}
	  }
		

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public static boolean parseJson(Map<String, JsonElement> jsonMap) {  
			 
			rawid = jsonMap.get("EID")==null?"":jsonMap.get("EID").getAsString(); 
			doi = jsonMap.get("DOI")==null?"":jsonMap.get("DOI").getAsString();
			provider_url = jsonMap.get("Link")==null?"":jsonMap.get("Link").getAsString();
//			if(!provider_url.equals("")) {
//				String[] tmpspl = provider_url.split("?")[1].split("&");
//				String eid = tmpspl[0].split("=")[1];
//			}
			title = (jsonMap.get("Title")==null || jsonMap.get("Title").getAsString().trim().equals("[No title available]"))?"":jsonMap.get("Title").getAsString();
			keyword = jsonMap.get("Author Keywords")==null?"":jsonMap.get("Author Keywords").getAsString().replace("; ", ";"); 
			subject_word = jsonMap.get("Index Keywords")==null?"":jsonMap.get("Index Keywords").getAsString().replace("; ", ";");
			abstract_ = (jsonMap.get("Abstract")==null || jsonMap.get("Abstract").getAsString().trim().equals("[No abstract available]"))?"":jsonMap.get("Abstract").getAsString();  
//			if(jsonMap.get("Abstract")==null || jsonMap.get("Abstract").getAsString().trim().equals("[No abstract available]")) {
//				System.out.println(abstract_+"----"+provider_url);
//			}
			begin_page = jsonMap.get("Page start")==null?"":jsonMap.get("Page start").getAsString(); 
			end_page = 	jsonMap.get("Page end")==null?"":jsonMap.get("Page end").getAsString(); 
			//doc_no = jsonMap.get("Art. No.")==null?"":jsonMap.get("Art. No.").getAsString(); 
			raw_type = jsonMap.get("Document Type")==null?"":jsonMap.get("Document Type").getAsString().trim();
			page_cnt = jsonMap.get("Page count")==null?"":jsonMap.get("Page count").getAsString(); 
			
			fund = jsonMap.get("Funding Details")==null?"":jsonMap.get("Funding Details").getAsString(); 
			fund_alt = jsonMap.get("Funding Text 1")==null?"":jsonMap.get("Funding Text 1").getAsString();
			
			String author_str = jsonMap.get("Authors")==null?"":jsonMap.get("Authors").getAsString();
			String auids_str = jsonMap.get("Author(s) ID")==null?"":jsonMap.get("Author(s) ID").getAsString();
			String organ_str = jsonMap.get("Affiliations")==null?"":jsonMap.get("Affiliations").getAsString();
			String auor_str = jsonMap.get("Authors with affiliations")==null?"":jsonMap.get("Authors with affiliations").getAsString();
			String[] aus = null,auids = null,organs = null,auors = null;
			if(author_str!=null && !author_str.equals("") && !author_str.trim().equals("[No author name available]")) {
				aus = author_str.replace("'", "").split(",");
			}
			if(auids_str!=null && !auids_str.equals("")&& !auids_str.trim().equals("[No author id available]")) {
				auids = auids_str.split(";");
			}
			if(organ_str!=null && !organ_str.equals("")) {
				organs = organ_str.split(";");
			}
			if(auor_str!=null && !auor_str.equals("")) {//作者与机构关系
				auors = auor_str.split(";");
			}
			if(aus != null && auids != null && aus.length == auids.length) {
				StringBuilder sb = new StringBuilder();
				for(int i=0;i<aus.length;i++) {
					sb.append(auids[i]).append("@").append(aus[i]).append(";");
				}
				author_id = sb.deleteCharAt(sb.length()-1).toString(); 
			}
			if(auors != null && auors.length>0) { 
				LinkedHashMap<String, String> authorMap = new LinkedHashMap<String, String>();
				for(int i=0;i<aus.length;i++) {
					for(int j=0;j<auors.length;j++) {
						if(auors[j].replace(",", "").replace(".", "").replace(" ", "").indexOf(aus[i].replace(",", "").replace(".", "").replace(" ", "")) != -1) { 
							String org = "";
							if(organs != null && organs.length>0) {
								for(int k=0;k<organs.length;k++) {
									if(auors[j].replace(",", "").replace(".", "").replace(" ", "").indexOf(organs[k].replace(",", "").replace(".", "").replace(" ", "")) != -1) { 
										org = org + organs[k].trim() + ";";
									} 
								}
							} 
							if(!org.equals("")) {
								authorMap.put(aus[i].trim().replace("[", "!").replace("]", "#"), org.substring(0,org.length()-1));
							}else {
								authorMap.put(aus[i].trim().replace("[", "!").replace("]", "#"), org);
							} 
							break;
						} 
					} 
				}  
//				System.out.println(provider_url);
//				for (Map.Entry<String, String> entry : authorMap.entrySet()) { 
//					System.out.println(entry.getKey() + ":" + entry.getValue()); 
//				}
				String[] result = AuthorOrgan.numberByMap(authorMap);
				if(authorMap.size()>0) {
					Map.Entry<String, String> fisrt = authorMap.entrySet().iterator().next();
					author_1st = fisrt.getKey().replace("!", "[").replace("#", "]");
					author = result[0].replace("!", "[").replace("#", "]");
					organ_1st = fisrt.getValue();
					organ = result[1];   
				} 
			}  
			String address = jsonMap.get("Correspondence Address")==null?"":jsonMap.get("Correspondence Address").getAsString(); 
			if(address != null || !address.equals("")) {
				String[] addrs = address.split(";");
				String[] tmpemail = addrs[addrs.length-1].split(":");
				if(tmpemail != null && tmpemail.length==2) {
					email = tmpemail[1] + ":" + addrs[0];
				} 
			} 
			journal_name = jsonMap.get("Source title")==null?"":jsonMap.get("Source title").getAsString();
			
			pub_year = jsonMap.get("Year")==null?"":jsonMap.get("Year").getAsString();  
			if(pub_year == null || pub_year.equals("")) {
				pub_date = "19000000";
				pub_year = "1900";  
			}else {
				pub_date = pub_year + "0000";
			}
			vol = jsonMap.get("Volume")==null?"":jsonMap.get("Volume").getAsString();
			num = jsonMap.get("Issue")==null?"":jsonMap.get("Issue").getAsString();
			issn = jsonMap.get("ISSN")==null?"":jsonMap.get("ISSN").getAsString();
			if(issn != null && !issn.equals("")) { 
				if(issn.length()==8) {
					issn = issn.substring(0, 4) + "-" + issn.substring(4, 8);
				}else if(issn.indexOf("-") == -1){
					if(issn.length()==7) {
						String istmp = "0"+issn;
						istmp = istmp.substring(0, 4) + "-" + istmp.substring(4, 8);
						if(issnValidator.isValid(istmp)){
							issn = istmp;
						}
					}else if(issn.length() < 5) {
						issn = "";
					}
					System.out.println(issn+"----"+provider_url);
				} 
			}
			isbn = jsonMap.get("ISBN")==null?"":jsonMap.get("ISBN").getAsString(); 
			if(isbn != null && !isbn.equals("")) { 
				isbn = isbn.replace("-", "");
			}
			if(!issn.equals("")) {
				source_type = "3";
			}
			publisher = jsonMap.get("Publisher")==null?"":jsonMap.get("Publisher").getAsString();
			String isa = jsonMap.get("Access Type")==null?"":jsonMap.get("Access Type").getAsString();
			is_oa = "0";
			if(isa != null && isa.trim().equals("Open Access")){
				is_oa = "1";
			} 
			language = "";
			String lgstr = jsonMap.get("Language of Original Document")==null?"":jsonMap.get("Language of Original Document").getAsString();
			if(lgstr != null && !lgstr.equals("")){  
				String[] lgStrings = lgstr.trim().toUpperCase().split(";");
				if(lgStrings.length>1) { 
					for(String lg : lgStrings) {
						String tmp = codelanguageMap.get(lg.trim());
						if(tmp==null || tmp.equals("")) {
							tmp = "UN";
						}
						language = language + tmp + ";" ;
					}  
					language = language.substring(0, language.length()-1); 
				}else {
					language = codelanguageMap.get(lgstr.trim().toUpperCase());
				} 
				if(language == null || language.equals("")) {
					language = "UN";
				} 
			}else {
				language = "UN";
			}
			String ref = jsonMap.get("References")==null?"":jsonMap.get("References").getAsString();
			if(ref != null && !ref.equals("")){
				ref_cnt = String.valueOf(ref.split(";").length);
			} 
			cited_cnt = jsonMap.get("Cited by")==null?"":jsonMap.get("Cited by").getAsString();
			
			pubmed_id = jsonMap.get("PubMed ID")==null?"":jsonMap.get("PubMed ID").getAsString();
			
//			XXXXObject xObj = new XXXXObject(); 
//			xObj.data.put("down_date", down_date);
//			xObj.data.put("batch", batch);
//			xObj.data.put("rawid", rawid); 
//			xObj.data.put("doi", doi); 
//			xObj.data.put("provider_url", provider_url);
//			xObj.data.put("title", title);
//			xObj.data.put("keyword", keyword);
//			xObj.data.put("abstract", abstract_); 
//			xObj.data.put("begin_page", begin_page); 
//			xObj.data.put("end_page", end_page); 
//			xObj.data.put("page_cnt", page_cnt); 
//			xObj.data.put("doc_no", doc_no);
//			xObj.data.put("raw_type", raw_type); 
//			xObj.data.put("pub_date", pub_date); 
//			xObj.data.put("fund", fund);
//			xObj.data.put("fund_alt", fund_alt); 
//			xObj.data.put("author_id", author_id); 
//			xObj.data.put("author_1st", author_1st);
//			xObj.data.put("author", author);
//			xObj.data.put("email", email);
//			xObj.data.put("organ_1st", organ_1st);
//			xObj.data.put("organ", organ); 
//			xObj.data.put("journal_name", journal_name);
//			xObj.data.put("pub_year", pub_year);
//			xObj.data.put("vol", vol);
//			xObj.data.put("num", num);
//			xObj.data.put("issn", issn); 
//			xObj.data.put("isbn", isbn); 
//			xObj.data.put("publisher", publisher); 
//			xObj.data.put("is_oa", is_oa);
//			xObj.data.put("language", language); 
//			xObj.data.put("ref_cnt", ref_cnt);
//			xObj.data.put("cited_cnt", cited_cnt); 
			
			
//			for (Map.Entry<String, String> entry : xObj.data.entrySet()) { 
//				System.out.println(entry.getKey() + ":" + entry.getValue()); 
//			}
			System.out.println("======================================"); 
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
				isbn = "";
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
				pubmed_id = "";
				source_type = ""; 
			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapJson = gson.fromJson(value.toString(), type); 
			down_date = "20190416"; 
			parseJson(mapJson);  
//			if(isbn.equals("") || rawid == null || rawid.equals("") || rawid.indexOf("-")==-1 || title.length() < 2) {
//				return;
//			}
//			if(source_type.equals("") || rawid == null || rawid.equals("") || rawid.indexOf("-")==-1 || title.length() < 2) {
//				return;
//			}
			if(issn.trim().length() > 0 || rawid == null || rawid.equals("") || rawid.indexOf("-")==-1 || title.length() < 2) {
				return;
			} 
			
			XXXXObject xObj = new XXXXObject(); 
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("rawid", rawid); 
			xObj.data.put("doi", doi); 
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("title", title);
			xObj.data.put("keyword", keyword);
			xObj.data.put("subject_word", subject_word); 
			xObj.data.put("abstract", abstract_); 
			xObj.data.put("begin_page", begin_page); 
			xObj.data.put("end_page", end_page); 
			xObj.data.put("page_cnt", page_cnt);  
			xObj.data.put("raw_type", raw_type); 
			xObj.data.put("pub_date", pub_date); 
			xObj.data.put("fund", fund);
			xObj.data.put("fund_alt", fund_alt); 
			xObj.data.put("author_id", author_id); 
			xObj.data.put("author_1st", author_1st);
			xObj.data.put("author", author);
			xObj.data.put("email", email);
			xObj.data.put("organ_1st", organ_1st);
			xObj.data.put("organ", organ); 
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("vol", vol);
			xObj.data.put("num", num);
			xObj.data.put("issn", issn); 
			xObj.data.put("isbn", isbn); 
			xObj.data.put("publisher", publisher); 
			xObj.data.put("is_oa", is_oa);
			xObj.data.put("language", language); 
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("cited_cnt", cited_cnt); 
			if(!cited_cnt.equals("")) { 
				xObj.data.put("cited_cnt", cited_cnt + "@" + down_date); 
			}  
			xObj.data.put("pubmed_id", pubmed_id);
			
			String sub_db_id = "00020"; 
			xObj.data.put("lngid", VipIdEncode.getLngid(sub_db_id, rawid, false)); 
			xObj.data.put("sub_db_id", sub_db_id); 
			xObj.data.put("sub_db", "QK");
			xObj.data.put("product", "SCOPUS"); 
			xObj.data.put("provider", "ELSEVIER");
			xObj.data.put("source_type", source_type);
			xObj.data.put("country", "UN");
			xObj.data.put("language", language); 
 
			
			context.getCounter("map", "count").increment(1); 
			context.getCounter("map", raw_type.toLowerCase()).increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			
			
//			
			
			
//			String logHDFSFile = "/user/qinym/log/" + DateTimeHelper.getNowDate() + "title.txt";
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "author:" + author);
		}

	}
}