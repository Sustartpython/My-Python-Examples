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
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//统计wos和ei的数据量
public class Json2RawXXXXObject extends InHdfsOutHdfsJobInfo {
	public static Logger logger = Logger.getLogger(Json2RawXXXXObject.class);
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

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));

		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

//		JobConfUtil.setTaskPerMapMemory(job, 1024 * 10);
		JobConfUtil.setTaskPerReduceMemory(job, 1024 * 6);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		TextOutputFormat.setCompressOutput(job, false);
		
		job.setNumReduceTasks(reduceNum);
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

//		static String rawid = ""; 
		static String down_date = "";
		static String batch = "";
//		
//		static String author = ""; 
//		static String author_id = "";
//		static String title = ""; 
//		static String pub_year = "";
//		static String journal_name = "";
//		static String vol = "";
//		static String num = "";
//		static String art_no = "";
//		static String begin_page = "";
//		static String end_page = ""; 
//		static String page_cnt = "";
//		static String cited_cnt = ""; 
//		static String doi = "";  
//		static String link = "";  
//		static String organ = "";
//		static String au_organ = "";
//		static String abstract_ = ""; 
//		static String keyword = "";  
//		static String subject_word = ""; 
//		static String msnum = "";
//		static String cas = "";
//		static String tradenames = "";
//		static String manufacturers = "";
//		static String fund = "";  
//		static String fund_alt = "";
//		static String references = "";
//		static String address = "";
//		static String editors = "";
//		static String sponsors = "";
//		static String publisher = "";
//		static String confer_name = "";
//		static String confer_date = "";
//		static String confer_location = "";
//		static String confer_code = "";
//		static String issn = ""; 
//		static String isbn = ""; 
//		static String coden = ""; 
//		static String pubmed_id = ""; 
//		static String language = ""; 
//		static String abbr_source_title = ""; 
//		static String raw_type = ""; 
//		static String pub_stage = ""; 
//		static String is_oa = "";
//		static String source = ""; 
//		static String eid = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

//		public static boolean parseJson(Map<String, JsonElement> jsonMap) {  
//			 
//			rawid = jsonMap.get("EID")==null?"":jsonMap.get("EID").getAsString(); 
//			eid = rawid;
//			for (Map.Entry<String, JsonElement> entry : jsonMap.entrySet()) { 
//				System.out.println(entry.getKey() + ":" + entry.getValue()); 
//			}
//			
//			
//			doi = jsonMap.get("DOI")==null?"":jsonMap.get("DOI").getAsString();
//			provider_url = jsonMap.get("Link")==null?"":jsonMap.get("Link").getAsString();
////			if(!provider_url.equals("")) {
////				String[] tmpspl = provider_url.split("?")[1].split("&");
////				String eid = tmpspl[0].split("=")[1];
////			}
//			title = (jsonMap.get("Title")==null || jsonMap.get("Title").getAsString().trim().equals("[No title available]"))?"":jsonMap.get("Title").getAsString();
//			keyword = jsonMap.get("Author Keywords")==null?"":jsonMap.get("Author Keywords").getAsString().replace("; ", ";"); 
//			subject_word = jsonMap.get("Index Keywords")==null?"":jsonMap.get("Index Keywords").getAsString().replace("; ", ";");
//			abstract_ = (jsonMap.get("Abstract")==null || jsonMap.get("Abstract").getAsString().trim().equals("[No abstract available]"))?"":jsonMap.get("Abstract").getAsString();  
////			if(jsonMap.get("Abstract")==null || jsonMap.get("Abstract").getAsString().trim().equals("[No abstract available]")) {
////				System.out.println(abstract_+"----"+provider_url);
////			}
//			begin_page = jsonMap.get("Page start")==null?"":jsonMap.get("Page start").getAsString(); 
//			end_page = 	jsonMap.get("Page end")==null?"":jsonMap.get("Page end").getAsString(); 
//			//doc_no = jsonMap.get("Art. No.")==null?"":jsonMap.get("Art. No.").getAsString(); 
//			raw_type = jsonMap.get("Document Type")==null?"":jsonMap.get("Document Type").getAsString().trim();
//			page_cnt = jsonMap.get("Page count")==null?"":jsonMap.get("Page count").getAsString(); 
//			
//			fund = jsonMap.get("Funding Details")==null?"":jsonMap.get("Funding Details").getAsString(); 
//			fund_alt = jsonMap.get("Funding Text 1")==null?"":jsonMap.get("Funding Text 1").getAsString();
//			
//			String author_str = jsonMap.get("Authors")==null?"":jsonMap.get("Authors").getAsString();
//			String auids_str = jsonMap.get("Author(s) ID")==null?"":jsonMap.get("Author(s) ID").getAsString();
//			String organ_str = jsonMap.get("Affiliations")==null?"":jsonMap.get("Affiliations").getAsString();
//			String auor_str = jsonMap.get("Authors with affiliations")==null?"":jsonMap.get("Authors with affiliations").getAsString();
//			String[] aus = null,auids = null,organs = null,auors = null;
//			if(author_str!=null && !author_str.equals("") && !author_str.trim().equals("[No author name available]")) {
//				aus = author_str.replace("'", "").split(",");
//			}
//			if(auids_str!=null && !auids_str.equals("")&& !auids_str.trim().equals("[No author id available]")) {
//				auids = auids_str.split(";");
//			}
//			if(organ_str!=null && !organ_str.equals("")) {
//				organs = organ_str.split(";");
//			}
//			if(auor_str!=null && !auor_str.equals("")) {//作者与机构关系
//				auors = auor_str.split(";");
//			}
//			if(aus != null && auids != null && aus.length == auids.length) {
//				StringBuilder sb = new StringBuilder();
//				for(int i=0;i<aus.length;i++) {
//					sb.append(auids[i]).append("@").append(aus[i]).append(";");
//				}
//				author_id = sb.deleteCharAt(sb.length()-1).toString(); 
//			}
//			if(auors != null && auors.length>0) { 
//				LinkedHashMap<String, String> authorMap = new LinkedHashMap<String, String>();
//				for(int i=0;i<aus.length;i++) {
//					for(int j=0;j<auors.length;j++) {
//						if(auors[j].replace(",", "").replace(".", "").replace(" ", "").indexOf(aus[i].replace(",", "").replace(".", "").replace(" ", "")) != -1) { 
//							String org = "";
//							if(organs != null && organs.length>0) {
//								for(int k=0;k<organs.length;k++) {
//									if(auors[j].replace(",", "").replace(".", "").replace(" ", "").indexOf(organs[k].replace(",", "").replace(".", "").replace(" ", "")) != -1) { 
//										org = org + organs[k].trim() + ";";
//									} 
//								}
//							} 
//							if(!org.equals("")) {
//								authorMap.put(aus[i].trim().replace("[", "!").replace("]", "#"), org.substring(0,org.length()-1));
//							}else {
//								authorMap.put(aus[i].trim().replace("[", "!").replace("]", "#"), org);
//							} 
//							break;
//						} 
//					} 
//				}  
////				System.out.println(provider_url);
////				for (Map.Entry<String, String> entry : authorMap.entrySet()) { 
////					System.out.println(entry.getKey() + ":" + entry.getValue()); 
////				}
//				String[] result = AuthorOrgan.numberByMap(authorMap);
//				if(authorMap.size()>0) {
//					Map.Entry<String, String> fisrt = authorMap.entrySet().iterator().next();
//					author_1st = fisrt.getKey().replace("!", "[").replace("#", "]");
//					author = result[0].replace("!", "[").replace("#", "]");
//					organ_1st = fisrt.getValue();
//					organ = result[1];   
//				} 
//			}  
//			String address = jsonMap.get("Correspondence Address")==null?"":jsonMap.get("Correspondence Address").getAsString(); 
//			if(address != null || !address.equals("")) {
//				String[] addrs = address.split(";");
//				String[] tmpemail = addrs[addrs.length-1].split(":");
//				if(tmpemail != null && tmpemail.length==2) {
//					email = tmpemail[1] + ":" + addrs[0];
//				} 
//			} 
//			journal_name = jsonMap.get("Source title")==null?"":jsonMap.get("Source title").getAsString();
//			
//			pub_year = jsonMap.get("Year")==null?"":jsonMap.get("Year").getAsString();  
//			if(pub_year == null || pub_year.equals("")) {
//				pub_date = "19000000";
//				pub_year = "1900";  
//			}else {
//				pub_date = pub_year + "0000";
//			}
//			vol = jsonMap.get("Volume")==null?"":jsonMap.get("Volume").getAsString();
//			num = jsonMap.get("Issue")==null?"":jsonMap.get("Issue").getAsString();
//			issn = jsonMap.get("ISSN")==null?"":jsonMap.get("ISSN").getAsString();
//			if(issn != null && !issn.equals("")) { 
//				if(issn.length()==8) {
//					issn = issn.substring(0, 4) + "-" + issn.substring(4, 8);
//				}else if(issn.indexOf("-") == -1){
//					if(issn.length()==7) {
//						String istmp = "0"+issn;
//						istmp = istmp.substring(0, 4) + "-" + istmp.substring(4, 8);
//						if(issnValidator.isValid(istmp)){
//							issn = istmp;
//						}
//					}else if(issn.length() < 5) {
//						issn = "";
//					}
//					System.out.println(issn+"----"+provider_url);
//				} 
//			}
//			isbn = jsonMap.get("ISBN")==null?"":jsonMap.get("ISBN").getAsString(); 
//			if(isbn != null && !isbn.equals("")) { 
//				isbn = isbn.replace("-", "");
//			}
//			if(!issn.equals("")) {
//				source_type = "3";
//			}
//			publisher = jsonMap.get("Publisher")==null?"":jsonMap.get("Publisher").getAsString();
//			String isa = jsonMap.get("Access Type")==null?"":jsonMap.get("Access Type").getAsString();
//			is_oa = "0";
//			if(isa != null && isa.trim().equals("Open Access")){
//				is_oa = "1";
//			} 
//			language = "";
//			String lgstr = jsonMap.get("Language of Original Document")==null?"":jsonMap.get("Language of Original Document").getAsString();
//			if(lgstr != null && !lgstr.equals("")){  
//				String[] lgStrings = lgstr.trim().toUpperCase().split(";");
//				if(lgStrings.length>1) { 
//					for(String lg : lgStrings) {
//						String tmp = codelanguageMap.get(lg.trim());
//						if(tmp==null || tmp.equals("")) {
//							tmp = "UN";
//						}
//						language = language + tmp + ";" ;
//					}  
//					language = language.substring(0, language.length()-1); 
//				}else {
//					language = codelanguageMap.get(lgstr.trim().toUpperCase());
//				} 
//				if(language == null || language.equals("")) {
//					language = "UN";
//				} 
//			}else {
//				language = "UN";
//			}
//			String ref = jsonMap.get("References")==null?"":jsonMap.get("References").getAsString();
//			if(ref != null && !ref.equals("")){
//				ref_cnt = String.valueOf(ref.split(";").length);
//			} 
//			cited_cnt = jsonMap.get("Cited by")==null?"":jsonMap.get("Cited by").getAsString();
//			
//			pubmed_id = jsonMap.get("PubMed ID")==null?"":jsonMap.get("PubMed ID").getAsString();
//			
// 
//			System.out.println("======================================"); 
//			return true;
//		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				down_date = "";
				batch = "";
			}
//			{
//				rawid = ""; 
//				down_date = "";
//				batch = "";
//				
//				author = ""; 
//				author_id = "";
//				title = ""; 
//				pub_year = "";
//				journal_name = "";
//				vol = "";
//				num = "";
//				art_no = "";
//				begin_page = "";
//				end_page = ""; 
//				page_cnt = "";
//				cited_cnt = ""; 
//				doi = "";  
//				link = "";  
//				organ = "";
//				au_organ = "";
//				abstract_ = ""; 
//				keyword = "";  
//				subject_word = ""; 
//				msnum = "";
//				cas = "";
//				tradenames = "";
//				manufacturers = "";
//				fund = "";  
//				fund_alt = "";
//				references = "";
//				address = "";
//				editors = "";
//				sponsors = "";
//				publisher = "";
//				confer_name = "";
//				confer_date = "";
//				confer_location = "";
//				confer_code = "";
//				issn = ""; 
//				isbn = ""; 
//				coden = ""; 
//				pubmed_id = ""; 
//				language = ""; 
//				abbr_source_title = ""; 
//				raw_type = ""; 
//				pub_stage = ""; 
//				is_oa = "";
//				source = ""; 
//				eid = ""; 
//			}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> jsonMap = gson.fromJson(value.toString(), type);
			down_date = "20190416";
			String rawid = jsonMap.get("EID") == null ? "" : jsonMap.get("EID").getAsString();
			String title = (jsonMap.get("Title") == null
					|| jsonMap.get("Title").getAsString().trim().equals("[No title available]")) ? ""
							: jsonMap.get("Title").getAsString();
			if (rawid == null || rawid.equals("") || rawid.indexOf("-") == -1 || title.length() < 2) {
				return;
			}
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("rawid", rawid);
			for (Map.Entry<String, JsonElement> entry : jsonMap.entrySet()) {
				xObj.data.put(entry.getKey(), entry.getValue() == null ? "" : entry.getValue().getAsString().trim());
			}
			context.getCounter("map", "count").increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

//			String logHDFSFile = "/user/qinym/log/" + DateTimeHelper.getNowDate() + "title.txt";
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "author:" + author);
		}

	}
}