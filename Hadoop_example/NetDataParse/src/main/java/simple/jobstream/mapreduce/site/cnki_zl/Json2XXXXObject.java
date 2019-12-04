package simple.jobstream.mapreduce.site.cnki_zl;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;


//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject  extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 100;
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Html2XXXXObject";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}
	
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    
		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job)
	{
	
	}

	public String GetHdfsInputPath()
	{
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath()
	{
		return outputHdfsPath;
	}
	
	public static class ProcessMapper extends 
			Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		private static String rawid = "";
		private static String lngid = "";
		private static String title = "";
		private static String identifier_issn = "";// 申请号
		private static String date_created = ""; // 申请日
		private static String identifier_standard = "";// 公开号
		private static String date_impl = "";// 公开日
		private static String creator_cluster = "";// 申请人
		private static String creator_institution = "";// 申请人地址
		private static String creator = "";// 发明人
		private static String agency = "";// 代理机构
		private static String agents = "";// 代理人
		private static String province_code = "";// 国省代码
		private static String description = "";// 摘要
		private static String subject_csc = "";// 主分类号
		private static String subject_isc = "";// 专利分类号
		private static String language = "ZH";
		private static String country = "CN";
		private static String provider = "";
		private static String provider_url = "";
		private static String provider_id = "";
		private static String type = "7";
		private static String medium = "2";
		private static String batch = "";
		private static String date = "";
		private static String owner = "";
		private static String page = "";
		private static String description_core = "";
		private static String legal_status = "";
		private static String description_type = "";
		private static String sub_db_id = "00003";
		private static String author_1st = "";
		private static String clc_no_1st = "";
		private static String raw_type = "7";
		private static String down_date="";
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}
		
		public void parseHtml(String DownDateStr,String DB,String Filename,String Detail,String Legal_status){
			{
				// 老字段
				rawid = "";
				lngid = "";
				title = "";
				identifier_issn = "";// 申请号
				date_created = ""; // 申请日
				identifier_standard = "";// 公开号
				date_impl = "";// 公开日
				creator_cluster = "";// 申请人
				creator_institution = "";// 申请人地址
				creator = "";// 发明人
				agency = "";// 代理机构
				agents = "";// 代理人
				province_code = "";// 国省代码
				description = "";// 摘要
				subject_csc = "";// 主分类号
				subject_isc = "";// 专利分类号
				language = "ZH";
				country = "CN";
				provider = "";
				provider_url = "";
				provider_id = "";
				type = "7";
				medium = "2";
				date = "";
				owner = "";
				page = "";
				description_core = "";
				legal_status = "";
				description_type = "";
				sub_db_id = "00003";
				raw_type = "7";
				down_date = "";
			}
			
			String DownDate = DownDateStr;
			String db = DB;
			String filename = Filename;
			String detail = Detail;
			String legal_status_str=Legal_status;
//			JSONTokener jsonTokener = new JSONTokener(htmlText); 		
//			try{
//				JSONObject o = (JSONObject) jsonTokener.nextValue();
//				DownDate = o.getString("DownDate");
//				db = o.getString("db");
//				filename = o.getString("filename");
//				detail = o.getString("detail");
//				legal_status = o.getString("legal_status");
//
//			}catch(Exception e){
//				return null;
//			}
			HashMap<String,String> map = new HashMap<String,String>();
			
				
			detail = detail.replace("<![CDATA[", "");
			
			
			Document doc = Jsoup.parse(detail);
			
			Element tableElement = doc.select("table[id = box]").first();
			if(tableElement == null){
				return;
			}
			Element titleElement = doc.select("td[style = font-size:18px;font-weight:bold;text-align:center;]").first();
			if(titleElement == null){
				return;

			}
			title = titleElement.text().trim();

			Elements trElemens = tableElement.select("tr");
			if(trElemens == null){
				return;
			}
			int i = 0;
		

			for(Element tr:trElemens){
				Elements tdElements = tr.select("td");
				String itemStr = "";
				String [] itemStrArray;
				for(Element td:tdElements){
					
					if (i % 2 == 1){
						itemStr = itemStr + td.text();
						//System.out.println(itemStr);
						
						if(itemStr.contains("【申请号】")){
							identifier_issn = itemStr.replace("【申请号】", "").trim().replace("  ", "");
							
							
						}else if(itemStr.contains("【申请日】")){
							date_created = itemStr.replace("【申请日】", "").trim().replace("  ", "").replace("-", "");;

							
						}else if(itemStr.contains("【公开号】")){
							identifier_standard = itemStr.replace("【公开号】", "").trim().replace("  ", "");

						}else if(itemStr.contains("【公开日】")){
							date_impl = itemStr.replace("【公开日】", "").trim().replace("  ", "").replace("-", "");;
	
						}else if(itemStr.contains("【申请人】")){
							creator_cluster = itemStr.replace("【申请人】", "").trim().replace("  ", "");
	
						}else if(itemStr.contains("【地址】")){
							creator_institution = itemStr.replace("【地址】", "").trim().replace(" ", "");
	
						}else if(itemStr.contains("【发明人】")){
							creator = itemStr.replace("【发明人】", "").trim().replace("  ", "");

						}else if(itemStr.contains("【专利代理机构】")){
							agency = itemStr.replace("【专利代理机构】", "").trim().replace("  ", "");
		
						}else if(itemStr.contains("【代理人】")){
							agents = itemStr.replace("【代理人】", "").trim().replace("  ", "");

						}else if(itemStr.contains("【摘要】")){
							description = itemStr.replace("【摘要】", "").trim().replace("  ", "");

						}else if(itemStr.contains("【国省代码】")){
							province_code = itemStr.replace("【国省代码】", "").trim().replace("  ", "");
		
						}else if(itemStr.contains("【主分类号】")){
							subject_csc = itemStr.replace("【主分类号】", "").trim().replace("  ", "");
	
						}else if(itemStr.contains("【专利分类号】")){
							subject_isc = itemStr.replace("【专利分类号】", "").trim().replace("  ", "");
	
						}else if(itemStr.contains("【页数】")){
							page = itemStr.replace("【页数】", "").trim().replace(" ", "");
						}else if(itemStr.contains("【主权项】")){
							description_core = itemStr.replace("【主权项】", "").trim().replace(" ", "");
						}
						
						itemStr = "";
					}else{
						itemStr = itemStr + td.text();
					}
					i = i + 1;
				}
			}
			if(identifier_standard.length()==0){
				return;
			}
			
			doc = Jsoup.parse(legal_status_str);

			Element tableTagElement = doc.select("table[class = state]").first();
			if(tableTagElement == null){
				legal_status = "";
				
			}else{
				Elements trTagsElements = tableTagElement.select("tr");
				
				for(Element tr:trTagsElements){
					Elements tdTagsElements = tr.select("td");
					String line = "";
					for (Element td: tdTagsElements){
						line = line + td.text() + "◆";
					}
					if (line.split("◆").length> 1){
						String legalDate = line.split("◆")[0];
						String legalItem = line.split("◆")[1];
						line = "[" + legalDate + "]" + legalItem + ";";
						legal_status = legal_status + line;
					}

				}

			
			}
			
			if(legal_status.contains("未查询到本专利法律状态信息")){
				legal_status = "not";
			}
		}
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);
			//String htmlText,String DownDateStr,String DB,String Filename,String Detail,String Legal_status
			String DownDateStr = mapField.get("DownDate").toString();
			String DB  = mapField.get("db").toString();
			String htmlText  = mapField.get("detail").toString();
			String Legal_status  = mapField.get("legal_status").toString();
			String Filename ="";
			if (mapField.containsKey("filename")) {
				Filename = mapField.get("filename").toString();
			}
		
	    	
			parseHtml(DownDateStr,DB,Filename,htmlText,Legal_status);
			
			// 处理
			rawid = identifier_standard;
			context.getCounter("map", "countAll").increment(1);
			
			if(legal_status.length() < 1){
				context.getCounter("map", "Not legal_status").increment(1);
				return;
			}
			if(legal_status.equals("not")){
				legal_status = "";
			}
			if(rawid.length() <1 ){
				context.getCounter("map", "Not Find rawid").increment(1);
				return;
			}
			
			// 剔除rawid为空的数据
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			
			// 处理rawtype
			 if(DB.equals("SCPD_WG")){
				 description_type = "外观设计";
			 }else if(DB .equals("SCPD_XX")){
				 description_type = "实用新型";
			 }else if(DB .equals("SCPD_FM")){
				 description_type = "发明专利";
			 }	 
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			if (date_impl.length() == 8) {
				date = date_impl.substring(0,4);
				context.getCounter("map", "date_impl.len ==8 ").increment(1);

			} else {
				context.getCounter("map", "date_created.len !=8 ").increment(1);
			}

			if (down_date.length() <1) {
				down_date = "20190617";
			}

			provider_url = "http://dbpub.cnki.net/grid2008/dbpub/detail.aspx?dbname=SCPD&filename=" + rawid;
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00003");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "SCPD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", down_date);
				xObjOut.data.put("batch", batch);
				xObjOut.data.put("source_type", "7");
				xObjOut.data.put("ipc_no", subject_isc);
				xObjOut.data.put("ipc_no_1st", subject_csc);
				xObjOut.data.put("loc_no", "");
				xObjOut.data.put("loc_no_1st", "");
				xObjOut.data.put("cpc_no", "");
				xObjOut.data.put("cpc_no_1st", "");
				xObjOut.data.put("ecla_no", "");
				xObjOut.data.put("ecla_no_1st", "");
				xObjOut.data.put("ccl_no", "");
				xObjOut.data.put("ccl_no_1st", "");
				xObjOut.data.put("fi_no", "");
				xObjOut.data.put("fi_no_1st", "");
				xObjOut.data.put("agency", agency);
				xObjOut.data.put("agent",agents.replace(",", ";").replaceAll(";$", ""));
				xObjOut.data.put("applicant", creator_cluster.replace(",", ";").replaceAll(";$", ""));
				xObjOut.data.put("applicant_addr", creator_institution);
				xObjOut.data.put("claim",description_core);
				xObjOut.data.put("legal_status",legal_status);
				xObjOut.data.put("pct_app_data", "");
				xObjOut.data.put("pct_enter_nation_date", "");
				xObjOut.data.put("pct_pub_data", "");
				xObjOut.data.put("priority", "");
				xObjOut.data.put("priority_date", "");
				xObjOut.data.put("priority_no", "");
				xObjOut.data.put("app_no",identifier_issn);
				xObjOut.data.put("app_date", date_created); // 申请日
				xObjOut.data.put("pub_no",identifier_standard);
				xObjOut.data.put("doi", "");
				xObjOut.data.put("provider_url", provider_url);
				xObjOut.data.put("title", title);
				xObjOut.data.put("keyword", "");
				xObjOut.data.put("clc_no_1st", "");
				xObjOut.data.put("clc_no", "");
				xObjOut.data.put("abstract",description);
				xObjOut.data.put("raw_type",description_type);
				xObjOut.data.put("pub_date", date_impl);// 公开日
				xObjOut.data.put("page_cnt", page);
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("word_cnt", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("fund", "");
				xObjOut.data.put("author", creator.replace(",", ";").replaceAll(";$", ""));
				xObjOut.data.put("organ",creator_cluster.replace(",", ";").replaceAll(";$", ""));
				xObjOut.data.put("organ_area",province_code);
				xObjOut.data.put("pub_year", date);
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
				xObjOut.data.put("family_pub_no", "");
				xObjOut.data.put("ref_cnt", "");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");

			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
