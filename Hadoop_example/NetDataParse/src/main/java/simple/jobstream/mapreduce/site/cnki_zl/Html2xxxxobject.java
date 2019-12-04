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















import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2xxxxobject extends InHdfsOutHdfsJobInfo {
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
		job.setReducerClass(ProcessReducer.class);
		
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
		
		public static HashMap<String,String> parseHtml(String htmlText){
			
			String DownDate = "";
			String db = "";
			String filename = "";
			String detail = "";
			String legal_status = "";
			JSONTokener jsonTokener = new JSONTokener(htmlText); 
			
			
			try{
				JSONObject o = (JSONObject) jsonTokener.nextValue();
				DownDate = o.getString("DownDate");
				db = o.getString("db");
				filename = o.getString("filename");
				detail = o.getString("detail");
				legal_status = o.getString("legal_status");

			}catch(Exception e){
				return null;
			}
			HashMap<String,String> map = new HashMap<String,String>();
			
				
			detail = detail.replace("<![CDATA[", "");
			
			
			Document doc = Jsoup.parse(detail);
			
			Element tableElement = doc.select("table[id = box]").first();
			if(tableElement == null){
				return null;
			}
			Element titleElement = doc.select("td[style = font-size:18px;font-weight:bold;text-align:center;]").first();
			if(titleElement == null){
				return null;

			}
			String title = titleElement.text().trim();
			map.put("title", title);

			Elements trElemens = tableElement.select("tr");
			if(trElemens == null){
				return null;
			}
			int i = 0;
			String identifier_issn = "";//申请号
			String date_created = ""; //申请日
			String identifier_standard = "";//公开号
			String date_impl = "";//公开日
			String creator_cluster = "";//申请人
			String creator_institution = "";//申请人地址
			String creator = "";//发明人
			String agency = "";//代理机构
			String agents = "";//代理人
			String province_code = "";//国省代码
			String description = "";//摘要
			String subject_csc = "";//主分类号
			String subject_isc = "";//专利分类号
			String page = "";
			String description_core = "";
			String legalState = "";
			

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
							date_created = itemStr.replace("【申请日】", "").trim().replace("  ", "");

							
						}else if(itemStr.contains("【公开号】")){
							identifier_standard = itemStr.replace("【公开号】", "").trim().replace("  ", "");

						}else if(itemStr.contains("【公开日】")){
							date_impl = itemStr.replace("【公开日】", "").trim().replace("  ", "");
	
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
				return null;
			}
			
			doc = Jsoup.parse(legal_status);

			Element tableTagElement = doc.select("table[class = state]").first();
			if(tableTagElement == null){
				legalState = "";
				
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
						legalState = legalState + line;
					}

				}

			
			}
			
			if(legal_status.contains("未查询到本专利法律状态信息")){
				legalState = "not";
			}
			
			map.put("legalState", legalState);
			map.put("DownDate", DownDate);
			map.put("db", db);
			map.put("subject_isc", subject_isc);
			map.put("subject_csc", subject_csc);
			map.put("province_code", province_code);
			map.put("description", description);
			map.put("agents", agents);
			map.put("agency", agency);
			map.put("creator", creator);
			map.put("creator_institution", creator_institution);
			map.put("creator_cluster", creator_cluster);
			map.put("date_impl", date_impl);
			map.put("identifier_standard", identifier_standard);
			map.put("date_created", date_created);
			map.put("identifier_issn", identifier_issn);
			map.put("page", page);
			map.put("description_core", description_core);
			
	

		



			return map;
		}
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	HashMap<String, String> map = new HashMap<String, String>();
	    	map = parseHtml(value.toString());
	    	

			
			

	    	if(map != null){
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("title", map.get("title"));
				
				xObj.data.put("identifier_issn", map.get("identifier_issn"));
				xObj.data.put("date_created", map.get("date_created"));
				xObj.data.put("identifier_standard", map.get("identifier_standard"));
				xObj.data.put("date_impl", map.get("date_impl"));
				xObj.data.put("creator_cluster", map.get("creator_cluster"));
				xObj.data.put("creator_institution", map.get("creator_institution"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("agency", map.get("agency"));
				xObj.data.put("agents", map.get("agents"));
				xObj.data.put("province_code", map.get("province_code"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("subject_csc", map.get("subject_csc"));
				xObj.data.put("subject_isc", map.get("subject_isc"));
				xObj.data.put("page", map.get("page"));
				xObj.data.put("description_core", map.get("description_core"));
				xObj.data.put("legalState", map.get("legalState"));
				xObj.data.put("DownDate", map.get("DownDate"));
				xObj.data.put("db", map.get("db"));
				xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));
		    	xObj.data.put("down_date", map.get("DownDate"));
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("identifier_issn")), new BytesWritable(bytes));	
	    	}else{
	    		return;
	    	}
		}
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//选最大的一个
					bOut = item;
				}
			}
			
			context.getCounter("reduce", "count").increment(1);
		
			context.write(key, bOut);
		}
	}
}
