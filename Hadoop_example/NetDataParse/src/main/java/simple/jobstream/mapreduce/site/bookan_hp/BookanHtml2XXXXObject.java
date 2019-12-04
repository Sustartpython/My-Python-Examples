package simple.jobstream.mapreduce.site.bookan_hp;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class BookanHtml2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}
	
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
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
		
		public static String batch = "";
		static int cnt = 0;	
		
		//国家class
		static String getCountrybyString(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("中国", "CN");
			hashTable.put("英国", "UK");
			hashTable.put("日本", "JP");
			hashTable.put("美国", "US");
			hashTable.put("法国", "FR");
			hashTable.put("德国", "DE");
			hashTable.put("韩国", "KR");
			hashTable.put("国际", "UN");
			
			if (null != hashTable.get(text)) {
				text = hashTable.get(text);
			}
			else {
				text = "UN";
			}	
			
			return text;
		}
		//语言class
		static String getLanguagebyCountry(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("CN", "ZH");
			hashTable.put("UK", "EN");
			hashTable.put("US", "EN");
			hashTable.put("JP", "JA");
			hashTable.put("FR", "FR");
			hashTable.put("DE", "DE");
			hashTable.put("UN", "UN");
			hashTable.put("KR", "KR");
			text = hashTable.get(text);
			
			return text;
		}
		public String inputPath = "";
		
		
		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			inputPath = VipcloudUtil.GetInputPath((FileSplit)context.getInputSplit());	//两级路径
		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	String text = value.toString().trim();
	    	
	        Gson gson = new Gson();
	        Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
	        Map<String, JsonElement> mapField = gson.fromJson(text, type);
	    	
	        String  downdate = mapField.get("downdate").getAsString();
	    	
	    	//标题
	    	String title = "";
			//issn (在json里是isbn字段)
	    	String issn = "";
			//出版社
	    	String publisher = "";
	    	//出版日期
	    	String pub_date = "";
	    	//文献类型
	    	String source_type = "3";
	    	String down_date = "";
	    	String provider_url = "";
	    	//页码
	    	String page_info = "";
	    	//
	    	String raw_type = "";
	    	//栏目信息
	    	String column_info = "";
	    	//期刊刊名id
	    	String journal_raw_id = "";
	    	//期刊名
	    	String journal_name = "";
	    	//出版年
	    	String pub_year = "";
	    	//期
	    	String num = "";
	    	//图片地址
	    	String cover_path = "";
	    	//国家
	    	String country = "CN";
	    	//语言
	    	String language = "ZH";
	    	
	    	String rawid = "";
	    	
	    	String sub_db_id = "00031";
	    	
	    	String product = "CTGUBOOKAN";
	    	
	    	String sub_db = "QK";
	    	
	    	String provider = "BOOKAN";
	    	
	    	
	    	JsonObject issuemsg = mapField.get("bookmsg").getAsJsonObject();
	    	
	    	JsonObject datamsg = issuemsg.get("data").getAsJsonArray().get(0).getAsJsonObject();
	    	
	    	journal_name = datamsg.get("resourceName").getAsString();
	    	journal_raw_id = datamsg.get("resourceId").getAsString();
	    	pub_year =  datamsg.get("issueYear").getAsString();
	    	pub_date = datamsg.get("publish").getAsString().replace("-", "");
			if (pub_date.equals("00000000")) {
				pub_date = pub_year + "0000";
			}
	    	
	    	num  = datamsg.get("issueNo").getAsString();
	    	issn = datamsg.get("isbn").getAsString();
	    	String issueId = datamsg.get("issueId").getAsString();
	    	provider_url = "http://zq.bookan.com.cn/?t=detail&id=23579&ct=1&is="+issueId+"&rid="+journal_raw_id;
	    	
	    	JsonObject articleemsg = mapField.get("article").getAsJsonObject();
	    	JsonArray dataarray = null;
	    	try {
	    		dataarray = articleemsg.get("data").getAsJsonArray();
	    		}catch (Exception e) {
					// TODO: handle exception
	    			return ;
				}
	    	for (JsonElement data:dataarray) {
//	    		try {
	    		rawid = data.getAsJsonObject().get("id").getAsString();
//	    		}catch (Exception e) {
//					// TODO: handle exception
//	    			throw new InterruptedException("no info :"+provider_url+":"+data);
//				}
	    		column_info = data.getAsJsonObject().get("name").getAsString();
	    		page_info =	data.getAsJsonObject().get("page").getAsString();
	    		if (data.getAsJsonObject().get("sublevels").isJsonNull()) {
	    			title = column_info;
	    			column_info = "";
	    			//在此处调用xobj
	    			XXXXObject xObj = new XXXXObject();
//		    		rawid = rawid.trim();
//		    		if (rawid.equals("23790864")) {
//		    			context.getCounter("map", "18129058").increment(1);
//		    			context.getCounter("map", title).increment(1);
//		    			context.getCounter("map",issueId+"/"+journal_raw_id).increment(1);
//		    			context.getCounter("map",inputPath).increment(1);
//		    			if (inputPath.equals("/2019/20190220")) {
//		    				throw new InterruptedException("json is:"+value);
//		    			}
//		    			
//		    			//throw new InterruptedException("json is:"+value);
//		    		}

	    			xObj.data.put("rawid", rawid);
	    			xObj.data.put("title", title);
	    			xObj.data.put("issn", issn);
	    			xObj.data.put("publisher", publisher);
	    			xObj.data.put("pub_date", pub_date);
	    			xObj.data.put("source_type", source_type);
	    			xObj.data.put("batch", batch);
	    			xObj.data.put("down_date", down_date);
	    			xObj.data.put("provider_url", provider_url);
	    			xObj.data.put("page_info", page_info);
	    			xObj.data.put("raw_type", raw_type);
	    			xObj.data.put("column_info", column_info);
	    			xObj.data.put("journal_raw_id", journal_raw_id);
	    			xObj.data.put("journal_name", journal_name);
	    			xObj.data.put("pub_year", pub_year);
	    			xObj.data.put("num", num);
	    			xObj.data.put("cover_path", cover_path);
	    			xObj.data.put("country", country);
	    			xObj.data.put("language", language);
	    			xObj.data.put("sub_db_id", sub_db_id);
	    			xObj.data.put("product", product);
	    			xObj.data.put("sub_db", sub_db);
	    			xObj.data.put("provider", provider);
	    			xObj.data.put("issueId", issueId);

	    			context.getCounter("map", "count").increment(1);
    				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
	    			context.write(new Text(rawid), new BytesWritable(bytes));	
	    			
	    		}else {
	    			JsonArray array = data.getAsJsonObject().get("sublevels").getAsJsonArray();
	    			for (JsonElement dataarticle: array) {
	    				rawid = dataarticle.getAsJsonObject().get("id").getAsString();
	    				title = dataarticle.getAsJsonObject().get("name").getAsString();
	    	    		page_info =	dataarticle.getAsJsonObject().get("page").getAsString();
	    	    		//在此处调用xobj
		    			XXXXObject xObj = new XXXXObject();
//			    		rawid = rawid.trim();
//			    		if (rawid.equals("23790864")) {
//			    			context.getCounter("map", "18129058").increment(1);
//			    			context.getCounter("map", title).increment(1);
//			    			context.getCounter("map",issueId+"/"+journal_raw_id).increment(1);
//			    			context.getCounter("map",inputPath).increment(1);
//			    			if (inputPath.equals("/2019/20190220")) {
//			    				throw new InterruptedException("json is:"+value);
//			    			}
//			    			//throw new InterruptedException("json is:"+value);
//			    		}
		    			

		    			
		      			xObj.data.put("rawid", rawid);
		    			xObj.data.put("title", title);
		    			xObj.data.put("issn", issn);
		    			xObj.data.put("publisher", publisher);
		    			xObj.data.put("pub_date", pub_date);
		    			xObj.data.put("source_type", source_type);
		    			xObj.data.put("batch", batch);
		    			xObj.data.put("down_date", down_date);
		    			xObj.data.put("provider_url", provider_url);
		    			xObj.data.put("page_info", page_info);
		    			xObj.data.put("raw_type", raw_type);
		    			xObj.data.put("column_info", column_info);
		    			xObj.data.put("journal_raw_id", journal_raw_id);
		    			xObj.data.put("journal_name", journal_name);
		    			xObj.data.put("pub_year", pub_year);
		    			xObj.data.put("num", num);
		    			xObj.data.put("cover_path", cover_path);
		    			xObj.data.put("country", country);
		    			xObj.data.put("language", language);
		    			xObj.data.put("sub_db_id", sub_db_id);
		    			xObj.data.put("product", product);
		    			xObj.data.put("sub_db", sub_db);
		    			xObj.data.put("provider", provider);
		    			
		    			context.getCounter("map", "count").increment(1);
	    				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
		    			context.write(new Text(rawid), new BytesWritable(bytes));
	    			}  
	    		}
	    		
	    	}
	    }				
	}
}
