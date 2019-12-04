package simple.jobstream.mapreduce.site.wanfang_zl;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;










import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "Step1_html2xxxxobject";
		if (testRun) {
			jobName = "wfzl_" + jobName;
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
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

	    	HashMap<String, String>map = parseHtml(value.toString());
			if(map != null){
				
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("title_c", map.get("title_c"));
				xObj.data.put("maintype", map.get("maintype"));
				xObj.data.put("showwriter", map.get("showwriter"));
				xObj.data.put("showorgan", map.get("showorgan"));
				xObj.data.put("applicantaddr", map.get("applicantaddr"));
				xObj.data.put("provincecode", map.get("provincecode"));
				xObj.data.put("applicationnum", map.get("applicationnum"));
				xObj.data.put("applicationdata", map.get("applicationdata"));
				xObj.data.put("media_c", map.get("media_c"));
				xObj.data.put("opendata", map.get("opendata"));
				xObj.data.put("mainclass", map.get("mainclass"));
				xObj.data.put("classnum", map.get("classnum"));
				xObj.data.put("remark_c", map.get("remark_c"));
				xObj.data.put("sovereignty", map.get("sovereignty"));
				xObj.data.put("agents", map.get("agents"));
				xObj.data.put("agency", map.get("agency"));
				xObj.data.put("legalstatus", map.get("legalstatus"));
				
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));		
			}else{
				context.getCounter("map", "null").increment(1);
				return;
			}
		}
	}
	
	public static HashMap<String,String> parseHtml(String htmlText){
		HashMap<String,String> map = new HashMap<String,String>();
		String rawid = "";
		String title_c="";
		String maintype = "";
		String years = "";
		String showwriter = "";
		String showorgan = "";
		String applicantaddr = "";
		String provincecode = "";
		String applicationnum = "";
		String applicationdata = "";
		String media_c = "";
		String opendata = "";
		String mainclass = "";
		String classnum = "";
		String remark_c = "";
		String sovereignty = "";
		String agents = "";
		String agency = "";
		String legalstatus = "";

		JsonObject obj = new JsonParser().parse(htmlText).getAsJsonObject();
		String html = obj.get("html").getAsString();
		rawid = obj.get("filename").getAsString();
		
		Document doc = Jsoup.parse(html);
		try{
			title_c = doc.select("title").first().text().trim();
			Element remark = doc.select("div[class=baseinfo-feild abstract]").first();
			Element remarkTag = remark.select("div[class=text]").first();
			remark_c = remarkTag.text().trim();
			
			
			//-----------------------
			//Element infoDiv = doc.select("div[class=fixed-width baseinfo-feild]").first();
			Elements listItem = doc.select("div[class=row]");
			for(Element e : listItem){
				Element item = e.select("span[class=pre]").first();
				Element value=e.select("span[class=text]").first();
				String itemText = item.text().trim();
				String valueText = value.text().trim();

				
				if(itemText.contains("专利类型")){
					maintype = valueText;
				}else if(itemText.contains("申请（专利）号")){
					applicationnum = valueText;
				}else if(itemText.contains("申请日期")){
					applicationdata = valueText;
				}else if(itemText.contains("公开(公告)日")){
					opendata = valueText;
				}else if(itemText.contains("公开(公告)号：")){
					media_c = valueText;
				}else if(itemText.contains("主分类号：")){
					mainclass = valueText;
				}else if(itemText.startsWith("分类号：")){
					classnum = valueText;
				}else if(itemText.contains("申请（专利权）人：")){
					showorgan = valueText;
				}else if(itemText.contains("发明（设计）人：")){
					showwriter = valueText;
				}else if(itemText.contains("主申请人地址：")){
					applicantaddr = valueText;
				}else if(itemText.contains("专利代理机构：")){
					agency = valueText;
				}else if(itemText.contains("代理人：")){
					agents = valueText;
				}else if(itemText.contains("国别省市代码：")){
					provincecode = valueText;
				}else if(itemText.contains("主权项：")){
					sovereignty = valueText;
				}else if(itemText.contains("法律状态：")){
					legalstatus = valueText;
				}
					
			}
			

		}catch(Exception e){
			e.printStackTrace();
		}
		if (rawid.trim().length() > 0) {			
			map.put("rawid", rawid);
			map.put("title_c", title_c);
			map.put("maintype", maintype);
			map.put("showwriter", showwriter);
			map.put("showorgan", showorgan);
			map.put("applicantaddr", applicantaddr);
			map.put("provincecode", provincecode);
			map.put("applicationnum", applicationnum);
			map.put("applicationdata", applicationdata);
			map.put("media_c", media_c);
			map.put("opendata", opendata);
			map.put("mainclass", mainclass);
			map.put("classnum", classnum);
			map.put("remark_c", remark_c);
			map.put("sovereignty", sovereignty);
			map.put("agents", agents);
			map.put("agency", agency);
			map.put("legalstatus", legalstatus);
			
			return map;
		}
		return map;
			
		
		
		
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
