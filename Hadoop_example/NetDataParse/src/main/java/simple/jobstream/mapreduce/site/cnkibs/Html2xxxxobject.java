package simple.jobstream.mapreduce.site.cnkibs;

import java.io.IOException;
import java.lang.reflect.Type;
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
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "cnkithesis.Html2XXXXObject";
		
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
			HashMap<String,String> map = new HashMap<String,String>();
			if(htmlText.split("★").length == 2){
				String filename = htmlText.split("★")[0];
				String dbcode = "";
				String rawid = filename;
				String title_c = "";
				String Showwriter = "";
				String showwriterInfo = "";
				String bsspeciality = "";
				String bsdegree = "";
				String Showorgan = "";
				String bstutorsname = "";
				String years = "";
				String classFid = "";
				String keyword_c = "";
				String remark_c = "";
				String tempString = "";
				String description_fund = "";
				Document doc = Jsoup.parse(htmlText);

				
				//老版本解析代码
				Element titleElement = doc.select("span[id = chTitle]").first();
				Element showwriterElement = doc.select("p:contains(【作者】)").first();
				Element authorInforElement = doc.select("p:contains(【作者基本信息】)").first();
				//Element showorganElement = doc.select("div[class = orgn]").first();
				Element remark_cElement = doc.select("p:contains(摘要)").first();
				Element keyword_cElement = doc.select("span[id = ChDivKeyWord").first();
				Element bstutorsnameElement = doc.select("p:contains(导师)").first();
				Element classFidElement = doc.select("li:contains(【分类号】)").first();
				Element description_fundElement = doc.select("div[class = keywords]:contains(【基金】").first();
				
				if (titleElement != null){
					title_c = titleElement.text();
				}
				
				if (showwriterElement != null){
					Showwriter = showwriterElement.text().replace("【作者】 ", "").replace("；", ";").trim();
					if(Showwriter.endsWith(";")){
						Showwriter = Showwriter.substring(0,Showwriter.length()-1);
					}
				
						
				}

				if (remark_cElement != null){
					remark_c = remark_cElement.text().replace("【摘要】", "").trim();
				}
				if (keyword_cElement != null){
					keyword_c = keyword_cElement.text().replace("；", ";").trim();
					if(keyword_c.endsWith(";")){
						keyword_c = keyword_c.substring(0,keyword_c.length()-1);
					}
				}
				if(bstutorsnameElement != null){
					bstutorsname = bstutorsnameElement.text().replace("【导师】", "").replace("；", ";").trim();
					if(bstutorsname.endsWith(";")){
						bstutorsname = bstutorsname.substring(0,bstutorsname.length()-1);
					}
				}
				if(classFidElement != null){
					classFid = classFidElement.text().replace("【分类号】", "");
					
				}
				if(authorInforElement != null){
					tempString = authorInforElement.text().trim().replace("【作者基本信息】 ", "");
					String [] tempStringarray = tempString.split("，");
				
				
					if (tempStringarray.length == 4){
						Showorgan = tempStringarray[0].trim();
						bsspeciality = tempStringarray[1].trim();
						years = tempStringarray[2].trim();
						bsdegree = tempStringarray[3].trim();
					}
					
					if(bsdegree.equals("博士")){
						dbcode = "CDFD";
					}else if(bsdegree.equals("硕士")){
						dbcode = "CMFD";
					}
					
				}
				if(description_fundElement != null){
					description_fund = description_fundElement.text().replace("【基金】", "").trim();
				}else{
					
				}
				
				
				
				map.put("dbcode",dbcode.trim());
				map.put("filename", filename.trim());
				map.put("rawid", rawid.trim());
				map.put("title",title_c.trim());
				map.put("creator", Showwriter.trim());
				map.put("creator_descipline",bsspeciality.trim());
				map.put("creator_degree", bsdegree.trim());
				map.put("creator_institution", Showorgan.trim());
				map.put("contributor", bstutorsname.trim());
				map.put("date", years.trim());
				map.put("subject_clc", classFid.trim());
				map.put("subject", keyword_c.trim());
				map.put("description", remark_c.trim());
				map.put("description_fund", description_fund.trim());
			}else{
				map = null;
			}


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
	    
			if(map.get("title").length() <1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
	    	if(map != null){
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("dbcode", map.get("dbcode"));
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("title", map.get("title"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("creator_degree", map.get("creator_degree"));
				xObj.data.put("creator_descipline", map.get("creator_descipline"));
				xObj.data.put("creator_institution", map.get("creator_institution"));
				xObj.data.put("contributor", map.get("contributor"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("subject_clc", map.get("subject_clc"));
				xObj.data.put("subject", map.get("subject"));
				xObj.data.put("date", map.get("date"));
				xObj.data.put("description_fund", map.get("description_fund"));
				
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));	
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
