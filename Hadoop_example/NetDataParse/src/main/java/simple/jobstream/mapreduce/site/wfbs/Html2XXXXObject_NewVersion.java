package simple.jobstream.mapreduce.site.wfbs;

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








import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject_NewVersion extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "wfbs.Html2XXXXObject";
		
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
				xObj.data.put("title_c",map.get("title_c"));
				xObj.data.put("Showwriter", map.get("Showwriter"));
				xObj.data.put("bsspeciality",map.get("bsspeciality"));
				xObj.data.put("bsdegree", map.get("bsdegree"));
				xObj.data.put("Showorgan", map.get("Showorgan"));
				xObj.data.put("bstutorsname", map.get("bstutorsname"));
				xObj.data.put("years", map.get("years"));
				xObj.data.put("class", map.get("class"));
				xObj.data.put("keyword_c", map.get("keyword_c"));
				xObj.data.put("remark_c", map.get("remark_c"));
				xObj.data.put("marksNumber", map.get("marksNumber"));
				xObj.data.put("doi", map.get("doi"));
				
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));		
			}else{
				return;
			}
		}
	}
	
	public static HashMap<String,String> parseHtml(String htmlText){
		HashMap<String,String> map = new HashMap<String,String>();
		String rawid = "";
		String title="";
		String remark_c = "";
		String doi="";
		String auto="";
		String major = "";
		String degree = "";
		String degreePlace = "";
		String language = "";
		String tutor = "";
		String years = "";
		String classfiID = "";
		String keyword = "";
		String pubdate = "";
		String marksNumber = "";
		if(htmlText.contains("★") && htmlText.contains("学科专业")){
			int itemLength = htmlText.split("★").length;
			if(itemLength == 2){
				rawid = htmlText.split("★")[0].toString();
				String text = htmlText.split("★")[1].toString();
				
				if(rawid.contains("log") || rawid.contains("RefPage")){
					map = null;

				}else{
					Document doc = Jsoup.parse(text.toString());
					try{
						title = doc.select("title").first().text().trim();
						Element remark = doc.select("meta[name=description]").first();
						remark_c = remark.attr("content").trim();
						Element infoDiv = doc.select("div[class=fixed-width baseinfo-feild]").first();
						Elements listItem = infoDiv.select("div[class^=row]");
						for(Element e : listItem){
							Element item = e.select("span[class=pre]").first();
							Element value=e.select("span[class=text]").first();
							Elements As = value.select("a");
							String itemText = item.text().trim();
							String valueText = value.text().trim();
							if(As.size() >1){
								
								valueText = "";
								int i = 0;
								for (Element a: As){
									valueText = valueText + a.text().trim() + ";";
								}
	
								valueText = valueText.replace(";;", ";").replace("'", "''");
								valueText = valueText.substring(0,valueText.length()-1);
						
							}
							
							if(itemText.contains("doi")){
								doi = valueText;
							}else if(itemText.contains("作者")){
								auto = valueText;
							}else if(itemText.contains("学科专业")){
								major = valueText;
							}else if(itemText.contains("授予学位")){
								degree = valueText;
							}else if(itemText.contains("学位授予单位")){
								degreePlace = valueText;
							}else if(itemText.contains("导师姓名")){
								tutor = valueText;
							}else if(itemText.contains("学位年度")){
								years = valueText;
							}else if(itemText.contains("语 种")){
								language = valueText;
							}else if(itemText.contains("分类号")){
								classfiID = valueText;
							}else if(itemText.contains("关键词")){
								keyword = valueText;
							}else if(itemText.contains("在线出版日期")){
								pubdate = valueText;
							}else if(itemText.contains("机标分类号")){
								marksNumber = valueText;
							}
								
						}
						
			
					}catch(Exception e){
						e.printStackTrace();
					}
					
					map.put("rawid", rawid);
					map.put("title_c",title);
					map.put("Showwriter", auto);
					map.put("bsspeciality",major);
					map.put("bsdegree", degree);
					map.put("Showorgan", degreePlace);
					map.put("bstutorsname", tutor);
					map.put("years", years);
					map.put("class", classfiID);
					map.put("keyword_c", keyword);
					map.put("remark_c", remark_c);
					map.put("marksNumber", marksNumber);
					map.put("doi", doi);
					
				}

				
			}else{
				map = null;
			}

		}else{
			map = null;
		
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
