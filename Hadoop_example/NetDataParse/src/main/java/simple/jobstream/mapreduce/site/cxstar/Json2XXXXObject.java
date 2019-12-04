package simple.jobstream.mapreduce.site.cxstar;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
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

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {		
		
		
		public void setup(Context context) throws IOException,
			InterruptedException {
				
		}
		

		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String ruid = mapJson.get("ruid").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);			
			
			String creator ="";
            String title = "";
            String identifier_pisbn = "";
            String date_created="";
            String subject="";
            String subject_clc="";
            String provider_subject="";
            String description="";
            String publisher="";
            String page="";
            String title_series="";
            Element titleTag = doc.select("div.detail-title > span").first();
            if (titleTag !=null) {
            	title = titleTag.text();
			}
            Element publisherTag = doc.select("a[class*=detail-publink]").first();
            if (publisherTag !=null) {
            	publisher = publisherTag.text();
			}
            Element descriptionTag = doc.select("div.detail-abstract > div").first();
            if (descriptionTag !=null) {
            	description = descriptionTag.text();
			}            
            for (Element ele: doc.select("div[class*=detail-pubdate]")) {
				String line = ele.text().trim();
				if (line.startsWith("出版时间：")) {
					date_created = line.substring("出版时间：".length());
				}
				else if (line.startsWith("页码：")) {
					page = line.substring("页码：".length());
				}
            }
            for (Element ele: doc.select("div.detail-fieldinfo > ul > li")) {
				String line = ele.text().trim();
				if (line.startsWith("作者：")) {
					creator = line.substring("作者：".length());
				}
				else if (line.startsWith("ISBN：")) {
					identifier_pisbn = line.substring("ISBN：".length());
				}
				else if (line.startsWith("主题：")) {
					subject = line.substring("主题：".length());
				}
				else if (line.startsWith("丛编：")) {
					title_series = line.substring("丛编：".length());
				}
				else if (line.startsWith("中图法分类号：")) {
					subject_clc = line.substring("中图法分类号：".length());
				}
				else if (line.startsWith("【学科分类】")) {
					provider_subject = line.substring("【学科分类】".length());
				}
            }
            for (Element ele: doc.select("span[id*=treestow]")) {
            	String line = ele.text().trim();
            	provider_subject = provider_subject + ";" + line;
            }
            
            if (!identifier_pisbn.equals("")) {
            	identifier_pisbn = identifier_pisbn.replace("-", "");
			}
            if (!date_created.equals("")) {
            	date_created = date_created.replace(".", "") + "00";
			}
            if (!provider_subject.equals("")) {
            	provider_subject = provider_subject.replace(" ", "");
			}
            
            
           
			
            

			XXXXObject xObj = new XXXXObject();


			xObj.data.put("title",title);	
			xObj.data.put("creator", creator);
			xObj.data.put("publisher", publisher);
			xObj.data.put("date_created", date_created);
			xObj.data.put("identifier_pisbn", identifier_pisbn);
			xObj.data.put("subject", subject);
			xObj.data.put("title_series", title_series);
			xObj.data.put("subject_clc", subject_clc);
			xObj.data.put("provider_subject", provider_subject);
			xObj.data.put("page", page);
			xObj.data.put("description", description);
			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(ruid), new BytesWritable(bytes));
            
	    }

	}
	//继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	//输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
			/*
			Text out =  new Text()
			for (BytesWritable val:values){
				VipcloudUtil.DeserializeObject(val.getBytes(), out);
				context.write(key,out);
			}
		//context.getCounter("reduce", "count").increment(1);*/
			BytesWritable bOut = new BytesWritable();	
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	
		
			context.write(key, bOut);
		
			
		}
	}
}