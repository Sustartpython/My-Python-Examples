package simple.jobstream.mapreduce.site.aps;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
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
public class Json2Txt extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 1;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "Json2Txt";
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
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

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
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {		
		
		
		public void setup(Context context) throws IOException,
			InterruptedException {
				
		}
		
		public static boolean isNumeric(String str){
		    Pattern pattern = Pattern.compile("[0-9]+");
		    return pattern.matcher(str).matches();   
		}
		

		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	
	    	String url = mapJson.get("url").trim();
	    	String vol = mapJson.get("vol").trim();
	    	String issue = mapJson.get("issue").trim();
	    	String html  = mapJson.get("detail").trim();
	    	
			Document doc = Jsoup.parse(html);
			
			String creator ="";
            String title = "";
            String date_created = "";
            String provider_subject="";
            String description="";
            String doi="";
            String beginpage="";
            String endpage="";
            String insitution="";
            String source="";
            String subject="";
            String eissn="";
            String pissn="";

           

            Element divTag = doc.select("section.article.authors.open > div.content").first();
            if (divTag !=null) {
            	return;
			}

            
            

			

			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			url = url.trim();

			context.write(new Text(url+"\n"), NullWritable.get());
            
	    }

	}
	//继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	//输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context) throws IOException, InterruptedException {
			/*
			Text out =  new Text()
			for (BytesWritable val:values){
				VipcloudUtil.DeserializeObject(val.getBytes(), out);
				context.write(key,out);
			}
		//context.getCounter("reduce", "count").increment(1);*/

			
			context.getCounter("reduce", "count").increment(1);			
			

		
			context.write(key, NullWritable.get());
		
			
		}
	}
}