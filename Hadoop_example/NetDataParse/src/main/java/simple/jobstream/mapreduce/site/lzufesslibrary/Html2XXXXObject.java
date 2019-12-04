package simple.jobstream.mapreduce.site.lzufesslibrary;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
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
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "lzufesslibrarybook." + this.getClass().getSimpleName();
//		String jobName = "Html2XXXXObject";
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
		
		static int cnt = 0;	
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> mapJson = gson.fromJson(value.toString(), type);
	    	String html  = mapJson.get("html").trim();
	    	String BID = mapJson.get("BID").trim();
	    	
			Document doc = Jsoup.parse(html);
			
            Elements infos = doc.select("div.gai_box");
            if (infos.isEmpty()) {
            	context.getCounter("map", "htmlerror").increment(1);
				return;
			}
            System.out.println(BID);
            for (Element info : infos) {
                String creator ="";
                String title = "";
                
                String press_year="";
                String subject="";
                String provider_subject="";
                String description="";

                Element titleTag =info.select(".biaoti > a").first();
                if (titleTag == null){                	
                	continue;
				}
                title = titleTag.text();
                Elements pTags = info.select(".box_r_r > dl > p");
                for(Element pTag:pTags){
                    //System.out.println(pTag.text());
                    if (pTag.text().startsWith("作者")){
                        //pTag.text().contains()
                    	creator = pTag.text();
                    }
                    else if (pTag.text().startsWith("出版日期")){
                        press_year = pTag.text();
                    }
                    else if (pTag.text().startsWith("主题词")){
                        subject = pTag.text();
                    }
                    else if (pTag.text().startsWith("分类：")){
                        provider_subject = pTag.text();
                    }
                    else if (pTag.text().startsWith("图书简介")){
                    	description = pTag.text();
                    }
                }

				XXXXObject xObj = new XXXXObject();
				xObj.data.put("title",title);	
				xObj.data.put("creator", creator);
				xObj.data.put("subject", subject);
				xObj.data.put("press_year", press_year);
				xObj.data.put("BID", BID);
				xObj.data.put("provider_subject", provider_subject);
				xObj.data.put("description", description);
				

				//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
				context.getCounter("map", "count").increment(1);
				//context.write(new Text(textString), NullWritable.get());
				
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(BID), new BytesWritable(bytes));
            }
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