package simple.jobstream.mapreduce.site.ciirSslibrary;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 4;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "ciirsslibrary." + this.getClass().getSimpleName();
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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(SimpleTextInputFormat.class);
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
	
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().trim();
	    	
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + line);
			}
	    	
	    	try {
	    		JsonObject jsonObj = new JsonParser().parse(line).getAsJsonObject();
				Type type = new TypeToken<Map<String,  Object>>(){}.getType();
				Gson gson = new Gson();
				
				JsonArray resultArray = jsonObj.get("data").getAsJsonObject().get("result").getAsJsonArray();
				for (JsonElement jEle : resultArray) {
					Map<String,  Object> mapField = gson.fromJson(jEle, type);
					XXXXObject xObject = new XXXXObject();
					String dxid ="";
					for (Map.Entry<String, Object> entry : mapField.entrySet()) {
						if (entry.getKey().equals("dxid")){
							dxid = entry.getValue().toString().trim();
						}
						xObject.data.put(entry.getKey().trim(),entry.getValue().toString().trim());
					}
					context.getCounter("map", "count").increment(1);			
					
					byte[] bytes = VipcloudUtil.SerializeObject(xObject);
					context.write(new Text(dxid), new BytesWritable(bytes));
				}
	    	} catch (Exception e) {
				// TODO: handle exception
				//return;
	    		System.out.println(e.toString());
			}
		}
	}
	

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values,Context context) throws IOException, InterruptedException {
			//*
			
			XXXXObject xObjOut = new XXXXObject();	//用于最后输出
			BytesWritable bwrite = new BytesWritable();
			for (BytesWritable item : values) {				
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				if (item.getLength() > bwrite.getLength()) {		//选最大一个
					bwrite = item;
					xObjOut = xObj;
				}
			}
			
			
			byte[] outData = VipcloudUtil.SerializeObject(xObjOut);			
			BytesWritable byteOut = new BytesWritable(outData);
			
			context.getCounter("reduce", "count").increment(1);
			context.getCounter("reduce", "count bytes").increment(byteOut.getLength());
			
			
			context.write(key, byteOut);
		}
	}
	//json 数据映射
	public class DataSet{

		public Data data;
		public Integer total;
		public boolean success;
		
		public class Data{
			public List<Result> result;

			public class Result{
				String author= ""; //作者
				String bookName= "";//书名
				String bookCardD= "";
				String cnFenlei= "";
				String coverurl= "";
				String d= "";
				String dataproviders= "";
				String date= "";
				String dxid= "";
				String fenlei= "";
				String introduce= "";
				String jpathD= "";
				String keyword= "";
				String page= "";
				String pdgD= "";
				String publisher= "";
				String ssid =  "";
			}
		}
	}
	
}
