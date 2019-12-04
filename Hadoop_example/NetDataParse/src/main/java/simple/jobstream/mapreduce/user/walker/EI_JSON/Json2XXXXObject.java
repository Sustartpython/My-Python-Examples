package simple.jobstream.mapreduce.user.walker.EI_JSON;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
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
		job.setNumReduceTasks(reduceNum);
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
	    	
	    	String line = value.toString().trim();
	    	
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + line);
			}
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			
			

			Map<String, String> mapField = gson.fromJson(line, type);		

			if (!mapField.containsKey("Title")) {
				context.getCounter("map", "no Title").increment(1);
				return;
			}

			if (!mapField.containsKey("Accession number")) {
				context.getCounter("map", "no Accession number").increment(1);
				return;
			}
			String AccessionNumber = mapField.get("Accession number");	
			if (AccessionNumber.length() < 1) {
				context.getCounter("map", "AccessionNumber length 0").increment(1);
				return;
			}

			String DOWNDate = mapField.get("DOWNDate");			
			if (DOWNDate.length() != 8) {
				context.getCounter("map", "DOWNDate.length() != 8").increment(1);
				return;
			}
			
			XXXXObject xxxxObject = new XXXXObject();
			xxxxObject.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));
			for (Map.Entry<String, String> entry : mapField.entrySet()) {
				xxxxObject.data.put(entry.getKey(), entry.getValue());
			}
			
			context.getCounter("map", "count").increment(1);			
			
			byte[] bytes = VipcloudUtil.SerializeObject(xxxxObject);
			context.write(new Text(AccessionNumber), new BytesWritable(bytes));
			
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
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//将buffer设为实际长度
		
			context.write(key, bOut);
		}
	}
}
