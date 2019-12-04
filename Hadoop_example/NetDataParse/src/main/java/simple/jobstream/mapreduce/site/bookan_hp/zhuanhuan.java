package simple.jobstream.mapreduce.site.bookan_hp;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

public class zhuanhuan extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
	public static String batch = "";
	
	public void pre(Job job)
	{
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		batch = job.getConfiguration().get("batch");
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
		job.setMapperClass(ProcessMapper.class);
//		job.setReducerClass(UniqXXXXObjectReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    
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
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		public String inputPath = "";
		
		public void setup(Context context) throws IOException, InterruptedException
		{
			
			inputPath = VipcloudUtil.GetInputPath((FileSplit)context.getInputSplit());	//两级路径
		}
		
	    public void map(Text key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			String pub_date="";
			String pub_year = "";
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				}
			}
			
			if (pub_date.equals("00000000")) {
				pub_date = pub_year + "0000";
			}
			
			xObj.data.put("pub_date", pub_date);
			
			
			byte[] outData = VipcloudUtil.SerializeObject(xObj);
			
			context.getCounter("map", "outCount").increment(1);
			
			context.write(key, new BytesWritable(outData));
	    	
		}
	}
	

}
