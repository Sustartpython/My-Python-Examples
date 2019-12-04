package simple.jobstream.mapreduce.site.cnipapatent;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class MergeXXXXObject2CoverXobj extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
		job.setJobName("cnipa."+jobName);
		
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
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
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
			
			if (inputPath.endsWith("/CoverXXXXObject4")) {		
				context.getCounter("map", "coverdate").increment(1);
				xObj.data.put("iscover", "true");	//本趟新数据
			}else {
				context.getCounter("map", "date").increment(1);
				xObj.data.put("iscover", "false");	//标识应该保留的数据
			
			}
			
			byte[] outData = VipcloudUtil.SerializeObject(xObj);
			
			context.getCounter("map", "outCount").increment(1);
			
			context.write(key, new BytesWritable(outData));
	    	
		}
	}
	
	public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	  	public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	  		
	  		XXXXObject xObjOut = new XXXXObject();
	    	boolean bOut = false;	//是否应该输出本对数据中的一条
	    	
	    	BytesWritable bOut1 = new BytesWritable();	//用于最后输出
	    	BytesWritable byte1 = new BytesWritable();	//
	    	BytesWritable byte2 = new BytesWritable();	//
	    	

			String cover_path = "";
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);				
				
				String iscover = "";
				String cover = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("iscover")) {
						iscover = updateItem.getValue().trim();
					}
					if (updateItem.getKey().equals("cover_path")) {
						cover = updateItem.getValue().trim();
					}
				}
				
				if (iscover.equals("true")) {
					cover_path = cover;
				}else {
					bOut = true;
					xObjOut = xObj;		//累积数据中的一条（这一条比新数据信息更全）
				}
				iscover = "";
			}
			
			if (bOut) {
				xObjOut.data.put("cover_path", cover_path);
				if (!cover_path.equals("")) {
					context.getCounter("reduce", "covercount").increment(1);
				}else {
					context.getCounter("reduce", "covernull").increment(1);
				}
				if (xObjOut.data.get("app_date").length() < 4){
					context.getCounter("reduce", "app_no null").increment(1);
				}
				context.getCounter("reduce", "outCount").increment(1);
				byte[] outData = VipcloudUtil.SerializeObject(xObjOut);			
				context.write(key, new BytesWritable(outData));
			}else {
				context.getCounter("reduce", "onlycover").increment(1);
			}
			
	    }
	}
}
