package simple.jobstream.mapreduce.site.jstor_qk;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.MapContext;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class MergeXXXXObject2Temp extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
  
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
	 
	    	if (inputPath.endsWith("/latest")) {		//累积老数据
	    		context.getCounter("map", "count latest").increment(1);
	    		xObj.data.put("NewData", "false");	//标识应该保留的数据
	    		
			}
	    	else {		//本趟新数据
	    		context.getCounter("map", "count newdata").increment(1);
	    		xObj.data.put("NewData", "true");	//本趟新数据
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
	  		
//	    	BytesWritable bOut = new BytesWritable();	//用于最后输出
//			for (BytesWritable item : values) {
//				if (item.getLength() > bOut.getLength()) {	//选最大的一个
//					bOut.set(item.getBytes(), 0, item.getLength());
//				}
//			}
//			
//			context.getCounter("reduce", "count").increment(1);			
//			
//			bOut.setCapacity(bOut.getLength()); 	//将buffer设为实际长度
//		
//			context.write(key, bOut);
	  		
//	    	BytesWritable bOut = new BytesWritable();	//用于最后输出
	    	XXXXObject outxObj = new XXXXObject();
	    	for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);	
				String NewData = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("NewData")) {
						NewData = updateItem.getValue().trim();
						break;  // 找到NewData 就不需要循环了 优化了性能
					}
				}
				if (NewData.equals("true")) {	//应该输出本组数据（中累积数据那一条）
					context.getCounter("reduce", "newcount").increment(1);	
					outxObj = xObj;
					break; //有了新的值就跳出for循环 不需要去找旧的值了
				}else {
					context.getCounter("reduce", "outcount").increment(1);	
					outxObj = xObj; //旧的值继续循环看是否有新值 直到循环完都没有就使用旧的值
				}
	    	}

    		context.getCounter("reduce", "count").increment(1);	
			byte[] outData = VipcloudUtil.SerializeObject(outxObj);			
			context.write(key, new BytesWritable(outData));
	    }
	}
}
