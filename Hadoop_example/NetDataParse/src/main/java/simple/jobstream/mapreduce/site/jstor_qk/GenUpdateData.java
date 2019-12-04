package simple.jobstream.mapreduce.site.jstor_qk;

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

//不能是“总数据-老数据”，新数据要全刷一遍。
public class GenUpdateData extends InHdfsOutHdfsJobInfo {
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
	    	if (key.toString().length() < 1) {		//key即是收录号（AccessionNumber）
	    		context.getCounter("map", "key null").increment(1);
				return;
			}
	    	
	    	XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			
			if (inputPath.endsWith("/latest")) {		
				context.getCounter("map", "old").increment(1);
				xObj.data.put("NewData", "false");	//标识应该保留的数据
			}
			else {
				context.getCounter("map", "new").increment(1);
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
	    	
	    	XXXXObject xObjOut = null;	//用于最后输出
	    	XXXXObject newxObj = null;
	    	XXXXObject oldxObj = null;
	    	int cnt = 0;
			for (BytesWritable item : values) {
				cnt += 1;
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);				
				String NewData = "";		

				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("NewData")) {
						NewData = updateItem.getValue().trim();
						if (NewData.equals("true")) {
							newxObj = xObj;
						}else {
							oldxObj = xObj;
						}
					}
				}
			}
			if (cnt == 1) {
				if (null != newxObj) { //如果新数据在latest里没有 就输出
					xObjOut = newxObj;
					context.getCounter("reduce", "newObj").increment(1);
				}
			}else if (cnt == 2) {
				newxObj.data.put("NewData", "true");
				oldxObj.data.put("NewData", "true");
				if (!newxObj.data.equals(oldxObj.data)) {
					context.getCounter("reduce", "equalsobj").increment(1);
					System.out.println("***newobj***" + newxObj.data);
					System.out.println("***oldobj***" + oldxObj.data);
					xObjOut = newxObj;
				}
			}else if (cnt > 2) {
				throw new InterruptedException("cnt > 2");
			}
			if (xObjOut != null) {
				context.getCounter("reduce", "outCount").increment(1);
				byte[] outData = VipcloudUtil.SerializeObject(xObjOut);			
				context.write(key, new BytesWritable(outData));
			}

	    }
  }
}
