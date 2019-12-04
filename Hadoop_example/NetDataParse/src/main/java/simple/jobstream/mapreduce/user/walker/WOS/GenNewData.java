package simple.jobstream.mapreduce.user.walker.WOS;

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
public class GenNewData extends InHdfsOutHdfsJobInfo {
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		
		
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
	    	
	    	XXXXObject bxObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), bxObj);

			
			String UT = "";
			String LIBName = "";		//SCI;SSCI;AHCI;ISTP;ISSHP;ESCI;CCR;IC

			
			for (Map.Entry<String, String> updateItem : bxObj.data.entrySet()) {
				if (updateItem.getKey().equals("UT")) {
					UT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("LIBName")) {
					LIBName = updateItem.getValue().trim();
				}
			}
			
			
			if (!inputPath.endsWith("/latest_temp")) {		//本趟新数据
				bxObj.data.put("LIBName", "NewData");	//标识应该保留的数据（合理利用原有字段）
				
				context.getCounter("map", "new").increment(1);
			}
			else {
				context.getCounter("map", "latest_temp").increment(1);
			}
			
			
			byte[] outData = VipcloudUtil.SerializeObject(bxObj);

			context.getCounter("map", "count").increment(1);
			context.write(new Text(UT), new BytesWritable(outData));
		}
	}
	
  public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	    public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	
	    	XXXXObject bxObjOut = new XXXXObject();	//用于最后输出

	    	boolean bOut = false;	//是否应该输出本条数据
	    	int cnt = 0;
			for (BytesWritable item : values) {
				cnt += 1;
				
				XXXXObject bxObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), bxObj);				
				
				String LIBName = "";		//SCI;SSCI;AHCI;ISTP;ISSHP;CCR;IC

				for (Map.Entry<String, String> updateItem : bxObj.data.entrySet()) {
					if (updateItem.getKey().equals("LIBName")) {
						LIBName = updateItem.getValue().trim();
					}
				}
				
				if (LIBName.equals("NewData")) {	//应该输出本组数据（中累积数据那一条）
					bOut = true;
				}
				else {
					bxObjOut = bxObj;		//累积数据中的一条（这一条比新数据信息更全）
				}
			}
			
			if (!bOut) {	//不输出
				return;
			}
			
			if (cnt > 2) {
				//说明新数据中ID相同的数据，除了lib不同还有其他不同
				context.getCounter("reduce", "unusual").increment(1);		
			}
			
			context.getCounter("reduce", "outCount").increment(1);
			
			byte[] outData = VipcloudUtil.SerializeObject(bxObjOut);			
			context.write(key, new BytesWritable(outData));
	    }
  }
}
