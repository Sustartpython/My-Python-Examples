package simple.jobstream.mapreduce.site.ei_zt;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//这个MR仅仅是拷贝作用
public class Temp2LatestGetSource extends InHdfsOutHdfsJobInfo {
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
		
	    public void map(Text key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {	
	    	if (key.toString().length() < 1) {		//key即是收录号（AccessionNumber）
	    		context.getCounter("map", "key null").increment(1);
				return;
			}
	    	
	    	XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			String DocumentType = "";
			String source = "";
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("Document type")) {
					DocumentType = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("Source")) {
					source = updateItem.getValue().trim();
				}
			}
	    	
			if (DocumentType.equals("Conference article (CA)") || DocumentType.equals("Conference proceeding (CP)"))
			{ ////会议
				context.getCounter("map", "eiconference_count").increment(1);
			}
			else if (DocumentType.equals("Dissertation (DS)"))
			{
				//学位
				context.getCounter("map", "eithesis_count").increment(1);
			}else {
				context.getCounter("map", "eijournal_count").increment(1);
				context.getCounter("map", "count").increment(1);
		    	
				context.write(new Text(source), value);		//拷贝
			}
			
	    	
	    	
//	    	String AccessionNumber = "";
//	    	String rawid="";
//	    	String rawidvalue="";
//	    	
//	    	XXXXObject xObj = new XXXXObject();
//			byte[] bs = new byte[value.getLength()];  
//			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
//			VipcloudUtil.DeserializeObject(bs, xObj);			
//			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
//				if (updateItem.getKey().equals("Accession number")) {
//					AccessionNumber = updateItem.getValue().trim();
//				}				
//				else if (updateItem.getKey().equals("rawid")) {
//					rawidvalue = updateItem.getValue().trim();
//				}
//			}
//			
//			if ((AccessionNumber.length() < 1) || (rawidvalue.length() < 1)) {
//				context.getCounter("map", "null").increment(1);
//				return;
//			}
//
//			context.getCounter("map", "notnull").increment(1);
//			context.write(key, value);
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
