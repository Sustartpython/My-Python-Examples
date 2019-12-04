package simple.jobstream.mapreduce.site.WOS;

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
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

public class MergeXXXXObject2Temp extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 400;
	
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
	    
	    JobConfUtil.setTaskPerReduceMemory(job, 5120);
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
	    	
	    	if (inputPath.endsWith("/latest")) {		//累积老数据
	    		context.getCounter("map", "count latest").increment(1);
			}
	    	else {		//本趟新数据
	    		context.getCounter("map", "count newdata").increment(1);
			}

			context.write(key, value);
		}
	}
	
	public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	  	public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	/**
	    	 * 如果大小差异大于指定百分比，取大值；否则，下载日期的新值；否则，取解析时间的新值
	    	 */
	    	
	    	float diffPercent = 0.3f;		//差距百分比
	    	
	    	BytesWritable bwOut = new BytesWritable();	//用于最后输出
	    	BytesWritable bw1 = new BytesWritable();	//
	    	BytesWritable bw2 = new BytesWritable();	//
	    	String down_date1 = "";			//下载日期
	    	String down_date2 = "";			//下载日期
	    	String parse_time1 = "";		//解析时间
	    	String parse_time2 = "";		//解析时间
	    	float len1 = 0;
	    	float len2 = 0;
	    	
	    	int cnt = 0;
	    	for (BytesWritable item : values) {
	    		cnt += 1;
	    		
	    		XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);		
				String down_date = "";
				String parse_time = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("DOWNDate")) {
						down_date = updateItem.getValue().trim();		
					}
					else if (updateItem.getKey().equals("parse_time")) {
						parse_time = updateItem.getValue().trim();
					}
				}
	    		
	    		if (1 == cnt) {
	    			bw1.set(item.getBytes(), 0, item.getLength());
	    			len1 = bw1.getLength();
	    			down_date1 = down_date;
	    			parse_time1 = parse_time;
				}
	    		else if (2 == cnt) {
	    			bw2.set(item.getBytes(), 0, item.getLength());
	    			len2 = bw2.getLength();
	    			down_date2 = down_date;
	    			parse_time2 = parse_time;
				}
			}

	    	if (cnt < 2) {
	    		context.getCounter("reduce", "cnt<2").increment(1);	
	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());
			}
	    	else if (cnt > 2) {
	    		//多于两个值，有问题
	    		context.getCounter("reduce", "cnt>2").increment(1);	
	    		return;
			}
	    	else {		//cnt==2
	    		context.getCounter("reduce", "cnt==2").increment(1);
	    		
    			//如果差异大于 diffPercent，取大值
//	    		if (Math.abs(len1 - len2) 
//	    				> (diffPercent * Math.max(len1, len2))) {
//	    			if (len1 > len2) {
//						bwOut.set(bw1.getBytes(), 0, bw1.getLength());
//					}
//					else {
//						bwOut.set(bw2.getBytes(), 0, bw2.getLength());
//					}
//				}
					
				//取下载新值
				if (down_date1.compareTo(down_date2) > 0) {
    	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());
    			}
    	    	else if (down_date1.compareTo(down_date2) < 0) {
    	    		bwOut.set(bw2.getBytes(), 0, bw2.getLength());
    			}
				else {		//下载时间相同时，取解析新值
					if (parse_time1.compareTo(parse_time2) > 0) {
	    	    		bwOut.set(bw1.getBytes(), 0, bw1.getLength());
	    			}
					else {
						bwOut.set(bw2.getBytes(), 0, bw2.getLength());
					}
				}

			}
	    	
	    	context.getCounter("reduce", "count").increment(1);		
	    	bwOut.setCapacity(bwOut.getLength()); 	//将buffer设为实际长度
			context.write(key, bwOut);
	    }
	}
}
