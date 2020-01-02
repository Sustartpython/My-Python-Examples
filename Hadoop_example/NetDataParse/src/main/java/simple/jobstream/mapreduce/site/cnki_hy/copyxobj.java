package simple.jobstream.mapreduce.site.cnki_hy;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.nodes.Element;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//这个MR仅仅是拷贝作用
public class copyxobj extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
  
	public void pre(Job job)
	{
		String jobName = "Old_to_New_latest";
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
		job.setMapperClass(ProcessMapper.class);
		//job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    //job.setOutputFormatClass(TextOutputFormat.class);
	    
	    
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
	
	public static class ProcessMapper extends 
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
	    public void map(Text key, BytesWritable value, Context context
	                    ) throws IOException, InterruptedException {		
	    	
			XXXXObject xObj_old = new XXXXObject();
			XXXXObject xObj_new = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj_old);
			for (Map.Entry<String, String> updateItem : xObj_old.data.entrySet()) {
				
				xObj_new.data.put(updateItem.getKey().toLowerCase(),updateItem.getValue());
			}
			//生成新数据
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj_new);
			context.write(key, new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);
					
		}
	}
	
  public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	    public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {
	    	
	    	context.getCounter("reduce", "count").increment(1);
	    }
  }
}