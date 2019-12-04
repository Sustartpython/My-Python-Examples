package simple.jobstream.mapreduce.user.walker.cnki_keyword_merge;

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

public class MergeXXXXObject extends InHdfsOutHdfsJobInfo {
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
	    	
	    	if (inputPath.endsWith("keyword_a/XXXXObject")) {		
	    		context.getCounter("map", "count keyword_a").increment(1);
			}
	    	else {		
	    		context.getCounter("map", "count keyword_c").increment(1);
			}

			context.write(key, value);
		}
	}
	
	public static class ProcessReducer extends
  			Reducer<Text, BytesWritable, Text, BytesWritable> {
	  	public void reduce(Text key, Iterable<BytesWritable> values, 
	                       Context context
	                       ) throws IOException, InterruptedException {	    	
	    	String keyword = "";
	    	String related_word = "";
	    	String similar_word = "";
	    	String alt_word = "";
	    	String summary = "";
	    	
	    	for (BytesWritable item : values) {
	    		XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("keyword")) {
						keyword = updateItem.getValue().trim();
					}
					else if (updateItem.getKey().equals("related_word")) {
						related_word = updateItem.getValue().trim();
					}
					else if (updateItem.getKey().equals("similar_word")) {
						similar_word = updateItem.getValue().trim();
					}
					else if (updateItem.getKey().equals("alt_word")) {
						alt_word = updateItem.getValue().trim();
					}
					else if (updateItem.getKey().equals("summary")) {
						summary = updateItem.getValue().trim();
					}
				}
			}
	    	
	    	if ((related_word.length() < 1) && (similar_word.length() < 1) && (alt_word.length() < 1) && (summary.length() < 1)) {
				context.getCounter("reduce", "related_similar_alt_summary null").increment(1);
				return;
			}
	    	
	    	XXXXObject xObj = new XXXXObject();
	    	xObj.data.put("id", key.toString());
	    	xObj.data.put("keyword", keyword);
	    	xObj.data.put("related_word", related_word);
	    	xObj.data.put("similar_word", similar_word);
	    	xObj.data.put("alt_word", alt_word);
	    	xObj.data.put("summary", summary);
	    	
	    	context.getCounter("reduce", "count").increment(1);		

	    	byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(key, new BytesWritable(bytes));			
	    }
	}
}
