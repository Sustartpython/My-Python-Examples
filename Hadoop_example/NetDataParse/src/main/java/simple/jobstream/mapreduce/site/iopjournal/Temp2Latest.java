package simple.jobstream.mapreduce.site.iopjournal;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;

//这个MR仅仅是拷贝作用
public class Temp2Latest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	
  
	public void pre(Job job)
	{
		String jobName = "iopjournal." + this.getClass().getSimpleName();
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
		//job.setReducerClass(ProcessReducer.class);
		
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
	    	context.getCounter("map", "count").increment(1);
	    	
	    	value.setCapacity(value.getLength()); 	//将buffer设为实际长度
	    	
			context.write(key, value);		//拷贝
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
