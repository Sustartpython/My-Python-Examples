package simple.jobstream.mapreduce.site.springer;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import com.process.frame.util.JobConfUtil;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;


public class DealLatest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;
	
	public static  String inputHdfsPath = "/RawData/springer/springerjournal/latest";
	public static  String outputHdfsPath = "/RawData/springer/springerjournal/latestClear";

	public void pre(Job job) {
		String jobName = "jigouku." + this.getClass().getSimpleName();
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

	public void SetMRInfo(Job job) {
		// job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		// System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		// job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		// System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
//		job.getConfiguration().set("io.compression.codecs",
//				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
//		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));


		job.setOutputFormatClass(TextOutputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);

//		JobConfUtil.setTaskPerMapMemory(job, 1024 * 8);
		JobConfUtil.setTaskPerReduceMemory(job, 1024 * 8);
		SequenceFileOutputFormat.setCompressOutput(job, false);
		
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath()
	{
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath()
	{
		return outputHdfsPath;
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, BytesWritable> {

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String title= "";
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().toLowerCase().equals("title")) {
					title = updateItem.getValue().trim().toLowerCase();
				}
			}
			context.getCounter("map", "总计").increment(1);

			if(title.length() >1){
				context.write(key, value);
				context.getCounter("map", "has title").increment(1);
			}

		}
	}

	public static class ProcessReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {

		public void reduce(Text key, Iterable<BytesWritable> values,
				Context context) throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reducer", "count").increment(1);
			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度
			context.write(key, bOut);
		}
	}
}