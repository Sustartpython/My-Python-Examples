package simple.jobstream.mapreduce.site.sd_qk;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.common.collect.ContiguousSet;
import com.process.frame.base.InHdfsOutHdfsJobInfo;

//这个MR仅仅是拷贝作用
public class XObj2XObj extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 1;

	// public static String inputHdfsPath = "/RawData/elsevier/sd_qk/latest";
	// public static String inputHdfsPath = "/RawData/elsevier/sd_qk/XXXXObject";

	public static String inputHdfsPath = "/RawData/elsevier/sd_qk/latest_temp";
	public static String outputHdfsPath = "/vipuser/walker/output/sd_qk/tmp";

	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();

		job.setJobName(jobName);

		// inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		// outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
		// job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.setNumReduceTasks(reduceNum);
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "count").increment(1);

			value.setCapacity(value.getLength()); // 将buffer设为实际长度

			if (!key.toString().startsWith("00014575699006")) {
				return;
			}
			context.getCounter("map", "out").increment(1);

			context.write(key, value); // 拷贝
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			for (BytesWritable item : values) {
				context.write(key, item);
				context.getCounter("reduce", "count").increment(1);
			}
		}
	}
}
