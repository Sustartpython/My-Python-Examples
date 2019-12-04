package simple.jobstream.mapreduce.site.wf_qk_ref;

import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
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

//导出latest数据，主要是工作是编号lngid
public class ExportLatest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 300;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
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

			context.getCounter("map", "outCount").increment(1);
			context.write(key, value);
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

		private static String getLngIDByRawSourceID(String rawsourceid, int idx) {
			rawsourceid = rawsourceid.toUpperCase();
			String lngID = "W_";
			for (int i = 0; i < rawsourceid.length(); i++) {
				lngID += String.format("%d", rawsourceid.charAt(i) + 0);
			}
			lngID = lngID + String.format("%04d", idx);

			return lngID;
		}

		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			String rawsourceid = key.toString();
			int idx = 0;
			for (BytesWritable item : values) {
				String lngid = getLngIDByRawSourceID(rawsourceid, ++idx);

				context.getCounter("reduce", "outCount").increment(1);

				context.write(new Text(lngid), item);
			}
		}
	}
}
