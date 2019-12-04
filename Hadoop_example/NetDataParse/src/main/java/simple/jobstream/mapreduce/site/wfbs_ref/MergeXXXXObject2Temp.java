package simple.jobstream.mapreduce.site.wfbs_ref;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
//import org.apache.hadoop.mapred.analysejobhistory_jsp;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class MergeXXXXObject2Temp extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "wf_bs_ref." + this.getClass().getSimpleName();

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
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

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

		public String inputPath = "";

		public void setup(Context context) throws IOException, InterruptedException {

			inputPath = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit()); // 两级路径
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (inputPath.equals("/ref/latest")) { // 累积老数据
				context.getCounter("map", "count latest").increment(1);
			} else { // 本趟新数据
				context.getCounter("map", "count newdata").increment(1);
			}

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
			Set<String> refertextSet = new HashSet<String>();
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				String refertext = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("refertext")) {
						refertext = updateItem.getValue().trim();
						break;
					}
				}

				if (refertextSet.contains(refertext)) { // 排除重复引文
					context.getCounter("reduce", "repeat ref").increment(1);
					continue;
				}

				refertextSet.add(refertext);

				context.getCounter("reduce", "count").increment(1);

				// String lngid = getLngIDByRawSourceID(rawsourceid, ++idx);
				context.write(key, item);
			}
		}
	}
}
