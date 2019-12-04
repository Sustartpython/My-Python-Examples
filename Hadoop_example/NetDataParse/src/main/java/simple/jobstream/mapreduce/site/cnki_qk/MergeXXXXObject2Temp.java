package simple.jobstream.mapreduce.site.cnki_qk;

import java.io.IOException;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
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
	private static int reduceNum = 200;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "cnki_qk." + this.getClass().getSimpleName();
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
			if (inputPath.endsWith("/latest")) { // 累积老数据
				context.getCounter("map", "count latest").increment(1);
			} else { // 本趟新数据
				context.getCounter("map", "count newdata").increment(1);
			}

			context.write(key, value);
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			/**
			 * 如果大小差异小于10%，取新值；否则，取大值
			 */

			float diffPercent = 0.1f; // 差距百分比

			BytesWritable bOut = new BytesWritable(); // 用于最后输出
			BytesWritable byte1 = new BytesWritable(); //
			BytesWritable byte2 = new BytesWritable(); //
			String time1 = "";
			String time2 = "";
			float len1 = 0;
			float len2 = 0;

			int cnt = 0;
			for (BytesWritable item : values) {
				cnt += 1;

				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				String parse_time = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("parse_time")) {
						parse_time = updateItem.getValue().trim();
					}
				}

				if (1 == cnt) {
					byte1.set(item.getBytes(), 0, item.getLength());
					time1 = parse_time;
					len1 = byte1.getLength();
				} else if (2 == cnt) {
					byte2.set(item.getBytes(), 0, item.getLength());
					time2 = parse_time;
					len2 = byte2.getLength();
				}
			}

			if (cnt < 2) {
				context.getCounter("reduce", "cnt<2").increment(1);
				bOut.set(byte1.getBytes(), 0, byte1.getLength());
			} else if (cnt > 2) {
				// 多于两个值，有问题
				context.getCounter("reduce", "cnt>2").increment(1);
				return;
			} else { // cnt==2
				// 如果差异小于 diffPercent，取新值
				context.getCounter("reduce", "cnt==2").increment(1);
				if (Math.abs(len1 - len2) < (diffPercent * (len1 > len2 ? len1 : len2))) {
					if (time1.compareTo(time2) > 0) {
						bOut.set(byte1.getBytes(), 0, byte1.getLength());
					} else {
						bOut.set(byte2.getBytes(), 0, byte2.getLength());
					}
				} else { // 如果差异大于 diffPercent，取大值
					if (len1 > len2) {
						bOut.set(byte1.getBytes(), 0, byte1.getLength());
					} else {
						bOut.set(byte2.getBytes(), 0, byte2.getLength());
					}
				}
			}

			context.getCounter("reduce", "count").increment(1);
			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度
			context.write(key, bOut);
		}
	}
}
