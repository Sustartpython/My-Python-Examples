package simple.jobstream.mapreduce.site.iopjournal;

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

//不能是“总数据-老数据”，新数据要全刷一遍。
public class GenNewData extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
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
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);

			if (inputPath.endsWith("/latest_temp")) {
				context.getCounter("map", "latest_temp").increment(1);
				xObj.data.put("NewData", "false"); // 标识应该保留的数据
			} else {
				context.getCounter("map", "new").increment(1);
				xObj.data.put("NewData", "true"); // 本趟新数据
			}

			byte[] outData = VipcloudUtil.SerializeObject(xObj);

			context.getCounter("map", "outCount").increment(1);

			context.write(key, new BytesWritable(outData));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bwOut = new BytesWritable(); // 用于最后输出

			boolean boolOut = false; // 是否应该输出本对数据中的一条
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				String NewData = "";

				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("NewData")) {
						NewData = updateItem.getValue().trim();
					}
				}

				if (NewData.equals("true")) { // 应该输出本组数据（中累积数据那一条）
					boolOut = true;
				} else {
					bwOut.set(item.getBytes(), 0, item.getLength()); // 累积数据中的一条（这一条比新数据信息更全）
				}
			}

			if (!boolOut) { // 不输出
				return;
			}

			context.getCounter("reduce", "outCount").increment(1);

			bwOut.setCapacity(bwOut.getLength()); // 将buffer设为实际长度
			context.write(key, bwOut);
		}
	}
}