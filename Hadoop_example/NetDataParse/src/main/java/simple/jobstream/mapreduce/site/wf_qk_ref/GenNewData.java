package simple.jobstream.mapreduce.site.wf_qk_ref;

import java.io.IOException;
import java.util.ArrayList;
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
		String jobName = "wf_qk_ref." + this.getClass().getSimpleName();
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

			String rawsourceid = key.toString();

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);

			if (inputPath.equals("/ref/latest_temp")) {
				context.getCounter("map", "latest_temp").increment(1);
				xObj.data.put("NewData", "false"); // 标识应该保留的数据
			} else {
				context.getCounter("map", "new").increment(1);
				xObj.data.put("NewData", "true"); // 本趟新数据
			}

			byte[] outData = VipcloudUtil.SerializeObject(xObj);

			context.getCounter("map", "outCount").increment(1);
			context.write(new Text(rawsourceid), new BytesWritable(outData));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

		private static String getLngIDByRawSourceID(String rawsourceid, int idx) {
			rawsourceid = rawsourceid.toUpperCase();
			String lngID = "Wd";
			for (int i = 0; i < rawsourceid.length(); i++) {
				lngID += String.format("%d", rawsourceid.charAt(i) + 0);
			}
			lngID = lngID + String.format("%04d", idx);

			return lngID;
		}

		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			String rawsourceid = key.toString();

			// *
			ArrayList<XXXXObject> latestOutList = new ArrayList<XXXXObject>(); // latest目录中应该输出的数据

			boolean bOut = false; // 是否应该输出本组数据
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				String NewData = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("NewData")) {
						NewData = updateItem.getValue().trim();
						break;
					}
				}

				if (NewData.equals("true")) { // 应该输出本组数据（中累积数据那一条）
					bOut = true;
				} else {
					latestOutList.add(xObj);
				}
			}

			if (!bOut) { // 不输出
				return;
			}

			int idx = 0;
			for (XXXXObject xObj : latestOutList) {
				String lngid = getLngIDByRawSourceID(rawsourceid, ++idx);

				context.getCounter("reduce", "outCount").increment(1);

				context.write(new Text(lngid), new BytesWritable(VipcloudUtil.SerializeObject(xObj)));
			}
			// */
		}
	}
}
