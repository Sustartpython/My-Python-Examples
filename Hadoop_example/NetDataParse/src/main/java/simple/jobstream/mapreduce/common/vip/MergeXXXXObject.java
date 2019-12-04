package simple.jobstream.mapreduce.common.vip;

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

/**
 * <p>
 * Description: 合并 XXXXObject，主要供 JobNodeModel 类调用
 * </p>
 * 
 * @author qiuhongyang 2018年11月5日 上午10:47:53
 */
public class MergeXXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inPathX = "";
	public static String inPathY = "";
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String jobName = "MergeXXXXObject";

	public void pre(Job job) {
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));

		inPathX = job.getConfiguration().get("inPathX");
		inPathY = job.getConfiguration().get("inPathY");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath"); // 基类需要
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");

		jobName = job.getConfiguration().get("jobName");
		job.setJobName(jobName);
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
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

		System.out.println("******reduceNum*******" + reduceNum);
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

		public String curDir = ""; // 当前文件目录

		public void setup(Context context) throws IOException, InterruptedException {
			// 当前文件目录（两级路径）
			curDir = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit());

			inPathX = context.getConfiguration().get("inPathX");
			inPathY = context.getConfiguration().get("inPathY");

			System.out.println("map setup curDir:" + curDir);
			System.out.println("map setup inPathX:" + inPathX);
			System.out.println("map setup inPathY:" + inPathY);
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (inPathX.endsWith(curDir)) {
				context.getCounter("map", "count " + inPathX).increment(1);
			} else if (inPathY.endsWith(curDir)) {
				context.getCounter("map", "count " + inPathY).increment(1);
			} else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
				context.getCounter("map", "count error inPathX:" + inPathX).increment(1);
				context.getCounter("map", "count error inPathY:" + inPathY).increment(1);
			}

			context.write(key, value);
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			/**
			 * 主记录挑选方法：优先取下载日期新值；否则取解析时间（batch）新值。
			 */

			XXXXObject xObjMajor = null; // 主记录，用于最后输出
			XXXXObject xObjMinor = null; // 辅记录
			XXXXObject xObj1 = null;
			XXXXObject xObj2 = null;
			String down_date1 = ""; // 下载日期
			String down_date2 = ""; // 下载日期
			String batch1 = ""; // 解析时间
			String batch2 = ""; // 解析时间

			int cnt = 0;
			for (BytesWritable item : values) {
				cnt += 1;

				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				String down_date = "";
				String batch = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("down_date")) {
						down_date = updateItem.getValue().trim();
					} else if (updateItem.getKey().equals("batch")) {
						batch = updateItem.getValue().trim();
					}
				}

				if (1 == cnt) {
					xObj1 = xObj;
					down_date1 = down_date;
					batch1 = batch;
				} else if (2 == cnt) {
					xObj2 = xObj;
					down_date2 = down_date;
					batch2 = batch;
				}
			}

			if (cnt < 2) { // 一个值，直接输出
				context.getCounter("reduce", "cnt<2").increment(1);
				xObjMajor = xObj1;
			} else if (cnt > 2) { // 多于两个值，有问题
				context.getCounter("reduce", "error: cnt>2").increment(1);
				return;
			} else { // cnt==2
				context.getCounter("reduce", "cnt==2").increment(1);

				// 取下载新值
				if (down_date1.compareTo(down_date2) > 0) {
					xObjMajor = xObj1;
					xObjMinor = xObj2;
				} else if (down_date1.compareTo(down_date2) < 0) {
					xObjMajor = xObj2;
					xObjMinor = xObj1;
				} else { // 下载时间相同时，取解析新值
					if (batch1.compareTo(batch2) > 0) {
						xObjMajor = xObj1;
						xObjMinor = xObj2;
					} else {
						xObjMajor = xObj2;
						xObjMinor = xObj1;
					}
				}

				// 上面选出了主辅，下面进行合并
				for (Map.Entry<String, String> entryMinor : xObjMinor.data.entrySet()) {
					String keyMinor = entryMinor.getKey();
					String valMinor = entryMinor.getValue().trim();

					// 如果辅记录字段为空，跳过
					if (valMinor.length() < 1) {
						continue;
					}

					// 主记录不存在该字段或该字段内容为空时，将辅记录字段补充进去
					if ((!xObjMajor.data.containsKey(keyMinor)) || (xObjMajor.data.get(keyMinor).trim().length() < 1)) {
						xObjMajor.data.put(keyMinor, valMinor);
					}
				}
			}

			context.getCounter("reduce", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjMajor);
			context.write(key, new BytesWritable(bytes));
		}
	}
}
