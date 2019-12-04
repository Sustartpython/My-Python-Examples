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
 * <p>Description: 提取 XXXXObject，主要供 JobNodeModel 类调用</p>  
 * @author qiuhongyang 2018年11月5日 上午10:48:34
 */
public class ExtractXXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inPathX = "";	// 提供 key 的路径
	public static String inPathY = "";	// 提供 XXXXObject 的路径
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String jobName = "ExtractXXXXObject";

	public void pre(Job job) {
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));

		inPathX = job.getConfiguration().get("inPathX");
		inPathY = job.getConfiguration().get("inPathY");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
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
			if (inPathX.endsWith(curDir)) {	// 提供 key
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("***OnlyKey***", "");
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(key, new BytesWritable(bytes));		
				
				context.getCounter("map", "count " + inPathX).increment(1);
			}
			else if (inPathY.endsWith(curDir)) {	// 提供 XXXXObject
				context.write(key, value);	// 直接输出
				context.getCounter("map", "count " + inPathY).increment(1);
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
				context.getCounter("map", "count error inPathX:" + inPathX).increment(1);
				context.getCounter("map", "count error inPathY:" + inPathY).increment(1);
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			XXXXObject xObjOut = null; // 用于最后输出
			
			int cnt = 0;
			for (BytesWritable item : values) {
				cnt += 1;
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				boolean isOnlyKey = false;
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("***OnlyKey***")) {
//						context.getCounter("reduce", "OnlyKey").increment(1);
						isOnlyKey = true;
					}
				}
				
				// 不是 OnlyKey
				if (!isOnlyKey) {
					xObjOut = xObj;
				}
			}

			// 非交集
			if (cnt < 2) {
				context.getCounter("reduce", "cnt<2").increment(1);
				return;
			} 

			// 多于两个值
			// 用于根据引文提取题录的情况
			if (cnt > 2) {				
				context.getCounter("reduce", "cnt>2").increment(1);
//				return;
			} 
			
			// cnt==2，两条记录，输出一条。
			context.getCounter("reduce", "outCount").increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(key, new BytesWritable(bytes));		
		}
	}
}
