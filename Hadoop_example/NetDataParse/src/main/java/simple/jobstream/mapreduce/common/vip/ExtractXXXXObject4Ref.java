package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;
import java.util.LinkedList;
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
 * <p>Description: 提取引文 XXXXObject，主要供 JobNodeModel 类调用</p>  
 * @author qiuhongyang 2018年11月5日 上午10:48:34
 */
public class ExtractXXXXObject4Ref extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

	private static String jobName = "ExtractXXXXObject";

	public void pre(Job job) {
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));

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

		private static String curDir = ""; // 当前文件目录
		private static String inPathKey = "";	// 提供 key 的路径
		private static String inPathVal = "";	// 提供 XXXXObject 的路径

		public void setup(Context context) throws IOException, InterruptedException {
			// 当前文件目录（两级路径）
			curDir = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit()); 
			inPathKey = context.getConfiguration().get("inPathKey");
			inPathVal = context.getConfiguration().get("inPathVal");
			
			System.out.println("map setup curDir:" + curDir);
			System.out.println("map setup inPathKey:" + inPathKey);
			System.out.println("map setup inPathVal:" + inPathVal);
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (inPathKey.endsWith(curDir)) {	// 提供 key
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("***OnlyKey***", "");
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(key, new BytesWritable(bytes));		
				
				context.getCounter("map", "count " + inPathKey).increment(1);
			}
			else if (inPathVal.endsWith(curDir)) {	// 提供 XXXXObject
				context.write(key, value);	// 直接输出
				context.getCounter("map", "count " + inPathVal).increment(1);
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
				context.getCounter("map", "count error inPathKey:" + inPathKey).increment(1);
				context.getCounter("map", "count error inPathVal:" + inPathVal).increment(1);
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			boolean bout = false;	// 用于判断本组数据是否有输出
			LinkedList<XXXXObject> xobjList = new LinkedList<XXXXObject>(); // 用于最后输出

			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				
				boolean isOnlyKey = false;	
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("***OnlyKey***")) {						
						isOnlyKey = true;
						bout = true;
					}
				}
				
				if (isOnlyKey) {
//					context.getCounter("reduce", "equal ***OnlyKey***").increment(1);
				} 
				else {
//					context.getCounter("reduce", "not equal ***OnlyKey***").increment(1);
					xobjList.add(xObj);
				}
			}
			
			if (!bout) {	// 不输出没有key的数据
//				context.getCounter("reduce", "!bout").increment(1);
				return;
			}
			
			for (XXXXObject xObj : xobjList) {
				context.getCounter("reduce", "outCount").increment(1);
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(key, new BytesWritable(bytes));		
			}
		}
	}
}
