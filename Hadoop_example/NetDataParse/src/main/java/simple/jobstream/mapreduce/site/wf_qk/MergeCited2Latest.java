package simple.jobstream.mapreduce.site.wf_qk;

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

// 合并引文量、被引量
public class MergeCited2Latest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 200;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "wf_qk." + this.getClass().getSimpleName();

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
		job.getConfiguration().setFloat(
				"mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out
				.println("******mapred.reduce.slowstart.completed.maps*******"
						+ job.getConfiguration().get(
								"mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration()
				.set("io.compression.codecs",
						"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******"
				+ job.getConfiguration().get("io.compression.codecs"));
		
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

			if (inputPath.endsWith("/detail/latest_temp")) {
				context.getCounter("map", "count meta").increment(1);
				xObj.data.put("_type", "meta"); // 主题录
			} else {
				context.getCounter("map", "count cited").increment(1);
				xObj.data.put("_type", "cited"); // 引文量、被引量
			}

			byte[] outData = VipcloudUtil.SerializeObject(xObj);

			context.getCounter("map", "outCount").increment(1);

			context.write(key, new BytesWritable(outData));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {			
			XXXXObject xObjMeta = null;		//主题录
			XXXXObject xObjCited = null;		//引文量、被引量
			
			for (BytesWritable item : values) {

				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				String _type = "";

				for (Map.Entry<String, String> fieldItem : xObj.data.entrySet()) {
					if (fieldItem.getKey().equals("_type")) {
						_type = fieldItem.getValue().trim();
					}
				}

				if (_type.equals("meta")) {  
					xObjMeta = xObj;
				} else if (_type.equals("cited")) {
					xObjCited = xObj;  
				}
			}
			
			// 无主题录，直接返回
			if (xObjMeta == null) {		
				return;
			}
			
			// 引文量、被引量有值
			if (xObjCited != null) {
				for (Map.Entry<String, String> fieldItem : xObjCited.data.entrySet()) {
					xObjMeta.data.put(fieldItem.getKey(), fieldItem.getValue().trim());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObjMeta);
			context.write(key, new BytesWritable(bytes));
		}
	}
}
