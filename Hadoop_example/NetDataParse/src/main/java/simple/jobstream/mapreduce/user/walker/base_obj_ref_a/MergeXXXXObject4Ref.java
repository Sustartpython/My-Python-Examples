package simple.jobstream.mapreduce.user.walker.base_obj_ref_a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.List;

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
 * <p>Description: 合并 XXXXObject，base_obj_ref_a 专用</p>  
 * @author qiuhongyang 2018年12月13日 上午10:47:53
 */
public class MergeXXXXObject4Ref extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String jobName = "MergeXXXXObject";

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

		public String curDir = ""; // 当前文件目录

		public void setup(Context context) throws IOException, InterruptedException {
			// 当前文件目录（两级路径）
			curDir = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit()); 
			
			System.out.println("map setup curDir:" + curDir);
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (curDir.endsWith("latest")) {
				context.getCounter("map", "count latest").increment(1);
			}
			else if (curDir.endsWith("new")) {
				context.getCounter("map", "count new").increment(1);
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
			}

			context.write(key, value);
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			/**
			 * 将引文用批次号  batch（格式: 20181213_154321） 分组排序，取最新一组
			 */

			HashMap<String, ArrayList<XXXXObject>> map = new HashMap<String, ArrayList<XXXXObject>>();
			List<String> lstBatch = new ArrayList<String>();
			for (BytesWritable item : values) {
				XXXXObject xxxxObject = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xxxxObject);
				String batch = xxxxObject.data.get("batch");
				if (!lstBatch.contains(batch)) {
					lstBatch.add(batch);
				}			
				if (map.containsKey(batch)) {
					ArrayList<XXXXObject> lst = map.get(batch);
					lst.add(xxxxObject);
					map.put(batch, lst);
				}
				else {
					ArrayList<XXXXObject> lst = new ArrayList<XXXXObject>();
					lst.add(xxxxObject);
					map.put(batch, lst);
				}
			}

			// 逆序，最新的在最前面
			Collections.sort(lstBatch, new Comparator<String>() {
				@Override
				public int compare(String batch1, String bathc2) {
					int i = batch1.compareTo(bathc2);
					if (i > 0) {
						return -1;
					} else {
						return 1;
					}
				}
			});
			
			// 取最新的一组输出
			for (XXXXObject xxxxObject : map.get(lstBatch.get(0))) {
				byte[] bytes = VipcloudUtil.SerializeObject(xxxxObject);
				context.getCounter("reduce", "count").increment(1);
				context.write(key, new BytesWritable(bytes));
			}
		}
	}
}
