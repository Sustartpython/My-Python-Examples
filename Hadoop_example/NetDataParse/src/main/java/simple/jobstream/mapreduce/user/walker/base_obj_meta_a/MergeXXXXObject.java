package simple.jobstream.mapreduce.user.walker.base_obj_meta_a;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
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
 * <p>Description: 合并 XXXXObject，base_obj_meta_a 专用</p>  
 * @author qiuhongyang 2018年12月13日 上午10:47:53
 */
public class MergeXXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	public void pre(Job job) {
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");	 
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");

		job.setJobName(job.getConfiguration().get("jobName"));
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
			else if (curDir.endsWith("new_xobj")) {
				context.getCounter("map", "count new_xobj").increment(1);
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
			}
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			String lngid = key.toString();
			if (lngid.startsWith("_delete_")) {		// 删除数据
				xObj.data.put("_delete_", "true");		// 添加辅助字段
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);				
				context.getCounter("map", "count _delete_").increment(1);
				context.write(new Text(lngid.substring("_delete_".length())), new BytesWritable(bytes));
			}
			else {					
				xObj.data.put("_delete_", "false");		// 添加辅助字段
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);				
				context.getCounter("map", "count _normal_").increment(1);
				context.write(key, new BytesWritable(bytes));
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			/**
			 * 将引文用批次号  batch（格式: 20181213_154321） 分组排序，新的覆盖旧的。
			 */
			List<XXXXObject> xObjList = new ArrayList<XXXXObject>();
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				xObjList.add(xObj);
			}
			
			// 按 batch 从旧到新排序
			Collections.sort(xObjList, new Comparator<XXXXObject>() {
	            @Override
	            public int compare(XXXXObject obj1, XXXXObject obj2) {
	                int i = obj1.data.get("batch").compareTo(obj2.data.get("batch"));
	                if ( i > 0 ) {
	                    return 1;
	                } else {
	                    return -1;
	                }
	            }
	        });
			
			XXXXObject xObjOut = new XXXXObject();
			// 从旧到新覆盖
			for (XXXXObject xObj : xObjList) {
				xObjOut.data.putAll(xObj.data);
			}
			
			// 不删除就输出
			if (xObjOut.data.get("_delete_").equalsIgnoreCase("false")) {
				xObjOut.data.remove("_delete_");	// 删除辅助字段
				context.getCounter("reduce", "out count").increment(1);
				byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
				context.write(key, new BytesWritable(bytes));
			}
			else {
				context.getCounter("reduce", "del count").increment(1);
			}
			
		}
	}
}
