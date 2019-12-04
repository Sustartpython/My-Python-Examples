package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
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
 * <p>Description: 将引文信息,被引信息写回 XXXXObject，主要供 JobNodeModel 类调用</p>  
 * @author liuqingxin 2019年01月21日 下午13:45:12
 */
public class MergeRefidCitedid extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String jobName = "MergeRefidCitedid";

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
		private static String inPathRefid = "";	// 提供引文ID的路径
		private static String inPathCitedid = "";	// 提供被引文ID的路径
		private static String inPathXXXXObject = ""; // 提供 XXXXObject 的路径

		public void setup(Context context) throws IOException, InterruptedException {
			// 当前文件目录（两级路径）
			curDir = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit()); 
			inPathRefid = context.getConfiguration().get("inPathRefid");
			inPathCitedid = context.getConfiguration().get("inPathCitedid");
			inPathXXXXObject = context.getConfiguration().get("inPathXXXXObject");
			
			System.out.println("map setup curDir:" + curDir);
			System.out.println("map setup inPathRefid:" + inPathRefid);
			System.out.println("map setup inPathCitedid:" + inPathCitedid);
			System.out.println("map setup inPathXXXXObject:" + inPathXXXXObject);
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (inPathRefid!=null && inPathRefid.endsWith(curDir)) {	// 提供 ref_id
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("lngid")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***refid***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count " + inPathRefid).increment(1);
					}
				}																
			}
			else if (inPathCitedid!=null && inPathCitedid.endsWith(curDir)) {	// 提供 cited_id
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("lngid")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***citedid***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count " + inPathCitedid).increment(1);
					}
				}																						
			}
			else if (inPathXXXXObject.endsWith(curDir)) {	// 提供 XXXXObject
				context.write(key, value);	// 直接输出
				context.getCounter("map", "count " + inPathXXXXObject).increment(1);
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
				context.getCounter("map", "count error inPathRefid:" + inPathRefid).increment(1);
				context.getCounter("map", "count error inPathCitedid:" + inPathCitedid).increment(1);
				context.getCounter("map", "count error inPathXXXXObject:" + inPathXXXXObject).increment(1);
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			XXXXObject xObjOut = null; // 用于最后输出
			String ref_id = "";
			String cited_id = "";
			
			List<String> reflist = new ArrayList<>();
			List<String> citedlist = new ArrayList<>();
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				boolean isRefId = false;
				boolean isCitedId = false;
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("***refid***")) {
						reflist.add(entry.getValue());						
						isRefId = true;
					}
					else if (entry.getKey().equals("***citedid***")) {
						citedlist.add(entry.getValue());						
						isCitedId = true;
					}
				}
				
				// 不是 isRefId也不是 isCitedId
				if (!isRefId && !isCitedId) {
					xObjOut = xObj;
				}
			}
			if (xObjOut == null) {
				context.getCounter("reduce", "NOMainDataCount").increment(1);
				return;
			}
			
			if (!reflist.isEmpty()) {
				Collections.sort(reflist);
				for (String rid : reflist) {
					ref_id = ref_id + rid + ";";
				}
				ref_id = ref_id.replaceAll(";$", "");
				xObjOut.data.put("ref_id",ref_id);
				context.getCounter("reduce", "ref_id field count").increment(1);
			}
			
			if (!citedlist.isEmpty()) {
				Collections.sort(citedlist);				
				for (String cited : citedlist) {
					cited_id = cited_id + cited + ";";
				}				
				cited_id = cited_id.replaceAll(";$", "");				
				xObjOut.data.put("cited_id",cited_id);
				context.getCounter("reduce", "cited_id field count").increment(1);
			}
			
			

			context.getCounter("reduce", "outCount").increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(key, new BytesWritable(bytes));		
		}
	}
}
