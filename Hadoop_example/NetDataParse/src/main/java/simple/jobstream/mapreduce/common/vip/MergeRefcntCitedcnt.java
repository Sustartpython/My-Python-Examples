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
 * <p>Description: 将引文l量,被引量写回 XXXXObject，主要供 JobNodeModel 类调用</p>  
 * @author liuqingxin 2019年02月14日 下午17:04:12
 */
public class MergeRefcntCitedcnt extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;


	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String jobName = "MergeRefcntCitedcnt";

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

//		 job.setInputFormatClass(SimpleTextInputFormat.class);
//		 job.setOutputFormatClass(TextOutputFormat.class);

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
		private static String inPathRefcnt = "";	// 提供引文量的路径
		private static String inPathCitedcnt = "";	// 提供被引量的路径
		private static String inPathXXXXObject = ""; // 提供 XXXXObject 的路径

		public void setup(Context context) throws IOException, InterruptedException {
			// 当前文件目录（两级路径）
			curDir = VipcloudUtil.GetInputPath((FileSplit) context.getInputSplit()); 
			inPathRefcnt = context.getConfiguration().get("inPathRefcnt");
			inPathCitedcnt = context.getConfiguration().get("inPathCitedcnt");
			inPathXXXXObject = context.getConfiguration().get("inPathXXXXObject");
			
			System.out.println("map setup curDir:" + curDir);
			System.out.println("map setup inPathRefcnt:" + inPathRefcnt);
			System.out.println("map setup inPathCitedcnt:" + inPathCitedcnt);
			System.out.println("map setup inPathXXXXObject:" + inPathXXXXObject);
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			if (inPathXXXXObject.endsWith(curDir)) {	// 提供 XXXXObject
				context.write(key, value);	// 直接输出
				context.getCounter("map", "count " + inPathXXXXObject).increment(1);
			}
			//提供refcnt和citedcnt可能是同一文件夹
			else if (inPathRefcnt.equals(inPathCitedcnt)) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("ref_cnt")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***refcnt***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count refcnt" + inPathRefcnt).increment(1);
					}
					else if (entry.getKey().equals("cited_cnt")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***citedcnt***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count citedcnt" + inPathCitedcnt).increment(1);
					}
				}										
			}
			else if (inPathRefcnt!=null && inPathRefcnt.endsWith(curDir)) {	// 提供 ref_cnt
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("ref_cnt")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***refcnt***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count refcnt" + inPathRefcnt).increment(1);
					}					
				}											
			}
			else if (inPathCitedcnt!=null && inPathCitedcnt.endsWith(curDir)) {	// 提供 cited_cnt
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("cited_cnt")) {
						XXXXObject xObjout = new XXXXObject();
						xObjout.data.put("***citedcnt***", entry.getValue());
						byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
						context.write(key, new BytesWritable(bytes));
						context.getCounter("map", "count citedcnt" + inPathCitedcnt).increment(1);
					}
				}											
			}
			else {
				context.getCounter("map", "count error curDir:" + curDir).increment(1);
				context.getCounter("map", "count error inPathRefcnt:" + inPathRefcnt).increment(1);
				context.getCounter("map", "count error inPathCitedcnt:" + inPathCitedcnt).increment(1);
				context.getCounter("map", "count error inPathXXXXObject:" + inPathXXXXObject).increment(1);
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			XXXXObject xObjOut = null; // 用于最后输出
			String ref_cnt = "";
			String cited_cnt = "";
			
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);

				boolean isRefCnt = false;
				boolean isCitedCnt = false;
				for (Map.Entry<String, String> entry : xObj.data.entrySet()) {
					if (entry.getKey().equals("***refcnt***")) {
						ref_cnt = entry.getValue();						
						isRefCnt = true;
					}
					else if (entry.getKey().equals("***citedcnt***")) {
						cited_cnt = entry.getValue();						
						isCitedCnt = true;
					}
				}
				
				// 不是 isRefCnt也不是 isCitedCnt
				if (!isRefCnt && !isCitedCnt) {
					xObjOut = xObj;
				}
			}
			if (xObjOut == null) {
				context.getCounter("reduce", "NOMainDataCount").increment(1);
				return;
			}
			
			//有值才写入
			if (!ref_cnt.equals("")) {
				xObjOut.data.put("ref_cnt",ref_cnt);
				context.getCounter("reduce", "ref_cnt field count").increment(1);
			}
			if (!cited_cnt.equals("")) {
				xObjOut.data.put("cited_cnt",cited_cnt);
				context.getCounter("reduce", "cited_cnt field count").increment(1);
			}				

			context.getCounter("reduce", "outCount").increment(1);
			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(key, new BytesWritable(bytes));		
		}
	}
}
