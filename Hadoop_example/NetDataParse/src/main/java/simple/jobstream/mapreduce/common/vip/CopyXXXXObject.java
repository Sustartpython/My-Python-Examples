package simple.jobstream.mapreduce.common.vip;

import java.io.IOException;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;

/**
 * <p>Description: 拷贝 XXXXObject，主要供 JobNodeModel 类调用</p>  
 * @author qiuhongyang 2018年11月5日 上午9:16:09
 */
public class CopyXXXXObject extends InHdfsOutHdfsJobInfo {
	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";
	private static String jobName = "CopyXXXXObject";

	public void pre(Job job) {
		jobName = job.getConfiguration().get("jobName");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");

		job.setJobName(jobName);
	}

	@Override
	public void post(Job job) {
		// TODO Auto-generated method stub
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setMapperClass(ProcessMapper.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "count").increment(1);

			context.write(key, value); // 拷贝
		}
	}
}
