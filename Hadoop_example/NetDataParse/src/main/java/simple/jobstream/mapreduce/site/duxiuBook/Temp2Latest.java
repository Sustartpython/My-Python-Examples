package simple.jobstream.mapreduce.site.duxiuBook;

import java.io.IOException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;

//这个MR仅仅是拷贝作用
public class Temp2Latest extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 0;
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "DuxiuTemp2Latest";
		if (testRun) {
			jobName = "test_" + jobName;
		}

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
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		//job.setInputFormatClass(SimpleTextInputFormat.class);
		//job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}

	}

	public void post(Job job) {
		FileSystem fs = null;
		try {
			fs = FileSystem.get(job.getConfiguration());
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
		Path src = new Path("/RawData/chaoxing/duxiu_ts/big_htm/tmp");
		try {
			if (fs.exists(src)) {
				//更改为流程跑完之后的当前日期，精确到天
				Date dt = new Date();
				DateFormat df = new SimpleDateFormat("yyyyMMdd");
				String nowDate = df.format(dt);
				Path dst = new Path("/RawData/chaoxing/duxiu_ts/big_htm/" + nowDate);
				fs.rename(src, dst);
			}
		} catch (IllegalArgumentException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		} catch (IOException e) {
			// TODO Auto-generated catch block
			e.printStackTrace();
		}
	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			//file1.renameTo(new File("/RawData/chaoxing/duxiu_ts/big_htm/" + nowDate));
			// boolean sign = fs.rename(new Path(), new Path());

			context.getCounter("map", "count").increment(1);

			context.write(key, value); //拷贝
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter("reduce", "count").increment(1);

		}
	}
}
