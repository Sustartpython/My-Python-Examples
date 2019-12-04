package simple.jobstream.mapreduce.site.scopusjournal;

import java.io.IOException;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import com.process.frame.base.InHdfsOutHdfsJobInfo;

 
public class RawObj2XXXXObject3 extends InHdfsOutHdfsJobInfo { 
	public static Logger logger = Logger.getLogger(RawObj2XXXXObject3.class);
	static boolean testRun = false;
	static int testReduceNum = 7;
	static int reduceNum = 3;

//	batch = "";
	static String inputHdfsPath = "";
	static String outputHdfsPath = "";

	public void pre(Job job) {
//		inputHdfsPath = "/RawData/elsevier/scopus/rawXXXXObject";
//		outputHdfsPath = "/RawData/elsevier/scopus/XXXXObject";
		job.setJobName("ttest");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		//reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		//job.setJobName(job.getConfiguration().get("jobName"));
//		batch = job.getConfiguration().get("batch");
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
//		job.getConfiguration().set("io.compression.codecs",
//				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		//job.setInputFormatClass(TextInputFormat.class);
//		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		//job.setReducerClass(UniqXXXXObjectReducer.class);

		SequenceFileOutputFormat.setCompressOutput(job, true);

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

	// ======================================处理逻辑=======================================
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		
		public void setup(Context context) throws IOException, InterruptedException {
			
		}
		

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				throw new InterruptedException("123");

			}
		}

	}
}