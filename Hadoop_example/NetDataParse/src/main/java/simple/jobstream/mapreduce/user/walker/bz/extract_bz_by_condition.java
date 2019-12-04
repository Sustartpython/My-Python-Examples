package simple.jobstream.mapreduce.user.walker.bz;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class extract_bz_by_condition extends InHdfsOutHdfsJobInfo {
	private static boolean bBCP = true;
	private static boolean testRun = false;
	private static int testReduceNum = 0;
	private static int reduceNum = 0;
	public static final String inputHdfsPath = "/DataAnalysis/BasicInfo/TitleInfo/TitleInfo";
	public static final String outputHdfsPath = "/vipuser/walker/output/bz/extract_data_by_condition";
	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
		job.setJobName(jobName);
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, BytesWritable> {
		
		public void setup(Context context) throws IOException,
				InterruptedException {
	        
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}
		
	
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xxxobj);

			String lngid = key.toString().trim();
			String type = "";
			String language  = "";
			String srcid = "";
			String libid = "";

			for (Map.Entry<String, String> updateItem : xxxobj.data.entrySet()) {
				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue();
					type = type.trim();
				}else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue();
					language = language.trim();
				}else if (updateItem.getKey().equals("srcid")) {
					srcid = updateItem.getValue();
					srcid = srcid.trim();
				}else if (updateItem.getKey().equals("libid")) {
					libid = updateItem.getValue();
					libid = libid.trim();
				}
			}
			
			if (!type.equals("5")) {
				return;
			}
			
			if (!language.equals("1")) {
				context.getCounter("reduce", "5-1").increment(1);
			}
			
			context.getCounter("reduce", "outCount").increment(1);
			context.write(key, value);
		}

	}

	public static class ProcessReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		public static int cntLine = 0;	//记录进入reduce的次数
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {			
			
			String bookid = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";	//搞个大值便于后面筛选小值
			
			for (Text val : values) {
				String line = val.toString().trim();
				if (line.length() < bookid.length()) {
					bookid = line;
				}
			}
			
			String outText = bookid + "\t" + key.toString() + "\r";
			
			context.getCounter("reduce", "count").increment(1);
			context.write(new Text(outText), NullWritable.get());
		}

	}

	@Override
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	@Override
	public String getHdfsOutput() {
		return outputHdfsPath;
	}
}