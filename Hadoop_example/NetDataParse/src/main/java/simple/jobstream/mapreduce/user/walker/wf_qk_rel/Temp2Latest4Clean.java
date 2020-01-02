package simple.jobstream.mapreduce.user.walker.wf_qk_rel;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

// 清理数据
public class Temp2Latest4Clean extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "/RawData/wanfang/qk/rel/latest_bak20190603";
	public static String outputHdfsPath = "/RawData/wanfang/qk/rel/latest";

	public void pre(Job job) {
		String jobName = "wf_qk_rel." + this.getClass().getSimpleName();

		job.setJobName(jobName);

//		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
//		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setMapperClass(ProcessMapper.class);
		// job.setReducerClass(ProcessReducer.class);

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
		private static Map<String, String> mapJournalID = new HashMap<String, String>();

		public void setup(Context context) throws IOException, InterruptedException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/detail/_ref_file/journal_id.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.toLowerCase().trim();
					if (line.length() < 1) {
						continue;
					}
					mapJournalID.put(line, "");
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("mapJournalID size: " + mapJournalID.size());
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "inCount").increment(1);

			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xxxobj);

			String rawid = key.toString().trim();
			String batch = "";
			String down_date = "";
			String ref_cnt = "";
			String cited_cnt = "";

			for (Map.Entry<String, String> updateItem : xxxobj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				}
			}

			if (cited_cnt.indexOf('@') < 0) {
				boolean isMatch = Pattern.matches("^\\d+$", cited_cnt);
				if (isMatch) {
					cited_cnt = cited_cnt + "@20190101";
				}
				else {
					cited_cnt = "";
				}
			}
			
			if (cited_cnt.length() < 1) {
				context.getCounter("map", "cited_cnt blank").increment(1);
			}
			else if (cited_cnt.endsWith("@20190101")) {
				context.getCounter("map", "@20190101").increment(1);
			}

			xxxobj.data.put("cited_cnt", cited_cnt);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xxxobj);
			context.getCounter("map", "outCount").increment(1);
			context.write(key, new BytesWritable(bytes));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter("reduce", "count").increment(1);
		}
	}
}