package simple.jobstream.mapreduce.site.wf_qk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

// 统计万方医学期刊的文章总量
public class CountMed extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "wf_qk_med." + this.getClass().getSimpleName();
		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}

	public void post(Job job) {

	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		// job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
//		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		private static Set<String> pykmSet = new HashSet<String>();

		public void setup(Context context) throws IOException, InterruptedException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/detail/med/pykm.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					pykmSet.add(line);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "inCount").increment(1);
			String rawid = "";
			String pykm = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				}
			}

			// 过滤掉非医学刊
			if (!pykmSet.contains(pykm)) {
				context.getCounter("map", "not med").increment(1);
				return;
			}

			// rawid 为 doi，非万方自有数据
			if (rawid.startsWith("10.")) {
				context.getCounter("map", "rawid_10.").increment(1);
				return;
			}
			
			context.getCounter("map", "medCount").increment(1);
		}
	}

	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {
			context.getCounter("reduce", "count").increment(1);
		}
	}
}