package simple.jobstream.mapreduce.user.walker;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class ExportLngid extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 1;
	public static final String inputHdfsPath = "/DataAnalysis/cqu/finishdata/output";
	public static final String outputHdfsPath = "/user/tanl/TL/stat/ExportLngid";

	public void pre(Job job) {
		String jobName = "test." + this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

	
		job.setNumReduceTasks(reduceNum);
		
	}
	

	@Override
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	@Override
	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, Text> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

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

			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xxxobj);

			String rawid = "";
			String lngid = "";
			String provider = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				}				
			}

			String outKey = Integer.toString(lngid.length());
			String outVal = provider + "★" + lngid + "★" + rawid;

			context.write(new Text(outKey), new Text(outVal));
		}

	}

	public static class ProcessReducer extends Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			int cnt = 0;
			for (Text line : values) {
				cnt += 1;
				if (cnt > 10) {
					break;
				}
				String outLine = key.toString() + "★" + line.toString();
				context.getCounter("reduce", "outCount").increment(1);
				context.write(new Text(outLine), NullWritable.get());
			}			
		}
	}
}