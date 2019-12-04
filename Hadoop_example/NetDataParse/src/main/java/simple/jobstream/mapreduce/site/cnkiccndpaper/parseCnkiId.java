package simple.jobstream.mapreduce.site.cnkiccndpaper;

import java.awt.List;
import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import javax.sound.sampled.Line;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.DIV;
import org.json.JSONObject;
import org.json.JSONTokener;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

public class parseCnkiId extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "";
	public static String[] coveridArray = null;

	public void pre(Job job) {
		String jobName = "parsebsid";
		if (testRun) {
			jobName = "test_" + jobName;
		}

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
		// job.setOutputFormatClass(TextOutputFormat.class);
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);
		job.setOutputKeyClass(Text.class);
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, NullWritable> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public String[] getBookidArray() throws IOException {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			// check if the file exists
			Path path = new Path(ref_file_path);
			if (fs.exists(path)) {
				FSDataInputStream is = fs.open(path);
				// get the file info to create the buffer
				FileStatus stat = fs.getFileStatus(path);

				// create the buffer
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
				is.readFully(0, buffer);

				String coveridString = new String(buffer);

				return coveridString.split("\\★");
			} else {
				return null;
			}

		}

		public void map(LongWritable key, Text values, Context context) throws IOException, InterruptedException {

			String htmlText = values.toString();

			String catpath = "";
			// cnki报纸id
			if (htmlText.contains("tableStyle")) {
				JsonObject obj = null;
				obj = new JsonParser().parse(htmlText).getAsJsonObject();
				String html = obj.get("date").getAsString();
				String pykm = obj.get("pykm").getAsString();
				Document doc = Jsoup.parse(html);
				Elements divs = doc.select("tr");
				for (Element e : divs) {
					Element dateTag = e.select("td[align = center]").first();
					Element tagA = e.select("a").first();
					if (tagA == null) {
						continue;
					}
					String linkString = tagA.attr("href").trim();
					if(linkString.contains("&") && linkString.split("&").length >2) {
						String dbcode = linkString.split("&")[1].replace("dbCode=", "");
						String filename = linkString.split("&")[2].replace("fileName=", "");
						String title = tagA.text().trim();
						String line = filename + " " + dbcode + " " + pykm + " 0 0";
						context.getCounter("map", "total").increment(1);
						context.write(new Text(line), NullWritable.get());
					}
				
				}

			} else {
				return;
			}
		}
	}
	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter("reducer", "count").increment(1);
			context.write(key, NullWritable.get());
		}

	}
}