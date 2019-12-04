package simple.jobstream.mapreduce.site.ebsco_cmedm;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;


public class ExoprtID extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 1;
	
	public static final String inputHdfsPath = "/RawData/ebsco/aph_ASP/latest";
	public static final String outputHdfsPath = "/RawData/ebsco/aph_ASP/ID";
	

	
	public void pre(Job job) {
		String jobName = "cnki_bs." + this.getClass().getSimpleName();

		job.setJobName(jobName);
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
		
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		public static Set<String> isbnSet = new HashSet<String>();


		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}
			
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {	
				String lngid= "";
				String rawid = "";
				String oldid = "";
				String newid = "";
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().toLowerCase().equals("rawid")) {
						rawid = updateItem.getValue().trim();
					}
				}
				oldid = VipIdEncode.getLngid("00100", rawid, false);
				rawid="aph_"+rawid;
				lngid = VipIdEncode.getLngid("00100", rawid, false);
				newid =oldid  +"★" +lngid ;
				context.getCounter("map", "count").increment(1);
				context.write(new Text(newid), NullWritable.get());
				}

	}

	public static class ProcessReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			
			context.getCounter("reducer", "count").increment(1);
			context.write(key, NullWritable.get());
		}

	}
}