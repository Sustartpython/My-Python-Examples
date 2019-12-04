package simple.jobstream.mapreduce.user.walker;

import java.io.IOException;
import java.util.Map;

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

public class GetSomeFieldFromIntermediateResult extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;

	private static int reduceNum = 1;

	public static final String inputHdfsPath = "/RawData/BasicInfo_Filter_99_98_merge";
	// public static final String inputHdfsPath =
	// "/VipProcessData/BasicObject/TitleObject";
	// public static final String inputHdfsPath =
	// "/VipProcessData/BasicInfo/TitleInfo/TitleInfo";
	public static final String outputHdfsPath = "/user/qhy/output/GetSomeFieldFromIntermediateResult";

	public void pre(Job job) {
		String jobName = "GetSomeFieldFromIntermediateResult";
		if (testRun) {
			jobName = "test_" + jobName;
		}

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
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
//		JobConfUtil.setTaskPerMapMemory(job, 1024 * 10);
//		JobConfUtil.setTaskPerReduceMemory(job, 1024 * 8);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		public void setup(Context context) throws IOException, InterruptedException {
		
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		private String cleanField(String field) {
			field = field.trim();
			field = field.replaceAll("\r\n", "; ");
			field = field.replaceAll("\r", "; ");
			field = field.replaceAll("\n", "; ");
			field = field.replaceAll("\t", " ");

			return field;
		}

		public void map(Text key, BytesWritable values, Context context) throws IOException, InterruptedException {
			/*
			 * XXXXObject xxxobj = new XXXXObject();
			 * VipcloudUtil.DeserializeObject(values.getBytes(), xxxobj);
			 */
			XXXXObject bxObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), bxObj);
			
			String lngid = key.toString();
			
			String type = "";
			String language = "";
			String srcid = "";
			String libid = "";

			String num = "";
			String cnno = "";
			String issn = "";
			String firstclass = "";
			String title_c = "";

			String remark_c = "";
			String keyword_c = "";
			String beginpage = "";
			String endpage = "";
			String introduce = "";

			String years = "";
			String vol = "";
			String strrefsearch = "";
			String gch = "";
			String refids = "";

			for (Map.Entry<String, String> updateItem : bxObj.data.entrySet()) {
				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue();
					type = type.trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue();
					language = language.trim();
				} else if (updateItem.getKey().equals("srcid")) {
					srcid = updateItem.getValue();
					srcid = srcid.trim();
				} else if (updateItem.getKey().equals("libid")) {
					libid = updateItem.getValue();
					libid = libid.trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue();
					num = cleanField(num);
				} else if (updateItem.getKey().equals("cnno")) {
					cnno = updateItem.getValue();
					cnno = cleanField(cnno);
				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue();
					issn = cleanField(issn);
				} else if (updateItem.getKey().equals("firstclass")) {
					firstclass = updateItem.getValue();
					firstclass = cleanField(firstclass);
				} else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue();
					title_c = cleanField(title_c);
				} else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue();
					remark_c = cleanField(remark_c);
				} else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue();
					keyword_c = cleanField(keyword_c);
				} else if (updateItem.getKey().equals("beginpage")) {
					beginpage = updateItem.getValue();
					beginpage = cleanField(beginpage);
				} else if (updateItem.getKey().equals("endpage")) {
					endpage = updateItem.getValue();
					endpage = cleanField(endpage);
				} else if (updateItem.getKey().equals("introduce")) {
					introduce = updateItem.getValue();
					introduce = cleanField(introduce);
				} else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue();
					years = cleanField(years);
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue();
					vol = cleanField(vol);
					if (vol.length() < 1) {
						vol = "vol";
					}
				} else if (updateItem.getKey().equals("strrefsearch")) {
					strrefsearch = updateItem.getValue();
					strrefsearch = cleanField(strrefsearch);
				} else if (updateItem.getKey().equals("gch")) {
					gch = updateItem.getValue();
					gch = cleanField(gch);
				} else if (updateItem.getKey().equals("referids_real")) {
					refids = updateItem.getValue();
					refids = cleanField(refids);
				}
			}

			context.getCounter("map", "total").increment(1);
			if (!type.equals("7")) {
				return;
			}
			if (!language.equals("1")) {
				return;
			}
			context.getCounter("map", "7_1").increment(1);
			if (lngid.startsWith("TS")) {
				context.getCounter("map", "TS").increment(1);
			}
			else if (lngid.startsWith("DU_TS_")) {
				context.getCounter("map", "DU_TS_").increment(1);
			}
			else {
				context.getCounter("map", "outCount").increment(1);
				String line = lngid + "\t" + title_c;
				
				context.write(new Text(line), NullWritable.get());
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