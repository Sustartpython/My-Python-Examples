package simple.jobstream.mapreduce.user.walker.wf_qk_ref;

import java.io.IOException;
import java.util.Map;

import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer4Ref;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//
public class Old2A extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
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

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer4Ref.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
//		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

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

		private static int cnt = 0;
		private static String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

		private static ScriptEngineManager manager = null;
		private static ScriptEngine engine = null;

//		private static String lngid = "";		// lngid 在 reduce 中生成
//		private static String sub_db_id = "";
//		private static String product = "";
//		private static String sub_db = "";
//		private static String provider = "";
//		private static String down_date = "";
//		private static String batch = "";
//		private static String doi = "";
//		private static String title = "";
//		private static String title_alt = "";
//		private static String page_info = "";
//		private static String begin_page = "";
//		private static String end_page = "";
//		private static String jump_page = "";
//		private static String raw_type = "";
//		private static String author_1st = "";
//		private static String author = "";
//		private static String author_alt = "";
//		private static String pub_year = "";
//		private static String vol = "";
//		private static String num = "";
//		private static String publisher = "";
//		private static String cited_id = "";
//		private static String linked_id = "";
//		private static String refer_text_raw = "";
//		private static String refer_text_raw_alt = "";
//		private static String refer_text_site = "";
//		private static String refer_text_site_alt = "";
//		private static String refer_text = "";
//		private static String refer_text_alt = "";
//		private static String source_name = "";
//		private static String source_name_alt = "";
//		private static String strtype = "";

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String type = "";
			String lngid = key.toString();
			String rawsourceid = "";
			String refertext = "";
			String strtitle = "";
			String strtype = "";
			String strname = "";
			String strwriter1 = "";
			String stryearvolnum = "";
			String strpubwriter = "";
			String strpages = "";
			String doi = "";
			String disproof_id = "";

			XXXXObject xObjIn = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObjIn);
			for (Map.Entry<String, String> updateItem : xObjIn.data.entrySet()) {
				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawsourceid")) {
					rawsourceid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refertext")) {
					refertext = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strtitle")) {
					strtitle = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strtype")) {
					strtype = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strname")) {
					strname = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strwriter1")) {
					strwriter1 = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("stryearvolnum")) {
					stryearvolnum = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strpubwriter")) {
					strpubwriter = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strpages")) {
					strpages = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("disproof_id")) {
					disproof_id = updateItem.getValue().trim();
				}
			}
			if (type.equalsIgnoreCase("99")) {
				context.getCounter("map", "count 99").increment(1);
			}
			else {
				context.getCounter("map", "count not 99").increment(1);
				return;
			}
			

			XXXXObject xObj = new XXXXObject();
			{
//				xObj.data.put("lngid", lngid);		// lngid 在 reduce 中生成
				xObj.data.put("sub_db_id", "00004");
				xObj.data.put("product", "WANFANG");
				xObj.data.put("sub_db", "CSPD");
				xObj.data.put("provider", "WANFANG");
				xObj.data.put("down_date", "20190101");
				xObj.data.put("batch", "20190101_010101");
				xObj.data.put("doi", doi);
				xObj.data.put("title", strtitle);
				xObj.data.put("title_alt", "");
				xObj.data.put("page_info", strpages);
				xObj.data.put("begin_page", "");
				xObj.data.put("end_page", "");
				xObj.data.put("jump_page", "");
				xObj.data.put("raw_type", "");
				xObj.data.put("author_1st", "");
				xObj.data.put("author", strwriter1);
				xObj.data.put("author_alt", "");
				xObj.data.put("pub_year", stryearvolnum);
				xObj.data.put("vol", stryearvolnum);
				xObj.data.put("num", stryearvolnum);
				xObj.data.put("publisher", strpubwriter);
				xObj.data.put("cited_id", VipIdEncode.getLngid("00004", rawsourceid, false) + "@" + rawsourceid);
				xObj.data.put("linked_id", disproof_id);
				xObj.data.put("refer_text_raw", "");
				xObj.data.put("refer_text_raw_alt", "");
				xObj.data.put("refer_text_site", refertext);
				xObj.data.put("refer_text_site_alt", "");
				xObj.data.put("refer_text", refertext);
				xObj.data.put("refer_text_alt", "");
				xObj.data.put("source_name", strname);
				xObj.data.put("source_name_alt", "");
				xObj.data.put("strtype", strtype);
			}

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawsourceid), new BytesWritable(bytes));

		}
	}

}
