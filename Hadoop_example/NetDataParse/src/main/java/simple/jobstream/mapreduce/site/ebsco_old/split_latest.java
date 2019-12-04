package simple.jobstream.mapreduce.site.ebsco_old;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;

// 将以往数据转为A层格式
public class split_latest extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 30;

	public static String inputHdfsPath = "/RawData/ebsco/ebsco_old/latest";
//	public static String outputHdfsPath = "/RawData/ebsco/a9h_ASC/latest_XXXXObject";
	public static String outputHdfsPath = "/RawData/ebsco/bth_BSC/latest_XXXXObject";

	public void pre(Job job) {
		String jobName = "ebsco_a9h." + this.getClass().getSimpleName();
		job.setJobName(jobName);
//
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
		// 老字段
		private static String rawid = "";
		private static String db = "";
		private static String doi = "";
		private static String title = "";
		private static String title_alternative = "";
		private static String identifier_pissn = "";
		private static String creator = "";
		private static String creator_institution = "";
		private static String source = "";
		private static String description = "";
		private static String description_en = "";
		private static String subject = "";
		private static String language = "";
//		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				rawid = "";
				db = "";
				doi = "";
				title = "";
				title_alternative = "";
				identifier_pissn = "";
				creator = "";
				creator_institution = "";
				source = "";
				description = "";
				description_en = "";
				subject = "";
				language = "";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("db")) {
					db = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("title_alternative")) {
					title_alternative = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description_en")) {
					description_en = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}

			}
			context.getCounter("map", "count all").increment(1);
			
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("db", db);
				xObjOut.data.put("doi", doi);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alternative", title_alternative);
				xObjOut.data.put("identifier_pissn", identifier_pissn);
				xObjOut.data.put("creator", creator);
				xObjOut.data.put("creator_institution", creator_institution);
				xObjOut.data.put("source_all", source);
				xObjOut.data.put("description", description);
				xObjOut.data.put("description_en", description_en);
				xObjOut.data.put("subject", subject);
				xObjOut.data.put("language", language);
			}
			
			if (db.equals("bth")) {
				context.getCounter("map", "count_bth").increment(1);
				byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
				context.write(new Text(rawid), new BytesWritable(bytes));
			}
			
		}
	}
	
}
