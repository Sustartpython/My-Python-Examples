package simple.jobstream.mapreduce.site.cnki_hy;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

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
public class ZTXXXXObiectForA extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 30;

	public static String inputHdfsPath = "/RawData/cnki/hy/latest";
	public static String outputHdfsPath = "/RawData/cnki/hy/latestA";

	public void pre(Job job) {
		String jobName = "wanfang_zl." + this.getClass().getSimpleName();
		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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

		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		// 老字段
		private static String provider_id = "";
		private static String provider = "";
		private static String provider_url = "";
		private static String batch = "";
		private static String medium = "";
		private static String country = "";
		private static String description_fund = "";
		private static String subject_clc = "";
		private static String type = "";
		private static String language = "";
		private static String lngid = "";
		private static String rawid = "";
		private static String title = "";
		private static String date = "";
		private static String title_series = "";
		private static String creator = "";
		private static String creator_institution = "";
		private static String source = "";
		private static String source_institution = "";
		private static String subject = "";
		private static String description = "";
		private static String creator_release = "";
		private static String date_created = "";
		private static String day = "";
		private static String month = "";
		private static String year = "";
		private static String host_organ = "";
		private static String clc_no_1st = "";
		private static String organ_1st = "";
		private static String author_1st = "";

		// 重新生成batch
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				provider_id = "";
				provider = "";
				provider_url = "";
				batch = "";
				medium = "";
				country = "CN";
				description_fund = "";
				subject_clc = "";
				type = "";
				language = "ZH";
				lngid = "";
				rawid = "";
				title = "";
				date = "";
				title_series = "";
				creator = "";
				creator_institution = "";
				source = "";
				source_institution = "";
				subject = "";
				description = "";
				creator_release = "";
				date_created = "";
				year = "";
				day = "";
				month = "";
				host_organ = "";
				clc_no_1st = "";
				organ_1st = "";
				author_1st = "";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_c")) {
					title = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("hymeetingrecordname")) {
					title_series = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("showwriter")) {
					creator = updateItem.getValue().replace("； ", ";").replaceAll("；", ";").replaceAll(";$", "").trim();
				}

				if (updateItem.getKey().equals("media_c")) {
					source = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("hymeetingplace")) {
					source_institution = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_c")) {
					subject = updateItem.getValue().trim();
					subject = subject.replace("；", ";").replaceAll(";$", "");
				}
				if (updateItem.getKey().equals("remark_c")) {
					description = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("hypressorganization")) {
					creator_release = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("showorgan")) {
					creator_institution = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("flh")) {
					subject_clc = updateItem.getValue().replace(" ", ";").replace(" ", ";").replace("+", "").trim()
							.replace("+", "");
				}
				if (updateItem.getKey().equals("hymeetingdate")) {
					date_created = updateItem.getValue().trim();
					String[] list = date_created.split("-");
					if (list.length == 3) {
						year = list[0];
						month = list[1];
						day = list[2];
					} else if (list.length == 2) {
						year = list[0];
						month = list[1];
						day = "00";
					} else if (list.length == 1) {
						year = list[0];
						if (year.equals("")) {
							year = "1900";
						}
						month = "00";
						day = "00";
					} else if (list.length == 0) {
						year = "1900";
						month = "00";
						day = "00";
					}
					if (month.length() == 1) {
						month = "0" + month;
					}
					if (day.length() == 1) {
						day = "0" + day;
					}
					date_created = year + month + day;
					if (date_created.length() > 8) {
						date_created = date_created.substring(0, 8);
					}
					if (date_created.length() < 4) {
						date_created = "19000000";
					}
					date = date_created.substring(0, 4);
				}

				if (updateItem.getKey().equals("description_fund")) {
					description_fund = updateItem.getValue().trim();
					description_fund = description_fund.replace("【基金】", "");
				}
			}
			lngid = VipIdEncode.getLngid("00090", rawid, false);
			context.getCounter("map", "countAll").increment(1);
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 剔除rawid为空的数据
			if (rawid.length() < 1) {
				context.getCounter("map", "no rawid").increment(1);
				return;
			}
			lngid = VipIdEncode.getLngid("00090", rawid, false);
			provider_url = "http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CIPD&filename=" + rawid;

			if (subject_clc.contains(";")) {
				clc_no_1st = subject_clc.split(";")[0];
			} else {
				if (subject_clc.length() > 0) {
					clc_no_1st = subject_clc;
				}
			}

			// 处理第一机构
			if (creator_institution.contains(";")) {
				if (creator_institution.split(";").length < 1) {
					creator_institution = "";
					organ_1st = "";

					context.getCounter("map", "onle ;号").increment(1);
				} else {
					String news = "";
					List<String> list = Arrays.asList(creator_institution.split(";"));
					Set<String> set = new HashSet<String>(list);
					List<String> result = new ArrayList<String>(set);
					for (int i = 0; i < result.size(); i++) {
						news = news + ";" + result.get(i);
					}
					organ_1st = creator_institution.split(";")[0];
					creator_institution = news.replaceAll("^;", "").trim();
					organ_1st = creator_institution.split(";")[0];
				}
			} else {
				if (creator_institution.length() > 0) {
					organ_1st = creator_institution;
				}
			}

			// 处理第一作者
			if (creator.contains(";")) {
				author_1st = creator.split(";")[0];
			} else {
				if (creator.length() > 0) {
					author_1st = creator;
				}
			}
			// 去除关键字中多余的;号
			subject = StringHelper.cleanSemicolon(subject);

			// 处理作者问题
			creator = creator.replaceAll("； ", ";").replaceAll("；", ";").replaceAll("；$", "").replaceAll(";$", "");
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00090");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "CPFD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", "20190520");
				xObjOut.data.put("batch", "20190523_010101");
				xObjOut.data.put("doi", "");
				xObjOut.data.put("source_type", "6");
				xObjOut.data.put("provider_url", provider_url);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alt", "");
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("keyword", subject);
				xObjOut.data.put("keyword_alt", "");
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", clc_no_1st);
				xObjOut.data.put("clc_no", subject_clc);
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", "");
				xObjOut.data.put("abstract", description);
				xObjOut.data.put("abstract_alt", "");
				xObjOut.data.put("begin_page", "");
				xObjOut.data.put("end_page", "");
				xObjOut.data.put("jump_page", "");
				xObjOut.data.put("accept_date", date_created);
				xObjOut.data.put("pub_date", date_created);
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("pub_place", "");
				xObjOut.data.put("page_cnt", "");
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("fund", description_fund);
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", author_1st);
				xObjOut.data.put("author", creator);
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("corr_author", "");
				xObjOut.data.put("corr_author_id", "");
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ_1st", organ_1st.replaceAll("；$", ""));
				xObjOut.data.put("organ", creator_institution.replace("； ", ";").replaceAll("；$", ""));
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("host_organ", creator_release.replace("、", ";").replaceAll(";$", "").trim());
				xObjOut.data.put("host_organ_id", "");
				xObjOut.data.put("sponsor", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("pub_year", date);
				xObjOut.data.put("vol", "");
				xObjOut.data.put("num", "");
				xObjOut.data.put("is_suppl", "");
				xObjOut.data.put("issn", "");
				xObjOut.data.put("eissn", "");
				xObjOut.data.put("cnno", "");
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("meeting_name", source);
				xObjOut.data.put("meeting_name_alt", "");
				xObjOut.data.put("meeting_record_name", title_series);
				xObjOut.data.put("meeting_record_name_alt", "");
				xObjOut.data.put("meeting_place", source_institution);
				xObjOut.data.put("meeting_counts", "");
				xObjOut.data.put("meeting_code", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {

		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reducer", "count").increment(1);
			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度
			context.write(key, bOut);
		}
	}
}
