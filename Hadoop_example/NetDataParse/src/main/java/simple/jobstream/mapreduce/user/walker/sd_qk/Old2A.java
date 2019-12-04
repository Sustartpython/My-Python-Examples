package simple.jobstream.mapreduce.user.walker.sd_qk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.util.Calendar;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//这个MR仅仅是拷贝作用
public class Old2A extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "sd_qk." + this.getClass().getSimpleName();
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
		// 老字段
		String rawid = "";
		String title = "";
		String title_alternative = "";
		String creator = "";
		String creator_institution = "";
		String subject = "";
		String description = "";
		String description_en = "";
		String identifier_doi = "";
		String identifier_pissn = "";
		String volume = "";
		String issue = "";
		String source = "";
		String date = "";
		String page = "";
		String date_created = "";

//		String provider_subject = ""; 
//		String type = ""; 
//		String medium = ""; 
//		String batch = ""; 
//		String publisher = ""; 
//		String language = ""; 
//		String country = ""; 
//		String provider= ""; 
//		String provider_url = ""; 
//		String provider_id = ""; 
//		String gch = ""; 
		
		private static String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		
		// 期刊信息
		private HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();
		private static Map<String, String> mapCountry = new HashMap<String, String>(); // 国家代码表
		private static Map<String, String> mapLanguage = new HashMap<String, String>(); // 语言代码表
		private static Map<String, String> mapSubject = new HashMap<String, String>(); // issn-类别代码表
		private static Map<String, String> mapSource = new HashMap<String, String>(); // issn-刊名表

		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);
		
		// 初始化期刊信息
		private void initJournalInfo(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/journal_info.json"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					Gson gson = new Gson();
					Type type = new TypeToken<HashMap<String, String>>() {}.getType();
					HashMap<String, String> mapField = gson.fromJson(line, type);
					journalInfo.put(mapField.get("issn"), mapField);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("journalInfo size: " + journalInfo.size());
		}

		private static void initMapCountryLanguage(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/国家语言对照表.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 2) {
						continue;
					}

					String[] vec = line.split("★");
					if (vec.length != 3) {
						continue;
					}

					String issn = vec[0].trim();
					String country = vec[1].trim();
					String language = vec[2].trim();

					if (country.length() > 0) {
						mapCountry.put(issn, country);
					}

					if (language.length() > 0) {
						mapLanguage.put(issn, language);
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("mapCountry size:" + mapCountry.size());
			System.out.println("mapLanguage size:" + mapLanguage.size());
		}

		private static void initMapSubject(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/issn-provider_subject.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 9) {
						continue;
					}

					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}

					String issn = vec[0].trim();
					String subject = vec[1].trim();

					if (subject.length() > 0) {
						mapSubject.put(issn, subject);
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("mapSubject size:" + mapSubject.size());
		}

		private static void initMapSource(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/issn-source.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 9) {
						continue;
					}

					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}

					String issn = vec[0].trim();
					String source = vec[1].trim();

					if (source.length() > 0) {
						mapSource.put(issn, source);
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("mapSource size:" + mapSource.size());
		}

		public void setup(Context context) throws IOException, InterruptedException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			initJournalInfo(context);
			initMapCountryLanguage(fs); // 初始化issn、国家、语言字典
			initMapSubject(fs);
			initMapSource(fs);
		}
		
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{// 老字段
				rawid = "";
				title = "";
				title_alternative = "";
				creator = "";
				creator_institution = "";
				subject = "";
				description = "";
				description_en = "";
				identifier_doi = "";
				identifier_pissn = "";
				volume = "";
				issue = "";
				source = "";
				date = "";
				page = "";
				date_created = "";
//				provider_subject = ""; 
//				type = ""; 
//				medium = ""; 
//				batch = ""; 
//				publisher = ""; 
//				language = ""; 
//				country = ""; 
//				provider= ""; 
//				provider_url = ""; 
//				provider_id = ""; 
//				gch = "";
			}
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_alternative")) {
					title_alternative = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description_en")) {
					description_en = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
			}
			
			
			String[] vec = StringHelper.parsePageInfo(page);
			String beginpage = vec[0];
			String endpage = vec[1];
			String jumppage = vec[2];
			
			String firstwriter = "";
			String firstorgan = "";
			vec = creator.split(";");
			if (vec.length > 0) {
				firstwriter = vec[0].trim();
			}
			vec = creator_institution.split(";");
			if (vec.length > 0) {
				firstorgan = vec[0].trim();
			}
			
			String language = "EN";
			if (mapLanguage.containsKey(identifier_pissn)) {
				language = mapLanguage.get(identifier_pissn);
			}
			String country = "NL"; // NL（荷兰）
			if (mapCountry.containsKey(identifier_pissn)) {
				country = mapCountry.get(identifier_pissn);
			}
			String provider_subject = ""; // 原始专辑、类别
			if (mapSubject.containsKey(identifier_pissn)) {
				provider_subject = mapSubject.get(identifier_pissn);
			}
			
			
						
			
			if (!journalInfo.containsKey(identifier_pissn)) {
				context.getCounter("map", "no identifier_pissn" + identifier_pissn).increment(1);
//				LogMR.log2HDFS4Mapper(context, logHDFSFile, identifier_pissn);
				return;
			}
			
			
			
			// 去掉第一作者后面的编号
			firstwriter = firstwriter.replaceAll("(.+)\\[[^\\]]+\\]", "$1");
			// 去掉第一机构前面的编号
			firstorgan = firstorgan.replaceAll("\\[[^\\]]+\\](.+)", "$1");
			// 将作者机构以数字编号
			try {
				vec = AuthorOrgan.renumber(creator, creator_institution);
			} catch (Exception e) {
				LogMR.log2HDFS4Mapper(context, logHDFSFile, rawid + "\t" + creator + "\t" + creator_institution);
				context.getCounter("map", "except renuber").increment(1);
				return;
			}
			creator = vec[0];
			creator_institution = vec[1];
			
			// 清理版权申明
			if (description.trim().toLowerCase().endsWith("© 2008 Elsevier B.V. All rights reserved.".toLowerCase())) {
				context.getCounter("map", "BV rights").increment(1);
				description = description.substring(0, description.length()  - "© 2008 Elsevier B.V. All rights reserved.".length());
			}
			else if (description.trim().toLowerCase().endsWith("All rights reserved.".toLowerCase())) {
				context.getCounter("map", "rights").increment(1);
				description = description.substring(0, description.length()  - "All rights reserved.".length());
			}
			
			
			context.getCounter("map", journalInfo.get(identifier_pissn).get("content_type")).increment(1);
			String sub_db = "";
			String sub_db_id = "";
			if (journalInfo.get(identifier_pissn).get("content_type").equals("JL")) {
				sub_db = "QK";
				sub_db_id = "00018";
			}
			else if (journalInfo.get(identifier_pissn).get("content_type").equals("BS")) {
				sub_db = "BS";
				sub_db_id = "00109";
			}
			else {
				context.getCounter("map", "err content_type").increment(1);
			}
			
			String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", sub_db_id);
				xObjOut.data.put("product", "SCIENCEDIRECT");
				xObjOut.data.put("sub_db", sub_db);
				xObjOut.data.put("provider", "ELSEVIER");
				xObjOut.data.put("down_date", "20190101");
				xObjOut.data.put("batch", "20190101_010101");
				xObjOut.data.put("doi", identifier_doi);
				xObjOut.data.put("source_type", "3");
				xObjOut.data.put("provider_url", "https://www.sciencedirect.com/science/article/pii/" + rawid);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alt", title_alternative);
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("title_series", "");
				xObjOut.data.put("keyword", subject);
				xObjOut.data.put("keyword_alt", "");
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", "");
				xObjOut.data.put("clc_no", "");
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_word", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", provider_subject);
				xObjOut.data.put("abstract", description);
				xObjOut.data.put("abstract_alt", description_en);
				xObjOut.data.put("abstract_type", "");
				xObjOut.data.put("abstract_alt_type", "");
				xObjOut.data.put("page_info", page);
				xObjOut.data.put("begin_page", beginpage);
				xObjOut.data.put("end_page", endpage);
				xObjOut.data.put("jump_page", jumppage);
				xObjOut.data.put("doc_code", "");
				xObjOut.data.put("doc_no", "");
				xObjOut.data.put("raw_type", "");
				xObjOut.data.put("recv_date", "");
				xObjOut.data.put("accept_date", "");
				xObjOut.data.put("revision_date", "");
				xObjOut.data.put("pub_date", "");
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("pub_place", "");
				xObjOut.data.put("page_cnt", "");
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "pdf");
				xObjOut.data.put("column_info", "");
				xObjOut.data.put("fund", "");
				xObjOut.data.put("fund_alt", "");
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", firstwriter);
				xObjOut.data.put("author", creator);
				xObjOut.data.put("author_raw", "");
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("corr_author", "");
				xObjOut.data.put("corr_author_id", "");
				xObjOut.data.put("email", "");
				xObjOut.data.put("subject_dsa", "");
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("contributor", "");
				xObjOut.data.put("contributor_id", "");
				xObjOut.data.put("contributor_alt", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ_1st", firstorgan);
				xObjOut.data.put("organ", creator_institution);
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("preferred_organ", "");
				xObjOut.data.put("host_organ_id", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("journal_raw_id", identifier_pissn);
				xObjOut.data.put("journal_name", source);
				xObjOut.data.put("journal_name_alt", "");
				xObjOut.data.put("pub_year", date);
				xObjOut.data.put("vol", "");
				xObjOut.data.put("num", issue);
				xObjOut.data.put("is_suppl", "");
				xObjOut.data.put("issn", identifier_pissn.substring(0, 4) + "-" + identifier_pissn.substring(4));
				xObjOut.data.put("eissn", "");
				xObjOut.data.put("cnno", "");
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
				xObjOut.data.put("ref_cnt", "");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");
				xObjOut.data.put("is_topcited", "");
				xObjOut.data.put("is_hotpaper", "");
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			context.getCounter("reduce", "count").increment(1);
		}
	}
}
