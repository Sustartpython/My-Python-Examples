package simple.jobstream.mapreduce.site.pubmed;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.yarn.webapp.hamlet.Hamlet.STRONG;
import org.apache.xerces.impl.dv.xs.DayDV;
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.junit.Test;
import org.mockito.internal.matchers.And;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.pubmed.GetData;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class testparse extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
//		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
//		System.out.println(job.getConfiguration().get("io.compression.codecs"));
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);
//		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
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

	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

		private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

		int cnt = 0;
		private static String lngid = "";
		private static String rawid = "";
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String source_type = "";
		private static String provider_url = "";
		private static String title = "";
		private static String title_alt = "";
		private static String title_sub = "";
		private static String title_series = "";
		private static String keyword = "";
		private static String keyword_alt = "";
		private static String keyword_machine = "";
		private static String clc_no_1st = "";
		private static String clc_no = "";
		private static String clc_machine = "";
		private static String subject_word = "";
		private static String subject_edu = "";
		private static String subject = "";
		private static String abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
		private static String abstract_alt = "";
		private static String abstract_type = "";
		private static String abstract_alt_type = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String doc_code = "";
		private static String doc_no = "";
		private static String raw_type = "";
		private static String recv_date = "";
		private static String accept_date = "";
		private static String revision_date = "";
		private static String pub_date = "";
		private static String pub_date_alt = "";
		private static String pub_place = "";
		private static String page_cnt = "";
		private static String pdf_size = "";
		private static String fulltext_txt = "";
		private static String fulltext_addr = "";
		private static String fulltext_type = "";
		private static String column_info = "";
		private static String fund = "";
		private static String fund_id = "";
		private static String fund_alt = "";
		private static String author_id = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_raw = "";
		private static String author_alt = "";
		private static String corr_author = "";
		private static String corr_author_id = "";
		private static String email = "";
		private static String subject_dsa = "";
		private static String research_field = "";
		private static String contributor = "";
		private static String contributor_id = "";
		private static String contributor_alt = "";
		private static String author_intro = "";
		private static String organ_id = "";
		private static String organ_1st = "";
		private static String organ = "";
		private static String organ_alt = "";
		private static String preferred_organ = "";
		private static String host_organ_id = "";
		private static String organ_area = "";
		private static String journal_raw_id = "";
		private static String journal_name = "";
		private static String journal_name_alt = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String is_suppl = "";
		private static String issn = "";
		private static String eissn = "";
		private static String cnno = "";
		private static String publisher = "";
		private static String cover_path = "";
		private static String is_oa = "";
		private static String country = "";
		private static String language = "";
		private static String ref_cnt = "";
		private static String ref_id = "";
		private static String cited_id = "";
		private static String cited_cnt = "";
		private static String down_cnt = "";
		private static String is_topcited = "";
		private static String is_hotpaper = "";
		private static String year = "";
		private static String mouth = "";
		private static String day = "";
		private static String timeyear = "";
		private static String country1 = "";
		private static String language1 = "";
		private static String orc_id = "";
		private static String medlineDate = "";
		private static String pubmed_id = "";
		LinkedHashMap params = new LinkedHashMap();
		public static Map<String, String> codecountryMap = new HashMap<String, String>();
		public static Map<String, String> codelanguageMap = new HashMap<String, String>();
		public static Map<String, String> jidpublisherMap = new HashMap<String, String>();
		private static HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);

			initcountryMap(context);
			initlanguageMap(context);
			initJournalInfo(context);
		}

		private static void initcountryMap(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/pubmed/txt_file/country/code_country.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 3) {
						continue;
					}
					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					String country = vec[1].toLowerCase().trim();
					String code = vec[0].trim();

					if (country.length() < 1) {
						continue;
					}
					if (code.length() < 1) {
						continue;
					}
					String s = "[a-zA-Z]+";
					Pattern pattern = Pattern.compile(s);
					Matcher ma = pattern.matcher(country);
					if (ma.find()) {
						String realcountry = ma.group().toLowerCase().trim();
						codecountryMap.put(realcountry, code);
					}
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "countryMap: " +codecountryMap );
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "countryMapsize: " +codecountryMap.size()); publisher
			System.out.println("countryMap size:" + codecountryMap.size());
		}

		private static void initJournalInfo(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/pubmed/txt_file/journalInfo/journal_info.txt"));
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
					Type type = new TypeToken<HashMap<String, String>>() {
					}.getType();
					HashMap<String, String> mapField = gson.fromJson(line, type);
					journalInfo.put(mapField.get("nlmid"), mapField);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("journalInfo size: " + journalInfo.size());
		}

		private static void JournalInfoMap(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/pubmed/txt_file/publisher/journalinfo.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 3) {
						continue;
					}

					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					String jid = vec[0].toLowerCase().trim();

					String pub = vec[1].trim();

					if (jid.length() < 1) {
						continue;
					}
					if (pub.length() < 1) {
						continue;
					}

					jidpublisherMap.put(jid, pub);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
		}

		private static void initlanguageMap(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/pubmed/txt_file/language/code_language.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 3) {
						continue;
					}

					String[] vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					String language = vec[1].toLowerCase().trim();

					String code = vec[0].trim();
					String infodata = language + "_" + code;
//					LogMR.log2HDFS4Mapper(context, logHDFSFile, "language_code: " + infodata);
					if (language.length() < 1) {
						continue;
					}
					if (code.length() < 1) {
						continue;
					}

					codelanguageMap.put(language, code);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "languageMap: " +codelanguageMap );
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "languageMapsize: " +codelanguageMap.size());

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			{
				lngid = "";
				rawid = "";
				sub_db_id = "00038";
				product = "PUBMED";
				sub_db = "QK";
				provider = "NCBI";
				down_date = "20190226";
//				batch = "";			// 已在 setup 内初始化，无需再改
				doi = "";
				source_type = "3";
				provider_url = "";
				title = "";
				title_alt = "";
				title_sub = "";
				title_series = "";
				keyword = "";
				keyword_alt = "";
				keyword_machine = "";
				clc_no_1st = "";
				clc_no = "";
				clc_machine = "";
				subject_word = "";
				subject_edu = "";
				subject = "";
				abstract_ = "";
				abstract_alt = "";
				abstract_type = "";
				abstract_alt_type = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				doc_code = "";
				doc_no = "";
				raw_type = "";
				recv_date = "";
				accept_date = "";
				revision_date = "";
				pub_date = "";
				pub_date_alt = "";
				pub_place = "";
				page_cnt = "";
				pdf_size = "";
				fulltext_txt = "";
				fulltext_addr = "";
				fulltext_type = "";
				column_info = "";
				fund = "";
				fund_id = "";
				fund_alt = "";
				author_id = "";
				author_1st = "";
				author = "";
				author_raw = "";
				author_alt = "";
				corr_author = "";
				corr_author_id = "";
				email = "";
				subject_dsa = "";
				research_field = "";
				contributor = "";
				contributor_id = "";
				contributor_alt = "";
				author_intro = "";
				organ_id = "";
				organ_1st = "";
				organ = "";
				organ_alt = "";
				preferred_organ = "";
				host_organ_id = "";
				organ_area = "";
				journal_raw_id = "";
				journal_name = "";
				journal_name_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				is_suppl = "";
				issn = "";
				eissn = "";
				cnno = "";
				publisher = "";
				cover_path = "";
				is_oa = "";
				country = "US";
				language = "EN";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
				year = "";
				mouth = "";
				day = "";
				timeyear = "";
				country1 = "";
				language1 = "";
				orc_id = "";
				medlineDate = "";
				pubmed_id="";

			}
			String page_info_raw = "";
			String midId = "";
			String lngId = "";
			String date_err = "";
			String pmcid = "";

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(text, type);

			// 获取journal相关信息
			if (mapField.get("MedlineCitation").getAsJsonObject().has("Article")) {
				// 获取文章相关信息对象article
				JsonObject articleObjt = mapField.get("MedlineCitation").getAsJsonObject().get("Article")
						.getAsJsonObject();
				issn = GetData.getData(articleObjt, "Journal", "ISSN", "#text");
				vol = GetData.getData(articleObjt, "Journal", "JournalIssue", "Volume");
				num = GetData.getData(articleObjt, "Journal", "JournalIssue", "Issue");
				journal_name = GetData.getData(articleObjt, "Journal", "Title", "");
				title = GetData.getData(articleObjt, "ArticleTitle", "", "");

				// 处理bigjson中的语言
				language1 = GetData.getData(articleObjt, "Language", "", "").trim();
				if (language1.length() > 1) {
					if (codelanguageMap.containsKey(language1.toLowerCase())) {
						language = codelanguageMap.get(language1.toLowerCase());
					}
				}
//				if(language1.length() >1) {
//					language =language1;		
//				}
				if (articleObjt.has("Journal")) {

					JsonObject JournaldateObjt = articleObjt.get("Journal").getAsJsonObject();
					// 获取日月年，以备后续无pub_date时使用
					year = GetData.getData(JournaldateObjt, "JournalIssue", "PubDate", "Year");
					mouth = GetData.getData(JournaldateObjt, "JournalIssue", "PubDate", "Month");
					day = GetData.getData(JournaldateObjt, "JournalIssue", "PubDate", "Day");
					medlineDate = GetData.getData(JournaldateObjt, "JournalIssue", "PubDate", "MedlineDate");

					if (year.length() > 0) {
						if (mouth.length() > 1) {
							if (day.length() > 1) {
								timeyear = mouth + "," + day + "," + year;
							} else {
								timeyear = mouth + "," + year;
							}
						} else {
							timeyear = year + "0000";
						}
					} else {
						timeyear = "19000000";
					}
				}
				// 当前标签没有title,从其他标签获取
				if (title.length() < 1) {
					title = GetData.getData(articleObjt, "VernacularTitle", "", "");
				}
				page_info_raw = GetData.getData(articleObjt, "Pagination", "MedlinePgn", "");
				// 处理page
				// 匹配页码page
				Pattern pattern = Pattern.compile("(\\d{0,}-\\d{0,})|(^\\d{0,})");
				Matcher matcher = pattern.matcher(page_info_raw);
				if (matcher.find()) {
					page_info = matcher.group(0);
				}
//				journal_raw_id = GetData.getData(articleObjt, "Journal", "ISOAbbreviation", "");
				abstract_ = GetData.getData(articleObjt, "Abstract", "AbstractText", "");
				if (articleObjt.has("AuthorList")) {
					LinkedHashMap authorOrgan = new LinkedHashMap();
					JsonObject AuthorListjObj = articleObjt.get("AuthorList").getAsJsonObject();
					if (AuthorListjObj.has("Author")) {
						JsonElement jsonValueElement = AuthorListjObj.get("Author");
						authorOrgan = GetData.getAuthorOrgan(jsonValueElement, "", "", "");
						orc_id = GetData.getorcId(jsonValueElement, "", "", "");
						if (orc_id.length() > 0) {
							context.getCounter("map", "has orc_id").increment(1);
						}
						if (authorOrgan.size() > 0) {
							String[] AuthorOrganList = AuthorOrgan.numberByMap(authorOrgan);
							author = AuthorOrganList[0];
							organ = AuthorOrganList[1];
						}
					}
				}
			}
			// 获取关键字
			if (mapField.get("MedlineCitation").getAsJsonObject().has("KeywordList")) {

				// 获取关键字对象
				JsonElement keywordEle = mapField.get("MedlineCitation").getAsJsonObject().get("KeywordList");

				// 判断是否是一个Obic对象
				if (keywordEle.isJsonArray()) {
					for (JsonElement wordEle : keywordEle.getAsJsonArray()) {

						// 获取当前对象
						JsonObject currteObjc = wordEle.getAsJsonObject();
						String wordstr = GetData.getJsonValue(currteObjc, "Keyword", "#text");
						keyword += wordstr + ";";
					}
				} else {
					keyword = GetData.getJsonValue(keywordEle.getAsJsonObject(), "Keyword", "#text");
				}
				keyword = keyword.replaceAll("$;", "");
			}

			if (mapField.get("MedlineCitation").getAsJsonObject().has("MedlineJournalInfo")) {
				// 获取文章相关信息对象article
				JsonObject medlineObjt = mapField.get("MedlineCitation").getAsJsonObject().get("MedlineJournalInfo")
						.getAsJsonObject();
				if (medlineObjt.has("NlmUniqueID")) {
					// 获取期刊id
					journal_raw_id = medlineObjt.get("NlmUniqueID").getAsString();
				}
				// 获取json中的国家
				country1 = GetData.getData(medlineObjt, "Country", "", "").replaceAll("^ ", "").replaceAll(" $", "")
						.replaceAll("\\\\s*|\\t|\\r|\\n", "");
				if (country1.length() > 1) {

					// 正则国家中的字母
					String s = "[a-zA-Z]+";
					Pattern pattern = Pattern.compile(s);
					Matcher ma = pattern.matcher(country1);

					if (ma.find()) {

						String realcountry = ma.group().toLowerCase().trim();

						if (codecountryMap.containsKey(realcountry)) {
							country = codecountryMap.get(realcountry);
							context.getCounter("map", "country").increment(1);
						}
					}
				}
			}

			// 获取pubmeddata对象
			JsonObject pubmeddataObjc = mapField.get("PubmedData").getAsJsonObject();
			// 获取相关日期
			if (pubmeddataObjc.has("History")) {

				if (pubmeddataObjc.get("History").getAsJsonObject().has("PubMedPubDate")) {

					JsonElement pubmeddateEle = pubmeddataObjc.get("History").getAsJsonObject().get("PubMedPubDate");

					if (pubmeddateEle.isJsonArray()) {
						for (JsonElement dateEle : pubmeddateEle.getAsJsonArray()) {
							// 获取当前节点对象
							JsonObject currentobjc = dateEle.getAsJsonObject();
							if (currentobjc.has("@PubStatus")) {
								// 获取@PubStatus的值
								String PubStatus = currentobjc.get("@PubStatus").getAsString();

								if (PubStatus.equals("received")) {
									recv_date = GetData.getPubmedData(currentobjc);
								} else if (PubStatus.equals("revised")) {
									revision_date = GetData.getPubmedData(currentobjc);
								} else if (PubStatus.equals("accepted")) {
									accept_date = GetData.getPubmedData(currentobjc);
								} else if (PubStatus.equals("pubmed")) {
									pub_date = GetData.getPubmedData(currentobjc);
								}
							}
						}
					} else {
						date_err = "date_err";
					}
				}
			}
			if (pubmeddataObjc.getAsJsonObject().has("ArticleIdList")) {
				if (pubmeddataObjc.getAsJsonObject().get("ArticleIdList").getAsJsonObject().has("ArticleId")) {
					// 获取articleId节点对象
					JsonElement ArticleIdEle = pubmeddataObjc.getAsJsonObject().get("ArticleIdList").getAsJsonObject()
							.get("ArticleId");
					if (ArticleIdEle.isJsonArray()) {
						for (JsonElement OneIdtEle : ArticleIdEle.getAsJsonArray()) {

							// 获取当前节点对象
							JsonObject Idobjc = OneIdtEle.getAsJsonObject();

							// 获取当前节点@IdType的值
							if (Idobjc.has("@IdType")) {
								String curr_typename = Idobjc.get("@IdType").getAsString();
								if (curr_typename.equals("pubmed")) {
									rawid = GetData.getPubmedId(Idobjc, "#text");
								} else if (curr_typename.equals("doi")) {
									doi = GetData.getPubmedId(Idobjc, "#text");
								} else if (curr_typename.equals("pmc")) {
									pmcid = GetData.getPubmedId(Idobjc, "#text");
								} else if (curr_typename.equals("mid")) {
									midId = GetData.getPubmedId(Idobjc, "#text");
								}
							}
						}
					} else {
						// 获取单个的字段
						// 获取Id对象
						JsonObject Idobjc = ArticleIdEle.getAsJsonObject();
						// 获取当前节点@IdType的值
						if (Idobjc.has("@IdType")) {
							String curr_typename = Idobjc.get("@IdType").getAsString();
							if (curr_typename.equals("pubmed")) {
								rawid = GetData.getPubmedId(Idobjc, "#text");
							} else if (curr_typename.equals("doi")) {
								doi = GetData.getPubmedId(Idobjc, "#text");
							} else if (curr_typename.equals("pmc")) {
								pmcid = GetData.getPubmedId(Idobjc, "#text");
							}
						}
					}
				}
			}
			// 处理出版时间
			if (pub_date.length() > 4 && !pub_date.contains("19000000")) {
				pub_year = pub_date.substring(0, 4);
			} else {
				pub_date = "19000000";
				pub_year = "1900";
				if (pub_date.contains("19000000") && timeyear.length() > 0 && !timeyear.contains("19000000")) {
					// 重新从Journal信息从获取pub_date
					pub_date = DateTimeHelper.stdDate(timeyear);
					pub_year = pub_date.substring(0, 4);
				} else if (pub_date.contains("19000000") && timeyear.contains("19000000") && medlineDate.length() > 0) {
					// 重新从 medlineDate中取出新的年份，月份
					String Meyear = "";
					String Memonth = "";
					Matcher medineYear = Pattern.compile("(^2\\d+)|(^1\\d\\d\\d)").matcher(medlineDate);
					if (medineYear.find()) {
						Meyear = medineYear.group(1);
					}
					String s = "(Feb)|(Mar)|(Apr)|(May)|(Jun)|(Jul)|(Aug)|(Sep)|(Oct)|(Nov)|(Dec)";
					Pattern pattern = Pattern.compile(s);
					Matcher medineMonth = pattern.matcher(medlineDate);
					if (medineMonth.find()) {
						Memonth = medineMonth.group(0);
					}
					if (Meyear.length() > 1 && Memonth.length() > 1) {
						pub_date = DateTimeHelper.stdDate(Memonth + "," + Meyear);
					} else if (Meyear.length() > 1 && Memonth.length() < 1) {
						pub_date = DateTimeHelper.stdDate(Meyear + "0000");
					} else {
						pub_date = "19000000";
					}
					pub_year = pub_date.substring(0, 4);
				}
			}

			// 处理第一作者，第一机构
			if (author.contains(";")) {
				author_1st = author.split(";")[0];
			} else {
				author_1st = author;
			}
			if (organ.contains(";")) {
				organ_1st = organ.split(";")[1];
			} else {
				organ_1st = organ;
			}
			organ_1st = organ_1st.replaceAll("^\\[.*?\\]", "");
			author_1st = author_1st.replaceAll("\\[.*?\\]$", "");

			// 添加lngprovider_url
			if (rawid.length() > 0) {
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
				provider_url = "https://www.ncbi.nlm.nih.gov/pubmed/" + rawid;
			}
			// 处理orc_id
			orc_id = orc_id.replaceAll(";$", "");

			// 处理出版社publisher
			if (journalInfo.containsKey(journal_raw_id)) {
				if (journalInfo.get(journal_raw_id).containsKey("publisher")) {
					publisher = journalInfo.get(journal_raw_id).get("publisher");
					context.getCounter("map", "has_publisher").increment(1);
				}
			}

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				if (rawid.length() < 1) {
					context.getCounter("map", "Error: no title no rawid").increment(1);
				}
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}

			if (rawid.length() < 1) {
				context.getCounter("map", "Error: no rawid").increment(1);
				return;
			}
			if (journal_raw_id.length() < 1) {
				context.getCounter("map", "Error: no jid").increment(1);

			}
			if (date_err.length() > 1) {
				context.getCounter("map", "has: date_err").increment(1);
			}
			if (journal_name.length() < 1) {
				context.getCounter("map", "Error: no jname").increment(1);

			}
			if (!country.contains("US")) {
				context.getCounter("map", "Error: no US").increment(1);
			}
			if (!language.contains("EN")) {
				context.getCounter("map", "Error: no EN").increment(1);

			}
			if (issn.length() < 1) {
				context.getCounter("map", "Error: no issn").increment(1);

			}
			// 正则字母
			Matcher numcheck = Pattern.compile("([A-Za-z]+)").matcher(num);

			if (numcheck.find()) {

				String checkStr = numcheck.group(1).toLowerCase().trim();
				if (checkStr.contains("sup")) {
					is_suppl = "1";

				} else if (checkStr.startsWith("su")) {
					is_suppl = "1";

				}
			}
			Matcher volcheck = Pattern.compile("([A-Za-z]+)").matcher(vol);

			if (volcheck.find()) {

				String volcheckStr = volcheck.group(1).toLowerCase().trim();
				if (volcheckStr.contains("sup")) {
					is_suppl = "1";

				} else if (volcheckStr.startsWith("su")) {
					is_suppl = "1";

				}
			}
			if (num.contains("Sup") || num.contains("sup") || num.contains("u") || num.contains("p")
					|| vol.contains("Sup") || vol.contains("p") || vol.contains("u") || vol.contains("sup")) {
				is_suppl = "1";
				context.getCounter("map", "is_suppl: yes").increment(1);
			}
			pubmed_id = rawid;
			XXXXObject xObj = new XXXXObject();
			{
				xObj.data.put("lngid", lngid);
				xObj.data.put("rawid", rawid);
				xObj.data.put("sub_db_id", sub_db_id);
				xObj.data.put("product", product);
				xObj.data.put("sub_db", sub_db);
				xObj.data.put("provider", provider);
				xObj.data.put("down_date", down_date);
				xObj.data.put("batch", batch);
				xObj.data.put("doi", doi);
				xObj.data.put("source_type", source_type);
				xObj.data.put("provider_url", provider_url);
				xObj.data.put("title", title);
				xObj.data.put("title_alt", title_alt);
				xObj.data.put("title_sub", title_sub);
				xObj.data.put("title_series", title_series);
				xObj.data.put("keyword", keyword);
				xObj.data.put("keyword_alt", keyword_alt);
				xObj.data.put("keyword_machine", keyword_machine);
				xObj.data.put("clc_no_1st", clc_no_1st);
				xObj.data.put("clc_no", clc_no);
				xObj.data.put("clc_machine", clc_machine);
				xObj.data.put("subject_word", subject_word);
				xObj.data.put("subject_edu", subject_edu);
				xObj.data.put("subject", subject);
				xObj.data.put("abstract", abstract_);
				xObj.data.put("abstract_alt", abstract_alt);
				xObj.data.put("abstract_type", abstract_type);
				xObj.data.put("abstract_alt_type", abstract_alt_type);
				xObj.data.put("page_info", page_info);
				xObj.data.put("begin_page", begin_page);
				xObj.data.put("end_page", end_page);
				xObj.data.put("jump_page", jump_page);
				xObj.data.put("doc_code", doc_code);
				xObj.data.put("doc_no", doc_no);
				xObj.data.put("raw_type", raw_type);
				xObj.data.put("recv_date", recv_date);
				xObj.data.put("accept_date", accept_date);
				xObj.data.put("revision_date", revision_date);
				xObj.data.put("pub_date", pub_date);
				xObj.data.put("pub_date_alt", pub_date_alt);
				xObj.data.put("pub_place", pub_place);
				xObj.data.put("page_cnt", page_cnt);
				xObj.data.put("pdf_size", pdf_size);
				xObj.data.put("fulltext_txt", fulltext_txt);
				xObj.data.put("fulltext_addr", fulltext_addr);
				xObj.data.put("fulltext_type", fulltext_type);
				xObj.data.put("column_info", column_info);
				xObj.data.put("fund", fund);
				xObj.data.put("fund_id", fund_id);
				xObj.data.put("fund_alt", fund_alt);
				xObj.data.put("author_id", author_id);
				xObj.data.put("author_1st", author_1st);
				xObj.data.put("author", author);
				xObj.data.put("author_raw", author_raw);
				xObj.data.put("author_alt", author_alt);
				xObj.data.put("corr_author", corr_author);
				xObj.data.put("corr_author_id", corr_author_id);
				xObj.data.put("email", email);
				xObj.data.put("subject_dsa", subject_dsa);
				xObj.data.put("research_field", research_field);
				xObj.data.put("contributor", contributor);
				xObj.data.put("contributor_id", contributor_id);
				xObj.data.put("contributor_alt", contributor_alt);
				xObj.data.put("author_intro", author_intro);
				xObj.data.put("organ_id", organ_id);
				xObj.data.put("organ_1st", organ_1st);
				xObj.data.put("organ", organ);
				xObj.data.put("organ_alt", organ_alt);
				xObj.data.put("preferred_organ", preferred_organ);
				xObj.data.put("host_organ_id", host_organ_id);
				xObj.data.put("organ_area", organ_area);
				xObj.data.put("journal_raw_id", journal_raw_id);
				xObj.data.put("journal_name", journal_name);
				xObj.data.put("journal_name_alt", journal_name_alt);
				xObj.data.put("pub_year", pub_year);
				xObj.data.put("vol", vol);
				xObj.data.put("num", num);
				xObj.data.put("is_suppl", is_suppl);
				xObj.data.put("issn", issn);
				xObj.data.put("eissn", eissn);
				xObj.data.put("cnno", cnno);
				xObj.data.put("publisher", publisher);
				xObj.data.put("cover_path", cover_path);
				xObj.data.put("is_oa", is_oa);
				xObj.data.put("country", country);
				xObj.data.put("language", language);
				xObj.data.put("ref_cnt", ref_cnt);
				xObj.data.put("ref_id", ref_id);
				xObj.data.put("cited_id", cited_id);
				xObj.data.put("cited_cnt", cited_cnt);
				xObj.data.put("down_cnt", down_cnt);
				xObj.data.put("is_topcited", is_topcited);
				xObj.data.put("is_hotpaper", is_hotpaper);
				xObj.data.put("orc_id", orc_id);
				xObj.data.put("pmcid", pmcid);
				xObj.data.put("pubmed_id", pubmed_id);
			}
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);
		}
	}
}
