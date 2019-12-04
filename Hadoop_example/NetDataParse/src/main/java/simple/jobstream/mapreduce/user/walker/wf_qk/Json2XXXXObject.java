package simple.jobstream.mapreduce.user.walker.wf_qk;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Comparator;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.mockito.internal.matchers.EndsWith;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.GsonHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";

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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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
		private static int cnt = 0;
		private static String src_db = "null";
		private static String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		private static Context s_context = null;

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
		private static String pv_cnt = "";
		private static String is_topcited = "";
		private static String is_hotpaper = "";

		public void setup(Context context) throws IOException, InterruptedException {
			inputHdfsPath = context.getConfiguration().get("inputHdfsPath");
			outputHdfsPath = context.getConfiguration().get("outputHdfsPath");
			batch = context.getConfiguration().get("batch");

			s_context = context;
			initMapPykmLanguage(context);
		}

		// pykm - 语言
		private static Map<String, String> mapLanguage = new HashMap<String, String>();

		private static void initMapPykmLanguage(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/_rel_file/pykm_language.txt"));
			BufferedReader in = null;
			String line;
			String[] vec = null;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					vec = line.split("★");
					if (vec.length != 2) {
						continue;
					}
					mapLanguage.put(vec[0].trim(), vec[1].trim());
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("mapLanguage size: " + mapLanguage.size());
		}

		// 获取语言
		private static String getLanguage(String pykm) {
			String language = "ZH";
			if (mapLanguage.containsKey(pykm)) {
				if (mapLanguage.get(pykm).equals("eng")) {
					language = "EN";
				}
			}

			return language;
		}

		// 清理class，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanClass(String text) {
			text = text.replace("{", "").replace("}", "").trim();

			return text;
		}

		// 解析作者和机构，得到对应关系
		private void parseAuthorOrgan_with_op(JsonObject articleJsonObject) {
			// authors_name, authors_unit, authorsandunit
			// zhxxgb, 2018, 5 适合做样例

			String[] vec = null;
			LinkedHashMap<String, String> mapAuthorOrgan = new LinkedHashMap<String, String>();

			JsonElement opElement = articleJsonObject.get("op");
			// op 可能不存在，例如 lydk201801009
			if (null == opElement) {
				s_context.getCounter("map", "null opElement ").increment(1);
				return;
			}

			JsonElement perioAuthorsElement = opElement.getAsJsonObject().get("perioAuthors");

			if (null == perioAuthorsElement) {
				s_context.getCounter("map", "null perioAuthorsElement ").increment(1);
				return;
			}

			if (perioAuthorsElement.isJsonArray()) {
				Gson gson = new Gson();
				Type type = new TypeToken<ArrayList<HashMap<String, String>>>() {
				}.getType();
				ArrayList<HashMap<String, String>> arrayList = gson.fromJson(perioAuthorsElement.toString(), type);

				Collections.sort(arrayList, new Comparator<HashMap<String, String>>() {
					@Override
					public int compare(HashMap<String, String> map1, HashMap<String, String> map2) {
						if (Integer.parseInt(map1.get("authors_seq")) > Integer.parseInt(map2.get("authors_seq"))) {
							return 1;
						} else {
							return -1;
						}
					}
				});

				for (HashMap<String, String> hashMap : arrayList) {
					String authors_name = "";
					String unit_name = "";
					String scholar_id = "";
					String org_id = "";
					if (hashMap.containsKey("authors_name")) {
						// 去掉中括号，避免排序号出错
						authors_name = hashMap.get("authors_name").replace('[', ' ').replace(']', ' ').trim();
						if (authors_name.equalsIgnoreCase("null")) {
							authors_name = "";
						}
					}
					if (hashMap.containsKey("unit_name")) {
						// 替换单机构分号，避免排序号出错
						unit_name = hashMap.get("unit_name").replace(';', ',').trim();
						if (unit_name.equalsIgnoreCase("null")) {
							unit_name = "";
						}
					}
					if (hashMap.containsKey("scholar_id")) {
						scholar_id = hashMap.get("scholar_id");
					}
					if (hashMap.containsKey("org_id")) {
						org_id = hashMap.get("org_id");
					}

					if (authors_name.length() < 1) {
						continue;
					}
					mapAuthorOrgan.put(authors_name, unit_name);

					String author_id_item = "";
					String organ_id_item = "";
					if (!scholar_id.equalsIgnoreCase("null")) {
						author_id_item = scholar_id + "@" + authors_name;
					}
					if (!org_id.equalsIgnoreCase("null")) {
						organ_id_item = org_id + "@" + unit_name;
					}
					if (author_id_item.length() > 0) {
						author_id += author_id_item + ";";
					}
					if ((organ_id_item.length() > 0) && (organ_id.indexOf(organ_id_item) < 0)) {
						organ_id += organ_id_item + ";";
					}
				}

				try {
					vec = AuthorOrgan.numberByMap(mapAuthorOrgan);
				} catch (Exception e) {
					s_context.getCounter("map", "Exception AuthorOrgan").increment(1);
					LogMR.log2HDFS4Mapper(s_context, logHDFSFile, "************ao:" + rawid + ", "
							+ mapAuthorOrgan.toString() + ", " + articleJsonObject.toString());
					return;
				}

				author = vec[0];
				organ = vec[1];

			}

			author = StringHelper.cleanSemicolon(author);
			organ = StringHelper.cleanSemicolon(organ);
			author_id = StringHelper.cleanSemicolon(author_id);
			organ_id = StringHelper.cleanSemicolon(organ_id);

			author_alt = GsonHelper.getJsonValue(articleJsonObject, "trans_authors");

			if ((author.length() < 1) && (author_alt.length() > 0)) {
				author = author_alt;
				author_alt = "";
			}
			if ((organ.length() < 1) && (organ_alt.length() > 0)) {
				organ = organ_alt;
				organ_alt = "";
			}

			vec = author.split(";");
			if (vec.length > 0) {
				author_1st = vec[0].trim();
				author_1st = author_1st.replaceAll("\\[.*?\\]$", ""); // 去掉后面的标号
			}
			vec = organ.split(";");
			if (vec.length > 0) {
				organ_1st = vec[0].trim();
				organ_1st = organ_1st.replaceAll("^\\[.*?\\]", ""); // 去掉前面的标号
			}
		}

		// 解析作者和机构，得到对应关系
		private void parseAuthorOrgan(JsonObject articleJsonObject) {
			// authors_name, authors_unit, authorsandunit
			// zhxxgb, 2018, 5 适合做样例

			HashMap<String, String> mapOrganNo = new HashMap<String, String>();
			HashMap<String, String> mapAuthorOrgan = new HashMap<String, String>();

			JsonElement authorsandunitElement = articleJsonObject.get("authorsandunit");
			JsonElement authorsElement = articleJsonObject.get("authors_name");
			JsonElement authorsunitElement = articleJsonObject.get("authors_unit");

			if ((null != authorsandunitElement) && authorsandunitElement.isJsonArray() && 
					(null != authorsElement) && authorsElement.isJsonArray() && 
					(null != authorsunitElement) && authorsunitElement.isJsonArray()) {
				for (JsonElement jEle : authorsandunitElement.getAsJsonArray()) {
					String line = jEle.getAsString().trim();
					int idx = line.indexOf('≡');
					if (idx > 0) {
						String author = line.substring(0, idx).trim();
						String unit = line.substring(idx + 1).trim();
						mapAuthorOrgan.put(author, unit);
					}
				}

				int idx = 0;
				for (JsonElement jEle : authorsElement.getAsJsonArray()) {
					String author = jEle.getAsString().trim();
					author += author;
					if (mapAuthorOrgan.containsKey(author)) {
						if (!mapOrganNo.containsKey(mapAuthorOrgan.get(author))) {
							++idx;
							organ += "[" + idx + "]" + mapAuthorOrgan.get(author) + ";";
							mapOrganNo.put(mapAuthorOrgan.get(author), "[" + idx + "]");
						}

						author += mapOrganNo.get(mapAuthorOrgan.get(author));
					}
					author += ";";
				}
			} 
			if (author.length() < 1) {
				author = GsonHelper.getJsonValue(articleJsonObject, "authors_name");
			}
			if (organ.length() < 1) {
				organ = GsonHelper.getJsonValue(articleJsonObject, "authors_unit");
			}

			author = StringHelper.cleanSemicolon(author);
			organ = StringHelper.cleanSemicolon(organ);

			author_alt = GsonHelper.getJsonValue(articleJsonObject, "trans_authors");

			if ((author.length() < 1) && (author_alt.length() > 0)) {
				author = author_alt;
				author_alt = "";
			}
			if ((organ.length() < 1) && (organ_alt.length() > 0)) {
				organ = organ_alt;
				organ_alt = "";
			}

			String[] vec = author.split(";");
			if (vec.length > 0) {
				author_1st = vec[0].trim();
				author_1st = author_1st.replaceAll("\\[.*?\\]$", ""); // 去掉后面的标号
			}
			vec = organ.split(";");
			if (vec.length > 0) {
				organ_1st = vec[0].trim();
				organ_1st = organ_1st.replaceAll("^\\[.*?\\]", ""); // 去掉前面的标号
			}
		}

		// 获取机构地区
		private void parseOrganArea(JsonObject articleJsonObject) {
			JsonElement authareaElement = articleJsonObject.get("auth_area"); // 省
			JsonElement authcityElement = articleJsonObject.get("auth_city"); // 市
			JsonElement authcountyElement = articleJsonObject.get("auth_county"); // 县

			ArrayList<String> areaList = GsonHelper.jsonElement2Array(authareaElement);
			ArrayList<String> cityList = GsonHelper.jsonElement2Array(authcityElement);
			ArrayList<String> countryList = GsonHelper.jsonElement2Array(authcountyElement);

			// 县存在
			if (countryList.size() > 0) {
				// 最小个数
				int minLen = areaList.size();
				if (cityList.size() < minLen) {
					minLen = cityList.size();
				}
				if (countryList.size() < minLen) {
					minLen = countryList.size();
				}
				// 最大个数
				int maxLen = areaList.size();
				if (cityList.size() > maxLen) {
					maxLen = cityList.size();
				}
				if (countryList.size() > maxLen) {
					maxLen = countryList.size();
				}

				if (minLen > 0) {
					for (int i = 0; i < minLen; i++) {
						String item = areaList.get(i) + "," + cityList.get(i) + "," + countryList.get(i) + ";";
						if (organ_area.indexOf(item) < 0) {
							organ_area += item;
						}
					}
				}
			} else { // 县不存在
				// 最小个数
				int minLen = areaList.size();
				if (cityList.size() < minLen) {
					minLen = cityList.size();
				}

				// 最大个数
				int maxLen = areaList.size();
				if (cityList.size() > maxLen) {
					maxLen = cityList.size();
				}

				if (minLen > 0) {
					for (int i = 0; i < minLen; i++) {
						String item = areaList.get(i) + "," + cityList.get(i) + ";";
						if (organ_area.indexOf(item) < 0) {
							organ_area += item;
						}
					}
				}
			}

			// organ_area 没获得值
			if (organ_area.length() < 1) {
				if (cityList.size() > 0) { // 市
					for (int i = 0; i < cityList.size(); i++) {
						String item = cityList.get(i) + ";";
						if (organ_area.indexOf(item) < 0) {
							organ_area += item;
						}
					}
				} else if (areaList.size() > 0) { // 省
					for (int i = 0; i < areaList.size(); i++) {
						String item = areaList.get(i) + ";";
						if (organ_area.indexOf(item) < 0) {
							organ_area += item;
						}
					}
				} else if (countryList.size() > 0) { // 县
					for (int i = 0; i < countryList.size(); i++) {
						String item = countryList.get(i) + ";";
						if (organ_area.indexOf(item) < 0) {
							organ_area += item;
						}
					}
				}
			}

			// 将多个连续逗号换成单个逗号
			organ_area = organ_area.replaceAll(",+", ",");
			organ_area = organ_area.replaceAll(",;", ";");
			organ_area = StringHelper.cleanSemicolon(organ_area);
		}

		// 拆分起始页、结束页、跳转页
		private void parsePageInfo(String line) {
			int idx = line.indexOf(',');
			if (idx > 0) {
				jump_page = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉加号及以后部分
			}
			idx = line.indexOf('-');
			if (idx > 0) {
				end_page = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉减号及以后部分
			}
			begin_page = line.trim();
			if (end_page.length() < 1) {
				end_page = begin_page;
			}
		}

		// 解析所有日期相关信息
		private void parseDate(JsonObject articleJsonObject) {
			// "2017-04-28 00:00:00.0"
			String orig_pub_date = GsonHelper.getJsonValue(articleJsonObject, "orig_pub_date");
			// 在线出版日期 1494864000000 '2017-05-16 00:00:00'
			String abst_webdate = GsonHelper.getJsonValue(articleJsonObject, "abst_webdate");

			String[] vec = orig_pub_date.split(" ");
			if (vec.length == 2) {
				pub_date = vec[0].replace("-", "").trim();
			}

			SimpleDateFormat sdf = new SimpleDateFormat("yyyyMMdd");
			pub_date_alt = sdf.format(new Date(Long.valueOf(abst_webdate)));
		}

		public boolean parseArticle(JsonElement articleJsonElement) {
			{
				lngid = "";
				rawid = "";
				sub_db_id = "00004";
				product = "WANFANG";
				sub_db = "CSPD";
				provider = "WANFANG";
//				down_date = "";		// 已在外层（期）初始化，无需再改
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
				fulltext_type = "pdf";
				column_info = "";
				fund = "";
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
				country = "";
				language = "";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				pv_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
			}

			JsonObject articleJsonObject = articleJsonElement.getAsJsonObject();

			rawid = GsonHelper.getJsonValue(articleJsonObject, "article_id");
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			provider_url = "http://www.wanfangdata.com.cn/details/detail.do?_type=perio&id=" + rawid;
			journal_raw_id = GsonHelper.getJsonValue(articleJsonObject, "perio_id");
			issn = GsonHelper.getJsonValue(articleJsonObject, "issn");
			cnno = GsonHelper.getJsonValue(articleJsonObject, "cn");
			title = GsonHelper.getJsonValue(articleJsonObject, "title");
			if ((title.indexOf("<em>") > -1) && (title.indexOf("</em>") > -1)) {
				title = title.replaceAll("<em>", "").replaceAll("</em>", "");
			}
			title_alt = GsonHelper.getJsonValue(articleJsonObject, "trans_title");
			// 填充主标题
			if ((title.length() < 1) && (title_alt.length() > 0)) {
				title = title_alt;
				title_alt = "";
			}
			abstract_ = GsonHelper.getJsonValue(articleJsonObject, "summary");
			abstract_alt = GsonHelper.getJsonValue(articleJsonObject, "trans_abstract");
			doi = GsonHelper.getJsonValue(articleJsonObject, "doi");
			parseAuthorOrgan(articleJsonObject); // 解析所有作者机构信息
//			parseAuthorOrgan_with_op(articleJsonObject);
			parseOrganArea(articleJsonObject);
			parseDate(articleJsonObject); // 解析所有日期相关信息
			journal_name = GsonHelper.getJsonValue(articleJsonObject, "perio_title");
			journal_name_alt = GsonHelper.getJsonValue(articleJsonObject, "perio_title_en");
			pub_year = GsonHelper.getJsonValue(articleJsonObject, "publish_year");
			vol = GsonHelper.getJsonValue(articleJsonObject, "volume");
			num = GsonHelper.getJsonValue(articleJsonObject, "issue_num");
			clc_no = GsonHelper.getJsonValue(articleJsonObject, "orig_classcode").replace('%', ';');
			if (clc_no.length() > 0) {
				String[] vec = clc_no.split(";");
				if (vec.length > 0) {
					clc_no_1st = vec[0].trim();
				}
			}
			clc_machine = GsonHelper.getJsonValue(articleJsonObject, "auto_classcode"); // 机标分类号
			keyword = GsonHelper.getJsonValue(articleJsonObject, "keywords");
			keyword_alt = GsonHelper.getJsonValue(articleJsonObject, "trans_keys");
			fund = GsonHelper.getJsonValue(articleJsonObject, "fund_info").replace('%', ';');
			page_info = GsonHelper.getJsonValue(articleJsonObject, "page_range");
			parsePageInfo(page_info);
			page_cnt = GsonHelper.getJsonValue(articleJsonObject, "page_cnt");
			String download_num = GsonHelper.getJsonValue(articleJsonObject, "download_num");
			if (!StringHelper.isNumeric(download_num)) {
				download_num = "0";
			}
			down_cnt = download_num + "@" + down_date;
			String abstract_reading_num = GsonHelper.getJsonValue(articleJsonObject, "abstract_reading_num");
			if (!StringHelper.isNumeric(abstract_reading_num)) {
				abstract_reading_num = "0";
			}
			pv_cnt = abstract_reading_num + "@" + down_date;
//			ref_cnt = GsonHelper.getJsonValue(articleJsonObject, "refdoc_cnt");		// 与详情页不一致，靠不住
//			cited_cnt = GsonHelper.getJsonValue(articleJsonObject, "cited_cnt");		// 与详情页不一致，靠不住
			column_info = GsonHelper.getJsonValue(articleJsonObject, "column_name");
			doc_code = GsonHelper.getJsonValue(articleJsonObject, "literature_code");
			doc_no = GsonHelper.getJsonValue(articleJsonObject, "doc_num");

			country = "CN";
			language = getLanguage(journal_raw_id);

			src_db = GsonHelper.getJsonValue(articleJsonObject, "source_db");

			{
//				System.out.println("lngid: " + lngid);
//				System.out.println("rawid: " + rawid);
//				System.out.println("sub_db_id: " + sub_db_id);
//				System.out.println("product: " + product);
//				System.out.println("sub_db: " + sub_db);
//				System.out.println("provider: " + provider);
//				System.out.println("down_date: " + down_date);
//				System.out.println("batch: " + batch);
//				System.out.println("doi: " + doi);
//				System.out.println("source_type: " + source_type);
//				System.out.println("provider_url: " + provider_url);
//				System.out.println("title: " + title);
//				System.out.println("title_alt: " + title_alt);
//				System.out.println("title_sub: " + title_sub);
//				System.out.println("title_series: " + title_series);
//				System.out.println("keyword: " + keyword);
//				System.out.println("keyword_alt: " + keyword_alt);
//				System.out.println("keyword_machine: " + keyword_machine);
//				System.out.println("clc_no_1st: " + clc_no_1st);
//				System.out.println("clc_no: " + clc_no);
//				System.out.println("clc_machine: " + clc_machine);
//				System.out.println("subject_word: " + subject_word);
//				System.out.println("subject_edu: " + subject_edu);
//				System.out.println("subject: " + subject);
//				System.out.println("abstract_: " + abstract_);
//				System.out.println("abstract_alt: " + abstract_alt);
//				System.out.println("abstract_type: " + abstract_type);
//				System.out.println("abstract_alt_type: " + abstract_alt_type);
//				System.out.println("page_info: " + page_info);
//				System.out.println("begin_page: " + begin_page);
//				System.out.println("end_page: " + end_page);
//				System.out.println("jump_page: " + jump_page);
//				System.out.println("doc_code: " + doc_code);
//				System.out.println("doc_no: " + doc_no);
//				System.out.println("raw_type: " + raw_type);
//				System.out.println("recv_date: " + recv_date);
//				System.out.println("accept_date: " + accept_date);
//				System.out.println("revision_date: " + revision_date);
//				System.out.println("pub_date: " + pub_date);
//				System.out.println("pub_date_alt: " + pub_date_alt);
//				System.out.println("pub_place: " + pub_place);
//				System.out.println("page_cnt: " + page_cnt);
//				System.out.println("pdf_size: " + pdf_size);
//				System.out.println("fulltext_txt: " + fulltext_txt);
//				System.out.println("fulltext_addr: " + fulltext_addr);
//				System.out.println("fulltext_type: " + fulltext_type);
//				System.out.println("column_info: " + column_info);
//				System.out.println("fund: " + fund);
//				System.out.println("fund_alt: " + fund_alt);
//				System.out.println("author_id: " + author_id);
//				System.out.println("author_1st: " + author_1st);
//				System.out.println("author: " + author);
//				System.out.println("author_raw: " + author_raw);
//				System.out.println("author_alt: " + author_alt);
//				System.out.println("corr_author: " + corr_author);
//				System.out.println("corr_author_id: " + corr_author_id);
//				System.out.println("email: " + email);
//				System.out.println("subject_dsa: " + subject_dsa);
//				System.out.println("research_field: " + research_field);
//				System.out.println("contributor: " + contributor);
//				System.out.println("contributor_id: " + contributor_id);
//				System.out.println("contributor_alt: " + contributor_alt);
//				System.out.println("author_intro: " + author_intro);
//				System.out.println("organ_id: " + organ_id);
//				System.out.println("organ_1st: " + organ_1st);
//				System.out.println("organ: " + organ);
//				System.out.println("organ_alt: " + organ_alt);
//				System.out.println("preferred_organ: " + preferred_organ);
//				System.out.println("host_organ_id: " + host_organ_id);
//				System.out.println("organ_area: " + organ_area);
//				System.out.println("journal_raw_id: " + journal_raw_id);
//				System.out.println("journal_name: " + journal_name);
//				System.out.println("journal_name_alt: " + journal_name_alt);
//				System.out.println("pub_year: " + pub_year);
//				System.out.println("vol: " + vol);
//				System.out.println("num: " + num);
//				System.out.println("is_suppl: " + is_suppl);
//				System.out.println("issn: " + issn);
//				System.out.println("eissn: " + eissn);
//				System.out.println("cnno: " + cnno);
//				System.out.println("publisher: " + publisher); 
//				System.out.println("cover_path: " + cover_path); 
//				System.out.println("is_oa: " + is_oa);
//				System.out.println("country: " + country);
//				System.out.println("language: " + language); 
//				System.out.println("ref_cnt: " + ref_cnt);
//				System.out.println("ref_id: " + ref_id); 
//				System.out.println("cited_id: " + cited_id); 
//				System.out.println("cited_cnt: " + cited_cnt);
//				System.out.println("down_cnt: " + down_cnt); 
//				System.out.println("is_topcited: " + is_topcited); 
//				System.out.println("is_hotpaper: " + is_hotpaper);
//				System.out.println("**************************************\n");
			} //

			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "inCount " + inputHdfsPath).increment(1);

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String issueLine = value.toString().trim();
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(issueLine, type);
			down_date = mapField.get("down_date").getAsString();

			JsonArray pageRowJsonArray = mapField.get("pageRow").getAsJsonArray();

			for (JsonElement articleJsonElement : pageRowJsonArray) {
				context.getCounter("map", "articleJsonElement ").increment(1);
				parseArticle(articleJsonElement); // 解析一行json
				if ((title.length() < 1) && (title_alt.length() < 1)) {
					context.getCounter("map", "error: no title").increment(1);
//					log2HDFSForMapper(context, "error: no title: " + rawid);
					LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
					continue;
				}

				context.getCounter("map", "src_db." + src_db).increment(1);

				// 多个时以分号分隔：WF;CQVIP;CNKI;DG;WSJS
				HashSet<String> dbSet = new HashSet<String>();
				for (String item : src_db.split(";")) {
					item = item.toUpperCase().trim();
					if (item.length() < 1) {
						continue;
					}
					dbSet.add(item);
				}

				// 中信所/ISTIC, 中国科学技术信息研究所, Institute of Scientific and Technical Information of
				// China
				if ((!dbSet.contains("WF")) && (!dbSet.contains("ISTIC"))) {
					continue;
				}

				if (rawid.startsWith("pre_")) {
					context.getCounter("map", "ignore pre_").increment(1);
					continue;
				}
				
				if ((title.length() < 1) && (title_alt.length() < 1)) {
					context.getCounter("map", "error: no title").increment(1);
					LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
					return;
				}
				{// 统计作者机构为空的情况
					if (author.length() < 1) {
						context.getCounter("map", "warning: no author").increment(1);
					}
					
					if (organ.length() < 1) {
						context.getCounter("map", "warning: no organ").increment(1);
					}
					
					if ((author.length() < 1) && (organ.length() < 1)) {
						context.getCounter("map", "warning: no author organ").increment(1);
						LogMR.log2HDFS4Mapper(context, logHDFSFile, "no author organ: " + rawid);
					}
				}

				XXXXObject xObj = new XXXXObject();
				{
					xObj.data.put("src_db", src_db);

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
					xObj.data.put("pv_cnt", pv_cnt);
					xObj.data.put("is_topcited", is_topcited);
					xObj.data.put("is_hotpaper", is_hotpaper);
				}
				context.getCounter("map", "outCount " + outputHdfsPath).increment(1);

				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(rawid), new BytesWritable(bytes));
			}
		}
	}

}
