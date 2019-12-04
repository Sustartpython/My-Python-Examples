package simple.jobstream.mapreduce.user.walker.cnki_qk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

/**
 * <p>
 * Description: 针对kns和海外版合版的解析
 * http://oversea.cnki.net/kcms/detail/detail.aspx?filename=KYGL2018S1010&dbcode=CJFD
 * </p>
 * 
 * @author qiuhongyang 2018年11月6日 下午1:36:17
 */
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
		private static String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";
		private static String has_oversea = "0"; // 是否有海外版的数据，是否用到海外版数据

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

		// 期刊信息
		private HashMap<String, HashMap<String, String>> journalInfo = new HashMap<String, HashMap<String, String>>();
		// 期刊分级
		private HashMap<String, String> journalLevel = new HashMap<String, String>();

		// 初始化期刊信息
		private void initJournalInfo(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/qk/_rel_file/journal_info.txt"));
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
					journalInfo.put(mapField.get("pykm"), mapField);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("journalInfo size: " + journalInfo.size());
		}

		// 初始化期刊分级
		private void initJournalLevel(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/qk/_rel_file/期刊分级.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					String[] vec = line.split("\t");
					if (vec.length != 2) {
						continue;
					}
					journalLevel.put(vec[0], vec[1]);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("journalLevel size: " + journalLevel.size());
		}

		public void setup(Context context) throws IOException, InterruptedException {

			batch = context.getConfiguration().get("batch");

//			initMapCountryLanguage(context);
			initJournalInfo(context);
			initJournalLevel(context);
		}

		// 获取国家
		private String getCountry(String pykm) {
			String country = "CN";
			if (journalInfo.containsKey(pykm)) {
				if (journalInfo.get(pykm).get("出版地").equals("美国")) {
					country = "US";
				}
			}

			return country;
		}

		// 获取语言
		private String getLanguage(String pykm) {
			String language = "ZH";
			if (journalInfo.containsKey(pykm)) {
				if (journalInfo.get(pykm).get("语种").equals("英文")) {
					language = "EN";
				}
			}

			return language;
		}

		// 获取国内统一刊号
		private String getCNNO(String pykm) {
			String cnno = "";
			if (journalInfo.containsKey(pykm)) {
				cnno = journalInfo.get(pykm).get("CN");
			}

			return cnno;
		}

		/**
		 * <p>
		 * Description: 解析 kns 详情页
		 * </p>
		 * 
		 * @author qiuhongyang 2018年11月18日 上午11:25:02
		 * @param detailHtml
		 */
		public void parseKNS(String detailHtml) {
			Document doc = Jsoup.parse(detailHtml);

			/**************************** begin title ****************************/
			{
				Element eleTitle = doc.select("div.wxTitle > h2.title").first();
				if (eleTitle != null) {
					// 去掉简版详情页的“简”字
					Element markJ = eleTitle.select("b.markJ").first();
					if (markJ != null) {
						markJ.remove();
					}
					title = eleTitle.text().trim();
				}

				if (title.length() < 1) {
					title = doc.title().trim();
					if (title.endsWith("- 中国知网")) {
						title = title.substring(0, title.length() - "- 中国知网".length()).trim();
					}
				}

//				System.out.println("title:" + title);
			}
			/**************************** end title ****************************/

			/**************************** begin author ****************************/
			{
				for (Element ele : doc.select("div.wxTitle > div.author > span > a")) {
					author += ele.text().trim() + ";";
					String line = ele.attr("onclick").trim();
//					System.out.println("line:" + line);
					Pattern pattern = Pattern.compile("^.*','(\\d+)'\\);$");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						author_id += matcher.group(1) + "@" + ele.text().trim() + ";";
					}
				}
				author = StringHelper.cleanSemicolon(author);
				author_id = StringHelper.cleanSemicolon(author_id);
				if (author.length() > 0) {
					String[] vec = author.split(";");
					if (vec.length > 0) {
						author_1st = vec[0].trim();
					}
				}

//				System.out.println("author:" + author);
//				System.out.println("author_id:" + author_id);
//				System.out.println("author_1st:" + author_1st);
			}
			/**************************** end author ****************************/

			/**************************** begin organ ****************************/
			{
				for (Element ele : doc.select("div.wxTitle > div.orgn > span > a")) {
					organ += ele.text().trim() + ";";
					String line = ele.attr("onclick").trim();
//					System.out.println("line:" + line);
					Pattern pattern = Pattern.compile("^.*','(\\d+)'\\)$");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						organ_id += matcher.group(1) + "@" + ele.text().trim() + ";";
					}
				}

				// 去掉最后的分号
				organ = StringHelper.cleanSemicolon(organ);
				organ_id = StringHelper.cleanSemicolon(organ_id);
				if (organ.length() > 0) {
					String[] vec = organ.split(";");
					if (vec.length > 0) {
						organ_1st = vec[0].trim();
					}
				}

//				System.out.println("organ:" + organ);
//				System.out.println("organ_id:" + organ_id);
//				System.out.println("organ_1st:" + organ_1st);
			}
			/**************************** end organ ****************************/

			/**************************** begin abstract_ ****************************/
			{
				Element ele = doc.select("span#ChDivSummary").first();

				if (ele != null) {
					abstract_ = ele.text().trim();
					if (abstract_.startsWith("<正>")) {
						abstract_ = abstract_.substring("<正>".length());
						abstract_type = "第一段";
					}
				}

				// System.out.println("abstract_:" + abstract_);
			}
			/**************************** end abstract_ ****************************/

			/**************************** begin fund ****************************/
			{

				for (Element ele : doc.select("div.wxBaseinfo > p:contains(基金：) > a")) {
					String fund_item = ele.text().replaceAll("[；\\s]+$", "");
					fund += fund_item + ";";
					String line = ele.attr("onclick").trim();
					final Pattern pattern = Pattern.compile("'([^']+?)'\\);");
					final Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						fund_id += matcher.group(1) + "@" + fund_item + ";";
					}
				}

				// 去掉最后的分号
				fund = StringHelper.cleanSemicolon(fund);
				fund_id = StringHelper.cleanSemicolon(fund_id);

//				System.out.println("fund:" + fund);
//				System.out.println("fund_id:" + fund_id);
			}
			/**************************** end fund ****************************/

			/**************************** begin keyword ****************************/
			{
				for (Element ele : doc.select("div.wxBaseinfo > p:contains(关键词：) > a")) {
					keyword += ele.text().replaceAll("[;；\\s]+$", "") + ";";
				}

				// 去掉最后的分号
				keyword = keyword.replaceAll(";+$", "");

				// System.out.println("keyword:" + keyword);
			}
			/**************************** end keyword ****************************/

			/**************************** begin clc_no ****************************/
			{
				Element eleClass = doc.select("div.wxBaseinfo > p:contains(分类号：)").first();
				if (eleClass != null) {
					clc_no = eleClass.text().trim();
				}

				if (clc_no.startsWith("分类号：")) {
					clc_no = clc_no.substring("分类号：".length()).trim();
				}

				if (clc_no.equals("+")) {
					clc_no = "";
				}
				if (clc_no.length() > 0) {
					String[] vec = clc_no.split(";");
					if (vec.length > 0) {
						clc_no_1st = vec[0].trim();
					}
				}

//				System.out.println("clc_no:" + clc_no);
//				System.out.println("clc_no_1st:" + clc_no_1st);
			}
			/**************************** end clc_no ****************************/

			/**************************** begin doi ****************************/
			{
				Element eleDOI = doc.select("div.wxBaseinfo > p:contains(DOI：)").first();
				if (eleDOI != null) {
					doi = eleDOI.text().trim();
				}

				if (doi.startsWith("DOI：")) {
					doi = doi.substring("DOI：".length());
				}

//				System.out.println("doi:" + doi);
			}
			/**************************** end doi ****************************/

			/**************************** begin down_cnt ****************************/
			{
				if (down_cnt.length() < 1) {
					down_cnt = "0@" + down_date;
				}

				////*[@id="mainArea"]/div[3]/div[3]/div[1]/div[3]/div[1]/div[2]/div[1]/div[1]/span[1]/label
				Element elePage = doc
						.select("div.dllink-down > div.info > div.total > span:contains(下载：)").first();
				if (elePage != null) {
					down_cnt = elePage.text().trim();
				}

				if (down_cnt.startsWith("下载：")) {
					down_cnt = down_cnt.substring("下载：".length());
					down_cnt = down_cnt + "@" + down_date;
				}

//				 System.out.println("down_cnt:" + down_cnt);
			}
			/**************************** end down_cnt ****************************/

			/**************************** begin page_info ****************************/
			{
				Element elePage = doc.select(
						"div.dllink-down > div.info > div.total > span:contains(页码：)")
						.first();
				String line = "";
				if (elePage != null) {
					line = elePage.text().trim();
				}

				if (line.startsWith("页码：")) {
					line = line.substring("页码：".length());
					page_info = line;
				}

				int idx = line.indexOf('+');
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

//				System.out.println("page_info:" + page_info);
//				System.out.println("begin_page:" + begin_page);
//				System.out.println("end_page:" + end_page);
//				System.out.println("jump_page:" + jump_page);
			}
			/**************************** end page_info ****************************/

			/**************************** begin page_cnt ****************************/
			{
				Element elepage_cnt = doc.select(
						"div.wxBaseinfo > div.dllink-box > div.dllink-box-left > div.dllink-down > div.info > div.total > span:contains(页数：)")
						.first();
				String line = "";
				if (elepage_cnt != null) {
					line = elepage_cnt.text().trim();
				}

				if (line.startsWith("页数：")) {
					page_cnt = line.substring("页数：".length());
				}

//				System.out.println("page_cnt:" + page_cnt);
			}
			/**************************** end page_cnt ****************************/

			/**************************** begin journal_name ****************************/
			{
				Element ele = doc.select("div.wxInfo > div.wxsour > div.sourinfo > p.title > a").first();
				if (ele != null) {
					journal_name = ele.text().trim();
				}

//				System.out.println("journal_name:" + journal_name);
			}
			/**************************** end journal_name ****************************/

			/**************************** begin journal_name ****************************/
			{
				Element ele = doc.select("div.wxInfo > div.wxsour > div.sourinfo > p:nth-child(2) > a").first();
				if (ele != null) {
					journal_name_alt = ele.text().trim();
				}

				// System.out.println("journal_name_alt:" + journal_name_alt);
			}
			/**************************** end journal_name ****************************/

			/**************************** begin issn ****************************/
			{
				Element eleISSN = doc.select("div.wxInfo > div.wxsour > div.sourinfo > p:contains(ISSN：)").first();
				if (eleISSN != null) {
					issn = eleISSN.text().trim();
				}

				if (issn.startsWith("ISSN：")) {
					issn = issn.substring("ISSN：".length());
				}

				// System.out.println("issn:" + issn);
			}
			/**************************** end issn ****************************/

			/****************************
			 * begin if_html_fulltext
			 ****************************/
			{
				Element eleISSN = doc.select("#DownLoadParts > a:contains(HTML阅读)").first();
				if (eleISSN != null) {
					fulltext_type += ";html";
				}
				fulltext_type = StringHelper.cleanSemicolon(fulltext_type);

				// System.out.println("if_html_fulltext:" + if_html_fulltext);
			}
			/**************************** end if_html_fulltext ****************************/
		}

		/**
		 * <p>
		 * Description: 解析海外版详情页
		 * </p>
		 * 
		 * @author qiuhongyang 2018年11月18日 上午11:22:43
		 * @param detailHtml
		 */
		public void parseOversea(String detailHtml) {
			String lowerDetailHtml = detailHtml.toLowerCase();
			if (lowerDetailHtml.indexOf("jname") < 0) {
				return;
			}
			if (lowerDetailHtml.indexOf(rawid.toLowerCase()) < 0) {
				return;
			}
			has_oversea = "1";
			Document doc = Jsoup.parse(detailHtml);

			/**************************** begin title ****************************/
			{
				Element eleTitleC = doc.select("span#chTitle").first();
				if (eleTitleC != null) {
					title = eleTitleC.text().trim();
				}

				if (title.length() < 1) {
					title = doc.title().trim();
					if (title.endsWith("- China Academic Journals Full-text Database")) {
						title = title
								.substring(0, title.length() - "- China Academic Journals Full-text Database".length())
								.trim();
					}
				}

//				System.out.println("title:" + title);
			}
			/**************************** end title ****************************/

			/**************************** begin title_e ****************************/
			{
				Element eleTitleE = doc.select("span#enTitle").first();
				if (eleTitleE != null) {
					title_alt = eleTitleE.text().trim();
				}

//				System.out.println("title_e:" + title_e);
			}
			/**************************** end title_e ****************************/

			/**************************** begin author_c ****************************/
			{// 中文作者不从海外版取
//				for (Element ele : doc.select("div.author > p:contains(【Author in Chinese】) > a")) {
//					author_c += ele.text().trim() + ";";
//				}
//				author_c = StringHelper.cleanSemicolon(author_c);	// 清理分号

//				System.out.println("author_c:" + author_c);
			}
			/**************************** end author_c ****************************/

			/**************************** begin author_id ****************************/
			{// 中文作者不从海外版取
//				if (author_c.length() > 0) {	// 有作者时才有意义
//					Element ele = doc.select("div.author > p:contains(【Author in Chinese】) > a").first();
//					if (ele != null) {
//						String href = ele.attr("href").trim();
//						author_id = URLHelper.getParamValueFirst(href, "code");
//						author_id = StringHelper.cleanSemicolon(author_id);	// 清理分号
//					}
//					
//					if (author_id.length() > 0) {	// 有作者ID时才有意义
//						String[] authorArray = author_c.split(";");
//						String[] idArray = author_id.split(";");
//						if (authorArray.length != idArray.length) {	// 海外版作者和作者编号个数不一致时，不保留作者ID
//							author_id = "";
//						}
//						else {
//							String tmp = "";
//							for (int i = 0; i < authorArray.length; i++) {
//								tmp += idArray[i] + "@" + authorArray[i] + ";";
//							}
//							author_id = StringHelper.cleanSemicolon(tmp);
//						}
//					}
//				}

//				System.out.println("author_id:" + author_id);
			}
			/**************************** end author_id ****************************/

			/**************************** begin author_raw ****************************/
			{
				Element eleAuthorE = doc.select("div.author > p:contains(【Author】)").first();
				if (eleAuthorE != null) {
					author_raw = eleAuthorE.text().trim();
					author_raw = author_raw.replaceAll("^【Author】", "").trim();
				}

				// 去掉最后的分号
				author_raw = author_raw.replaceAll(";+$", "");

//				System.out.println("author_raw:" + author_raw);
			}
			/**************************** end author_raw ****************************/

			/**************************** begin organ ****************************/
			{// 中文机构不从海外版取
//				for (Element ele : doc.select("div.author > p:contains(【Institution】) > a")) {
//					organ += ele.text().trim() + ";";
//				}
//
//				// 去掉最后的分号
//				organ = organ.replaceAll(";+$", "");
//
//				System.out.println("organ:" + organ);
			}
			/**************************** end organ ****************************/

			/**************************** begin organ_id ****************************/
			{// 中文机构不从海外版取
//				if (organ.length() > 0) {	// 有机构时才有意义
//					Element ele = doc.select("div.author > p:contains(【Institution】) > a").first();
//					if (ele != null) {
//						String href = ele.attr("href").trim();
//						organ_id = URLHelper.getParamValueFirst(href, "code");
//						organ_id = StringHelper.cleanSemicolon(organ_id);	// 清理分号
//					}
//					
//					if (organ_id.length() > 0) {	// 有机构ID时才有进一步处理的意义
//						String[] authorArray = organ.split(";");
//						String[] idArray = organ_id.split(";");
//						if (authorArray.length != idArray.length) {	// 海外版作者和作者编号个数不一致时，不保留作者ID
//							organ_id = "";
//						}
//						else {
//							String tmp = "";
//							for (int i = 0; i < authorArray.length; i++) {
//								tmp += idArray[i] + "@" + authorArray[i] + ";";
//							}
//							organ_id = StringHelper.cleanSemicolon(tmp);
//						}
//					}
//				}
//
//				System.out.println("organ_id:" + organ_id);
			}
			/**************************** end organ_id ****************************/

			/**************************** begin corr_author ****************************/
			{// 通讯作者
				Element ele = doc.select("div.author > p:contains(【通訊作者】)").first();
				if (ele != null) {
					corr_author = ele.text().trim();
					corr_author = corr_author.substring("【通訊作者】".length()).trim();
					corr_author = StringHelper.cleanSemicolon(corr_author); // 清理分号
				}
//				System.out.println("corr_author:" + corr_author);
			}
			/**************************** begin corr_author ****************************/

			/**************************** begin abstract_ ****************************/
			{
				Element ele = doc.select("span#ChDivSummary").first();

				if (ele != null) {
					abstract_ = ele.text().trim();
					if (abstract_.startsWith("<正>")) {
						abstract_ = abstract_.substring("<正>".length());
						abstract_type = "第一段";
					}
				}

//				System.out.println("abstract_:" + abstract_);
			}
			/**************************** end abstract_ ****************************/

			/**************************** begin abstract_alt ****************************/
			{
				Element ele = doc.select("span#EnChDivSummary").first();

				if (ele != null) {
					abstract_alt = ele.text().trim();
				}

//				System.out.println("abstract_alt:" + abstract_alt);
			}
			/**************************** end abstract_alt ****************************/

			/**************************** begin fund ****************************/
			{
				Element elefund = doc.select("#main > div > div.summary > div.keywords:contains(【Fund】)").first();
				if (elefund != null) {
					fund = elefund.text().trim();
					fund = fund.substring("【Fund】".length()).trim();
				}

				// 去掉最后的分号
				fund = fund.replaceAll(";+$", "");

//				 System.out.println("fund:" + fund);
			}
			/**************************** end fund ****************************/

			/**************************** begin keyword ****************************/
			{
				Element eleKeyword = doc.select("div.keywords:contains(【Key)").first();
				if (eleKeyword != null) {
					String text = eleKeyword.text().trim();

//					System.out.println("text: " + text);

					int idx1 = text.indexOf("【Keywords in Chinese】");
					int idx2 = text.indexOf("【Key words】");

					// 得到中文关键词
					if (idx1 > -1) {
						if (idx2 > -1) { // 同时存在中英关键词
							keyword = text.substring("【Keywords in Chinese】".length(), idx2).trim();
						} else { // 只存在中文关键词
							keyword = text.substring("【Keywords in Chinese】".length()).trim();
						}
					}

					// 得到英文关键词
					if (idx2 > -1) {
						keyword_alt = text.substring(idx2 + "【Key words】".length()).trim();
					}
				}
				// 替换中文分号和空白
				keyword = keyword.replaceAll("；\\s*", ";");
				keyword_alt = keyword_alt.replaceAll("；\\s*", ";");

				// 去掉最后的分号
				keyword = keyword.replaceAll(";+$", "");
				keyword_alt = keyword_alt.replaceAll(";+$", "");

//				System.out.println("keyword:" + keyword);
//				System.out.println("keyword_alt:" + keyword_alt);
			}
			/**************************** end keyword ****************************/

			/**************************** begin clc_no ****************************/
			{// 暂不从海外版取中图分类号
//				Element eleClass = doc.select("#main > div > div > ul.break > li:contains(【CLC code】)").first();
//				if (eleClass != null) {
//					clc_no = eleClass.text().trim();
//					clc_no = clc_no.substring("【CLC code】".length()).trim();
//				}
//
//				if (clc_no.equals("+")) {
//					clc_no = "";
//				}

//				System.out.println("clc_no:" + clc_no);
			}
			/**************************** end clc_no ****************************/

			/**************************** begin pub_date_alt ****************************/
			{
				Element ele = doc.select("#main > div > div > ul.break > li:contains(【Internet Publish Date】)").first();
				if (ele != null) {
					pub_date_alt = ele.text().trim();
					pub_date_alt = pub_date_alt.substring("【Internet Publish Date】".length()).trim();
					pub_date_alt = pub_date_alt.replaceAll("(\\d{4})-(\\d{2})-(\\d{2})\\s\\d{2}:\\d{2}", "$1$2$3");
				}

				if (!DateTimeHelper.checkDateByRange(pub_date_alt, 10001212, 30001212)) {
					pub_date_alt = "";
				}

//				System.out.println("pub_date_alt:" + pub_date_alt);
			}
			/**************************** end pub_date_alt ****************************/

			/**************************** begin doi ****************************/
			{// 海外版无doi
//				Element eleDOI = doc.select("div.wxBaseinfo > p:contains(DOI：)").first();
//				if (eleDOI != null) {
//					doi = eleDOI.text().trim();
//				}
//
//				if (doi.startsWith("DOI：")) {
//					doi = doi.substring("DOI：".length());
//				}

//				System.out.println("doi:" + doi);
			}
			/**************************** end doi ****************************/

			/**************************** begin down_cnt ****************************/
			{
				if (down_cnt.length() < 1) {
					down_cnt = "0@" + down_date;
				}
				Element ele = doc.select("#main > div > div > ul.break > li:contains(【Downloads】)").first();
				if (ele != null) {
					down_cnt = ele.text().trim();
					down_cnt = down_cnt.substring("【Downloads】".length()).trim();
					down_cnt = down_cnt + "@" + down_date;
				}

//				System.out.println("down_cnt:" + down_cnt);
			}
			/**************************** end down_cnt ****************************/

			/**************************** begin page_info ****************************/
			{// 海外版详情页无页码信息
			}
			/**************************** end page_info ****************************/

			/**************************** begin page_cnt ****************************/
			{// 海外版详情页没有页数
			}
			/**************************** end page_cnt ****************************/

			/**************************** begin journal_name ****************************/
			{
				Element ele = doc.select("#jname").first();
				if (ele != null) {
					journal_name = ele.text().trim();
				}

//				System.out.println("journal_name:" + journal_name);
			}
			/**************************** end journal_name ****************************/

			/****************************
			 * begin journal_name_alt
			 ****************************/
			{
				Element ele = doc.select("#jnameen").first();
				if (ele != null) {
					journal_name_alt = ele.text().trim();
				}

//				System.out.println("journal_name_alt:" + journal_name_alt);
			}
			/**************************** end journal_name_alt ****************************/

			/**************************** begin issn ****************************/
			{// 海外版无ISSN
			}
			/**************************** end issn ****************************/

			/*************** begin if_html_fulltext fulltext_type *************/
			{
//				Element ele = doc.select("#QK_nav > ul > li.html-read > a").first();
//				if (ele != null) {
//					fulltext_type += ";html";
//				}

//				System.out.println("if_html_fulltext:" + if_html_fulltext);
			}
			/************ end if_html_fulltext fulltext_type ********/
		}

		// 解析引文量相关
		void parseRefcount(String refcountText) {
			// {'REFERENCE':'36','SUB_REFERENCE':'113','CITING':'2','SUB_CITING':'0','CO_CITING':'2448','CO_CITED':'15'}

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(refcountText, type);
			ref_cnt = mapJson.get("REFERENCE"); // 引文量
			cited_cnt = mapJson.get("CITING"); // 被引量

			cited_cnt = cited_cnt + "@" + down_date;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			{
				has_oversea = "0"; // 是否有海外版的数据，是否用到海外版数据

				lngid = "";
				rawid = "";
				sub_db_id = "00002";
				product = "CNKI";
				sub_db = "CJFD";
				provider = "CNKI";
				down_date = "";
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
				fulltext_type = "caj;pdf";
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
				country = "";
				language = "";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
			}

			String line = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			rawid = mapJson.get("filename");
			provider_url = "http://kns.cnki.net/KCMS/detail/detail.aspx?dbcode=CJFD&filename=" + rawid;
			journal_raw_id = mapJson.get("pykm");
			pub_year = mapJson.get("pubyear");
			num = mapJson.get("num");
			page_info = mapJson.get("pageline");
			column_info = mapJson.get("col");
			down_date = mapJson.get("down_date");
			country = getCountry(journal_raw_id);
			language = getLanguage(journal_raw_id);
			cnno = getCNNO(journal_raw_id);
			String knsHtml = mapJson.get("kns");
			String overseaHtml = mapJson.get("oversea");
			String refcountText = mapJson.get("refcount");

			// 制止投毒事件
			if (knsHtml.toUpperCase().indexOf(rawid.toUpperCase()) < 0) {
				context.getCounter("map", "error: kns html not find filename").increment(1);
				return;
			}
			if ((knsHtml.toLowerCase().indexOf("layer7") > -1) && (knsHtml.toLowerCase().indexOf("ddos") > -1)) {
				context.getCounter("map", "error: kns ddos").increment(1);
				return;
			}

			parseKNS(knsHtml); // 解析 kns 详情页
			parseOversea(overseaHtml); // 解析海外版详情页
			parseRefcount(refcountText); // 解析引文

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
				}

				if (journalLevel.containsKey(journal_raw_id)) {
					String level = journalLevel.get(journal_raw_id);
					if ((level.toUpperCase().equals("A")) || (level.toUpperCase().equals("B"))
							|| (level.toUpperCase().equals("C"))) {
						context.getCounter("map_ABC", "count").increment(1);
						if (author.length() < 1) {
							context.getCounter("map_ABC", "warning: no author").increment(1);
						}

						if (organ.length() < 1) {
							context.getCounter("map_ABC", "warning: no organ").increment(1);
						}

						if ((author.length() < 1) && (organ.length() < 1)) {
							context.getCounter("map_ABC", "warning: no author organ").increment(1);
							LogMR.log2HDFS4Mapper(context, logHDFSFile, "A_B_C no author organ: " + rawid);
						}
					}
				}
			}

			lngid = VipIdEncode.getLngid("00002", rawid, false);
			XXXXObject xObj = new XXXXObject();
			{
				xObj.data.put("has_oversea", has_oversea);

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
			}

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
