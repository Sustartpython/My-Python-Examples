package simple.jobstream.mapreduce.site.cnki_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

/**
 * <p>
 * Description: 针对kns和海外版合版的解析
 * http://oversea.cnki.net/kcms/detail/detail.aspx?filename=KYGL2018S1010&dbcode=CJFD
 * </p>
 * 
 * @author qiuhongyang 2018年11月6日 下午1:36:17
 */
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
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

		static int cnt = 0;

		private static String batch = ""; // 获取外部值

		private static String rawid = "";
		private static String pykm = ""; // 拼音刊名
		private static String years = "";
		private static String num = "";
		private static String muinfo = "";
		private static String pageline = ""; // 页码信息
		private static String pagecount = ""; // 页数
		private static String title_c = "";
		private static String title_e = "";
		private static String author_c = "";
		private static String author_e = "";
		private static String corr_author = "";
		private static String author_id = "";
		private static String organ = "";
		private static String organ_id = "";
		private static String remark_c = "";
		private static String remark_e = "";
		private static String keyword_c = "";
		private static String keyword_e = "";
		private static String imburse = ""; // 基金
		private static String doi = "";
		private static String sClass = "";
		private static String process_date = ""; // 在线出版日期（online date）
		private static String name_c = "";
		private static String name_e = "";
		private static String issn = "";
		private static String pub1st = "0"; // 是否优先出版
		private static String if_html_fulltext = "0"; // 是否有 html 全文
		private static String fulltext_type = "caj;pdf";
		private static String down_cnt = ""; // 下载量
		private static String ref_cnt = ""; // 引文量
		private static String cited_cnt = ""; // 被引量
		private static String down_date = DateTimeHelper.getNowDate(); // 下载日期给个默认值

		public void setup(Context context) throws IOException, InterruptedException {

			batch = context.getConfiguration().get("batch");
		}

		static boolean isContainChinese(String str) {

			Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
			Matcher m = p.matcher(str);
			if (m.find()) {
				return true;
			}
			return false;
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
					title_c = eleTitle.text().trim();
				}

				if (title_c.length() < 1) {
					title_c = doc.title().trim();
					if (title_c.endsWith("- 中国知网")) {
						title_c = title_c.substring(0, title_c.length() - "- 中国知网".length()).trim();
					}
				}

//				System.out.println("title_c:" + title_c);
			}
			/**************************** end title ****************************/

			/**************************** begin author ****************************/
			{
				for (Element ele : doc.select("div.wxTitle > div.author > span")) {
					author_c += ele.text().trim() + ";";
				}

				// 去掉最后的分号
				author_c = author_c.replaceAll(";+$", "");

				// System.out.println("author_c:" + author_c);
			}
			/**************************** end author ****************************/

			/**************************** begin organ ****************************/
			{
				for (Element ele : doc.select("div.wxTitle > div.orgn > span")) {
					organ += ele.text().trim() + ";";
				}

				// 去掉最后的分号
				organ = organ.replaceAll(";+$", "");

				// System.out.println("organ:" + organ);
			}
			/**************************** end organ ****************************/

			/**************************** begin remark_c ****************************/
			{
				Element eleRemark = doc.select("span#ChDivSummary").first();

				if (eleRemark != null) {
					remark_c = eleRemark.text().trim();
				}

				// System.out.println("remark_c:" + remark_c);
			}
			/**************************** end remark_c ****************************/

			/**************************** begin imburse ****************************/
			{
				for (Element ele : doc.select("div.wxBaseinfo > p:contains(基金：) > a")) {
					imburse += ele.text().replaceAll("[；\\s]+$", "") + ";";
				}

				// 去掉最后的分号
				imburse = imburse.replaceAll(";+$", "");

				// System.out.println("imburse:" + imburse);
			}
			/**************************** end imburse ****************************/

			/**************************** begin keyword_c ****************************/
			{
				for (Element ele : doc.select("div.wxBaseinfo > p:contains(关键词：) > a")) {
					keyword_c += ele.text().replaceAll("[;；\\s]+$", "") + ";";
				}

				// 去掉最后的分号
				keyword_c = keyword_c.replaceAll(";+$", "");

				// System.out.println("keyword_c:" + keyword_c);
			}
			/**************************** end keyword_c ****************************/

			/**************************** begin sClass ****************************/
			{
				Element eleClass = doc.select("div.wxBaseinfo > p:contains(分类号：)").first();
				if (eleClass != null) {
					sClass = eleClass.text().trim();
					sClass = sClass.replace(';', ' ');
				}

				if (sClass.startsWith("分类号：")) {
					sClass = sClass.substring("分类号：".length()).trim();
				}

				if (sClass.equals("+")) {
					sClass = "";
				}

				// System.out.println("sClass:" + sClass);
			}
			/**************************** end sClass ****************************/

			/**************************** begin doi ****************************/
			{
				Element eleDOI = doc.select("div.wxBaseinfo > p:contains(DOI：)").first();
				if (eleDOI != null) {
					doi = eleDOI.text().trim();
				}

				if (doi.startsWith("DOI：")) {
					doi = doi.substring("DOI：".length());
				}

				System.out.println("doi:" + doi);
			}
			/**************************** end doi ****************************/

			/**************************** begin down_cnt ****************************/
			{
				if (down_cnt.length() < 1) {
					down_cnt = "0@" + down_date;
				}

				Element elePage = doc
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(下载：)").first();
				if (elePage != null) {
					down_cnt = elePage.text().trim();
				}

				if (down_cnt.startsWith("下载：")) {
					down_cnt = down_cnt.substring("下载：".length());
					down_cnt = down_cnt + "@" + down_date;
				}

				// System.out.println("down_cnt:" + down_cnt);
			}
			/**************************** end down_cnt ****************************/

			/**************************** begin pageline ****************************/
			{
				Element elePage = doc
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(页码：)").first();
				String line = "";
				if (elePage != null) {
					line = elePage.text().trim();
				}

				if (line.startsWith("页码：")) {
					pageline = line.substring("页码：".length());
				}

				// System.out.println("pageline:" + pageline);
			}
			/**************************** end pageline ****************************/

			/**************************** begin pagecount ****************************/
			{
				Element elePageCount = doc
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(页数：)").first();
				String line = "";
				if (elePageCount != null) {
					line = elePageCount.text().trim();
				}

				if (line.startsWith("页数：")) {
					pagecount = line.substring("页数：".length());
				}

				// System.out.println("pagecount:" + pagecount);
			}
			/**************************** end pagecount ****************************/

			/**************************** begin name_c ****************************/
			{
				Element ele = doc.select("div.wxInfo > div.wxsour > div.sourinfo > p.title > a").first();
				if (ele != null) {
					name_c = ele.text().trim();
				}

				// System.out.println("name_c:" + name_c);
			}
			/**************************** end name_c ****************************/

			/**************************** begin name_c ****************************/
			{
				Element ele = doc.select("div.wxInfo > div.wxsour > div.sourinfo > p:nth-child(2) > a").first();
				if (ele != null) {
					name_e = ele.text().trim();
				}

				// System.out.println("name_e:" + name_e);
			}
			/**************************** end name_c ****************************/

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
					if_html_fulltext = "1";
				}

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
			Document doc = Jsoup.parse(detailHtml);

			/**************************** begin title_c ****************************/
			{
				Element eleTitleC = doc.select("span#chTitle").first();
				if (eleTitleC != null) {
					title_c = eleTitleC.text().trim();
				}

				if (title_c.length() < 1) {
					title_c = doc.title().trim();
					if (title_c.endsWith("- China Academic Journals Full-text Database")) {
						title_c = title_c
								.substring(0,
										title_c.length() - "- China Academic Journals Full-text Database".length())
								.trim();
					}
				}

//				System.out.println("title_c:" + title_c);
			}
			/**************************** end title_c ****************************/

			/**************************** begin title_e ****************************/
			{
				Element eleTitleE = doc.select("span#enTitle").first();
				if (eleTitleE != null) {
					title_e = eleTitleE.text().trim();
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

			/**************************** begin author_e ****************************/
			{
				Element eleAuthorE = doc.select("div.author > p:contains(【Author】)").first();
				if (eleAuthorE != null) {
					author_e = eleAuthorE.text().trim();
					author_e = author_e.replaceAll("^【Author】", "").trim();
				}

				// 去掉最后的分号
				author_e = author_e.replaceAll(";+$", "");

//				System.out.println("author_e:" + author_e);
			}
			/**************************** end author_e ****************************/

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

			/**************************** begin remark_c ****************************/
			{
				Element ele = doc.select("span#ChDivSummary").first();

				if (ele != null) {
					remark_c = ele.text().trim();
					if (remark_c.startsWith("<正>")) {
						remark_c = remark_c.substring("<正>".length());
					}
				}

//				System.out.println("remark_c:" + remark_c);
			}
			/**************************** end remark_c ****************************/

			/**************************** begin remark_c ****************************/
			{
				Element ele = doc.select("span#EnChDivSummary").first();

				if (ele != null) {
					remark_e = ele.text().trim();
				}

//				System.out.println("remark_e:" + remark_e);
			}
			/**************************** end remark_c ****************************/

			/**************************** begin imburse ****************************/
			{
				Element eleImburse = doc.select("#main > div > div.summary > div.keywords:contains(【Fund】)").first();
				if (eleImburse != null) {
					imburse = eleImburse.text().trim();
					imburse = imburse.substring("【Fund】".length()).trim();
				}

				// 去掉最后的分号
				imburse = imburse.replaceAll(";+$", "");

//				 System.out.println("imburse:" + imburse);
			}
			/**************************** end imburse ****************************/

			/**************************** begin keyword_c ****************************/
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
							keyword_c = text.substring("【Keywords in Chinese】".length(), idx2).trim();
						} else { // 只存在中文关键词
							keyword_c = text.substring("【Keywords in Chinese】".length()).trim();
						}
					}

					// 得到英文关键词
					if (idx2 > -1) {
						keyword_e = text.substring(idx2 + "【Key words】".length()).trim();
					}
				}
				// 替换中文分号和空白
				keyword_c = keyword_c.replaceAll("；\\s*", ";");
				keyword_e = keyword_e.replaceAll("；\\s*", ";");

				// 去掉最后的分号
				keyword_c = keyword_c.replaceAll(";+$", "");
				keyword_e = keyword_e.replaceAll(";+$", "");

//				System.out.println("keyword_c:" + keyword_c);
//				System.out.println("keyword_e:" + keyword_e);
			}
			/**************************** end keyword_c ****************************/

			/**************************** begin sClass ****************************/
			{
				Element eleClass = doc.select("#main > div > div > ul.break > li:contains(【CLC code】)").first();
				if (eleClass != null) {
					sClass = eleClass.text().trim();
					sClass = sClass.substring("【CLC code】".length()).trim();
				}

				if (sClass.equals("+")) {
					sClass = "";
				}

//				System.out.println("sClass:" + sClass);
			}
			/**************************** end sClass ****************************/

			/**************************** begin process_date ****************************/
			{
				Element ele = doc.select("#main > div > div > ul.break > li:contains(【Internet Publish Date】)").first();
				if (ele != null) {
					process_date = ele.text().trim();
					process_date = process_date.substring("【Internet Publish Date】".length()).trim();
					process_date = process_date.replaceAll("(\\d{4})-(\\d{2})-(\\d{2})\\s\\d{2}:\\d{2}", "$1$2$3");
				}

				if (!DateTimeHelper.checkDateByRange(process_date, 10001212, 30001212)) {
					process_date = "";
				}

//				System.out.println("process_date:" + process_date);
			}
			/**************************** end process_date ****************************/

			/**************************** begin doi ****************************/
			{// 海外版无doi
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
				Element ele = doc.select("#main > div > div > ul.break > li:contains(【Downloads】)").first();
				if (ele != null) {
					down_cnt = ele.text().trim();
					down_cnt = down_cnt.substring("【Downloads】".length()).trim();
					down_cnt = down_cnt + "@" + down_date;
				}

//				System.out.println("down_cnt:" + down_cnt);
			}
			/**************************** end down_cnt ****************************/

			/**************************** begin pageline ****************************/
			{// 海外版详情页无页码信息
			}
			/**************************** end pageline ****************************/

			/**************************** begin pagecount ****************************/
			{// 海外版详情页没有页数
			}
			/**************************** end pagecount ****************************/

			/**************************** begin name_c ****************************/
			{
				Element ele = doc.select("#jname").first();
				if (ele != null) {
					name_c = ele.text().trim();
				}

//				System.out.println("name_c:" + name_c);
			}
			/**************************** end name_c ****************************/

			/**************************** begin name_e ****************************/
			{
				Element ele = doc.select("#jnameen").first();
				if (ele != null) {
					name_e = ele.text().trim();
				}

//				System.out.println("name_e:" + name_e);
			}
			/**************************** end name_e ****************************/

			/**************************** begin issn ****************************/
			{// 海外版无ISSN
			}
			/**************************** end issn ****************************/

			/*************** begin if_html_fulltext fulltext_type *************/
			{
				Element ele = doc.select("#QK_nav > ul > li.html-read > a").first();
				if (ele != null) {
					if_html_fulltext = "1";
					fulltext_type += ";html";
				}

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

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
				ex.printStackTrace();
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			{
				rawid = "";
				pykm = ""; // 拼音刊名
				years = "";
				num = "";
				muinfo = "";
				pageline = "";
				title_c = "";
				title_e = "";
				author_c = "";
				author_e = "";
				corr_author = "";
				author_id = "";
				organ = "";
				organ_id = "";
				remark_c = "";
				remark_e = "";
				keyword_c = "";
				keyword_e = "";
				imburse = ""; // 基金
				doi = "";
				sClass = "";
				process_date = "";
				name_c = "";
				name_e = "";
				issn = "";
				pub1st = "0"; // 是否优先出版
				if_html_fulltext = "0"; // 是否有 html 全文
				fulltext_type = "caj;pdf";
				down_cnt = ""; // 下载量
				cited_cnt = ""; // 被引量
				down_date = DateTimeHelper.getNowDate(); // 下载日期给个默认值
			}

			String line = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			rawid = mapJson.get("filename");
			pykm = mapJson.get("pykm");
			years = mapJson.get("pubyear");
			num = mapJson.get("num");
			pageline = mapJson.get("pageline");
			muinfo = mapJson.get("col");
			down_date = mapJson.get("down_date");
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

			if ((title_c.length() < 1) && (title_e.length() < 1)) {
				context.getCounter("map", "error: no title").increment(1);
				log2HDFSForMapper(context, "error: no title: " + rawid);
				return;
			}

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("pykm", pykm);
			xObj.data.put("title_c", title_c);
			xObj.data.put("title_e", title_e);
			xObj.data.put("author_c", author_c);
			xObj.data.put("author_e", author_e);
			xObj.data.put("corr_author", corr_author);
			xObj.data.put("author_id", author_id);
			xObj.data.put("organ", organ);
			xObj.data.put("organ_id", organ_id);
			xObj.data.put("remark_c", remark_c);
			xObj.data.put("remark_e", remark_e);
			xObj.data.put("keyword_c", keyword_c);
			xObj.data.put("keyword_e", keyword_e);
			xObj.data.put("imburse", imburse);
			xObj.data.put("muinfo", muinfo);
			xObj.data.put("doi", doi);
			xObj.data.put("sClass", sClass);
			xObj.data.put("name_c", name_c);
			xObj.data.put("name_e", name_e);
			xObj.data.put("issn", issn);
			xObj.data.put("years", years);
			xObj.data.put("num", num);
			xObj.data.put("pageline", pageline);
			xObj.data.put("pagecount", pagecount);
			xObj.data.put("pub1st", pub1st);
			xObj.data.put("if_html_fulltext", if_html_fulltext);
			xObj.data.put("fulltext_type", fulltext_type);
			xObj.data.put("down_cnt", down_cnt);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("process_date", process_date);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);

			context.getCounter("map", "count").increment(1);
			if (pub1st.equals("1")) {
				context.getCounter("map", "pub1st").increment(1);
			}

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
