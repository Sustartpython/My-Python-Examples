package simple.jobstream.mapreduce.site.iopjournal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.tools.ant.taskdefs.Replace;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将html格式转化为XXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName =  "iopjournal." + this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
//		job.setReducerClass(ProcessReducer.class);
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
		private static Map<String, String> mapMonth = new HashMap<String, String>();

		private static String title = "";
		private static String title_alternative = "";
		private static String creator = "";
		private static String creator_institution = "";
		private static String subject = "";
		private static String description = "";
		private static String description_en = "";
		private static String identifier_doi = "";
		private static String identifier_pissn = "";
		private static String volume = "";
		private static String issue = "";
		private static String source = "";
		// private static String publisher = "Elsevier Science";
		private static String date = "";
		private static String page = "";
		private static String date_created = "";

		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			initMapMonth(); // 初始化月份map
		}

		public static void initMapMonth() {
			mapMonth.put("january", "01");
			mapMonth.put("february", "02");
			mapMonth.put("march", "03");
			mapMonth.put("april", "04");
			mapMonth.put("may", "05");
			mapMonth.put("june", "06");
			mapMonth.put("july", "07");
			mapMonth.put("august", "08");
			mapMonth.put("september", "09");
			mapMonth.put("october", "10");
			mapMonth.put("november", "11");
			mapMonth.put("december", "12");

			mapMonth.put("spring", "03");
			mapMonth.put("summer", "06");
			mapMonth.put("autumn", "09");
			mapMonth.put("winter", "12"); // http://www.sciencedirect.com/science/article/pii/S0251108883800370

			mapMonth.put("1st quarter", "03"); // http://www.sciencedirect.com/science/article/pii/S1084856801000451
			mapMonth.put("2nd quarter", "06");
			mapMonth.put("3rd quarter", "09");
			mapMonth.put("4th quarter", "12"); // http://www.sciencedirect.com/science/article/pii/S0251108883800370
		}

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

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
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public boolean parseHtml(String rawid, Document doc) {

			Element eleArticleContent = doc.select("div#page-content").first();
			if (eleArticleContent == null) {
				// System.out.println("eleArticleContent == null");
				return false;
			}

			/**************************** begin title ****************************/
			{
				Element eleTitle = eleArticleContent.select("h1.wd-jnl-art-title").first();

				if (eleTitle != null) {
					title = eleTitle.text().trim();
				}

				if (title.length() < 1) {
					title = doc.title().trim();
					if (title.endsWith("- ScienceDirect")) {
						title = title.substring(0, title.length() - "- IOPscience".length()).trim();
					}
				}

				title = title.replaceAll("[ \\*]*$", ""); // 删除末尾的*
				if (title.length() < 1) {
					return false;
				}

//				System.out.println("title:" + title);	
//				System.out.println("title_alternative:" + title_alternative);	
			}
			/**************************** end title ****************************/

			/*****************************
			 * begin creator creator_institution
			 ********************************/
			{
				String item = "";
				for (Element eleAuthor : eleArticleContent.select("span > span.nowrap")) {
//					System.out.println(eleAuthor.text());
					String name = eleAuthor.select("span[itemprop=name]").first().text().trim();

					String sup = "";
					Element eleSup = eleAuthor.select("sup").first();
					if (eleSup != null) {
						sup = eleSup.text().trim();
					}

					item = name;
					if (sup.length() > 0) {
						item += "[" + sup + "]";
					}
					creator += item + ";";
				}
				creator = creator.replaceAll("[ ;]+?$", ""); // 去掉尾部多余的分号

				for (Element eleOrgan : eleArticleContent.select("div.wd-jnl-art-author-affiliations > p.mb-05")) {
//					System.out.println(eleOrgan.text());

					String sup = "";
					Element eleSup = eleOrgan.select("sup").first();
					if (eleSup != null) {
						sup = eleSup.text().trim();
						eleSup.remove();
					}

					String name = eleOrgan.text().trim();

					item = name;
					if (sup.length() > 0) {
						item = "[" + sup + "]" + item;
					}
					creator_institution += item + ";";
				}
				creator_institution = creator_institution.replaceAll(";+?$", ""); // 去掉尾部多余分号

//				System.out.println("creator:" + creator);
//				System.out.println("creator_institution:" + creator_institution);
			}
			/***************************
			 * end creator creator_institution
			 **************************/

			/*************************** begin subject **************************/
			{
				for (Element eleKeyword : eleArticleContent.select("div.wd-jnl-aas-keywords > p > a")) {
//					System.out.println(eleKeyword.text());
					subject += eleKeyword.text().trim() + ";";
				}
				subject = subject.replaceAll(";+?$", ""); // 去掉尾部多余分号
//				System.out.println("subject:" + subject);
			}
			/*************************** end subject **************************/

			/***************************
			 * begin description description_en
			 **************************/
			{
				Element eleAbs = eleArticleContent
						.select("div.article-text.wd-jnl-art-abstract.cf[itemprop=description]").first();
				if (eleAbs != null) {
					description = eleAbs.text().trim();
				}

//				System.out.println("description:" + description);
//				System.out.println("description_en:" + description_en);
			}
			/***************************
			 * end description description_en
			 **************************/

			/******************* begin identifier_doi ************************/
			{
				identifier_doi = rawid;
				Element eleDOI = eleArticleContent.select("div > div.col-no-break.wd-jnl-art-doi > p > a").first();
				if (eleDOI != null) {
					String url = eleDOI.text().trim();
					if (url.startsWith("https://doi.org/")) {
						identifier_doi = url.substring("https://doi.org/".length());
					}
				}

//				System.out.println("identifier_doi:" + identifier_doi);
			}
			/******************* end identifier_doi ************************/

			/****************************
			 * begin identifier_pissn
			 ******************************/
			{
				Element eleISSN = doc.select("head > meta[name=citation_issn]").first();
				if (eleISSN != null) {
					identifier_pissn = eleISSN.attr("content").trim();
				}
//				System.out.println("identifier_pissn:" + identifier_pissn);
			}
			/****************************
			 * end identifier_pissn
			 ******************************/

			/**************************** begin 出版日期 ******************************/
			{
				Element eleDate = eleArticleContent.select(" p > span[itemprop=datePublished]").first();
				if (eleDate != null) {
					String line = eleDate.text().trim();
					
					String year = "1900";
					String month = "00";
					String day = "00";
					
					Pattern pattern = Pattern.compile(".*\\b(\\d{4})\\b");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						year = matcher.group(1).trim();
					}	

					for (Map.Entry<String, String> entry : mapMonth.entrySet()) {						
						line = line.toLowerCase().trim();		// Published 19 December 2008;2009 February 19
						int idx = line.indexOf(entry.getKey());
						if (idx > -1) {
							month = entry.getValue();
							break;
						}
					}
					
					pattern = Pattern.compile("\\b(\\d{2})\\b.*");
					matcher = pattern.matcher(line);
					if (matcher.find()) {
						day = matcher.group(1).trim();
					}
					date = year;
					date_created = year + month + day;
				}

//				System.out.println("date:" + date);
//				System.out.println("date_created:" + date_created);
			}
			/**************************** end 出版日期 ******************************/

			/******************* begin source(刊名) ************************/
			{
				Element eleJournal = eleArticleContent.select("span[itemid=periodical] > span > a").first();
				if (eleJournal != null) {
					source = eleJournal.text().trim();
				}
//				System.out.println("source:" + source);
			}
			/******************* end source(刊名) ************************/

			return true;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "input").increment(1);
			// 防止混乱与错位
			{
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
				date = "1900";
				date_created = "19000000";
				page = "";
			}

			String line = value.toString().trim();
			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			String down_date = mapJson.get("DownDate");
			String rawid = mapJson.get("rawid");
			String issn = mapJson.get("issn");
			String vol = mapJson.get("vol");
			String num = mapJson.get("num");
			String pubdate = mapJson.get("pubdate");
			String jname = mapJson.get("jname");
			String html = mapJson.get("html");

			// 直接从json中获取值
			identifier_pissn = issn;
			volume = vol;
			issue = num;
			date_created = pubdate;
			date = date_created.substring(0, 4);
			source = jname;

			Document doc = Jsoup.parse(html);
			parseHtml(rawid, doc);

			// 没有title的数据不能要
			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				log2HDFSForMapper(context, "no title: " + rawid);
				return;
			}

			// identifier_pissn不应该不存在
			if (identifier_pissn.length() < 1) {
				log2HDFSForMapper(context, "issnNull text:" + value.toString());
				context.getCounter("map", "identifier_pissn.length() < 1").increment(1);
				log2HDFSForMapper(context, "no identifier_pissn: " + rawid);
				return;
			}

			if ((Integer.parseInt(date) < 1000) || (Integer.parseInt(date) > curYear + 1)) {
				context.getCounter("map", "err_date").increment(1);
				log2HDFSForMapper(context, "err_date:" + date + ";" + rawid);
				return;
			}

			if (date.equals("1900")) {
				context.getCounter("map", "warning:date_1900").increment(1);
				log2HDFSForMapper(context, "date_1900:" + rawid);
			}

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));
			xObj.data.put("down_date", down_date);
			xObj.data.put("rawid", rawid);
			xObj.data.put("title", title.trim());
			xObj.data.put("title_alternative", title_alternative.trim());
			xObj.data.put("creator", creator.trim());
			xObj.data.put("creator_institution", creator_institution.trim());
			xObj.data.put("subject", subject.trim());
			xObj.data.put("description", description.trim());
			xObj.data.put("description_en", description_en.trim());
			xObj.data.put("identifier_doi", identifier_doi.trim());
			xObj.data.put("identifier_pissn", identifier_pissn.trim());
			xObj.data.put("volume", volume.trim());
			xObj.data.put("issue", issue.trim());
			xObj.data.put("source", source.trim());
			xObj.data.put("date", date.trim());
			xObj.data.put("date_created", date_created.trim());
			xObj.data.put("batch", batch);

			context.getCounter("map", "out").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

}
