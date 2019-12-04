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
import org.apache.hadoop.mapreduce.Reducer;
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

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject4kns extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "cnki_qk." + this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat(
				"mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out
				.println("******mapred.reduce.slowstart.completed.maps*******"
						+ job.getConfiguration().get(
								"mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration()
				.set("io.compression.codecs",
						"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******"
				+ job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

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

	public static class ProcessMapper extends
			Mapper<LongWritable, Text, Text, BytesWritable> {

		static int cnt = 0;

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
		private static String organ = "";
		private static String remark_c = "";
		private static String keyword_c = "";
		private static String imburse = ""; // 基金
		private static String doi = "";
		private static String sClass = "";
		private static String name_c = "";
		private static String name_e = "";
		private static String issn = "";
		private static String pub1st = "0"; // 是否优先出版
		private static String if_html_fulltext = "0"; // 是否有 html 全文
		private static String down_cnt = "0"; // 下载量
		private static String ref_cnt = "0"; // 引文量
		private static String cited_cnt = "0"; // 被引量

		static boolean isContainChinese(String str) {

			Pattern p = Pattern.compile("[\u4e00-\u9fa5]");
			Matcher m = p.matcher(str);
			if (m.find()) {
				return true;
			}
			return false;
		}

		// 清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';'); // 全角转半角
			text = text.replaceAll(";\\s+?", ";"); // 去掉分号后的空白
			text = text.replaceAll("[\\s;]+?$", ""); // 去掉最后多余的空白和分号

			return text;
		}

		// 解析详情页
		public void parseDetail(String detailHtml) {
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
						title_c = title_c.substring(0,
								title_c.length() - "- 中国知网".length()).trim();
					}
				}

//				System.out.println("title_c:" + title_c);
			}
			/**************************** end title ****************************/

			/**************************** begin author ****************************/
			{
				for (Element ele : doc
						.select("div.wxTitle > div.author > span")) {
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
				for (Element ele : doc
						.select("div.wxBaseinfo > p:contains(基金：) > a")) {
					imburse += ele.text().replaceAll("[；\\s]+$", "") + ";";
				}

				// 去掉最后的分号
				imburse = imburse.replaceAll(";+$", "");

				// System.out.println("imburse:" + imburse);
			}
			/**************************** end imburse ****************************/

			/**************************** begin keyword_c ****************************/
			{
				for (Element ele : doc
						.select("div.wxBaseinfo > p:contains(关键词：) > a")) {
					keyword_c += ele.text().replaceAll("[;；\\s]+$", "") + ";";
				}

				// 去掉最后的分号
				keyword_c = keyword_c.replaceAll(";+$", "");

				// System.out.println("keyword_c:" + keyword_c);
			}
			/**************************** end keyword_c ****************************/

			/**************************** begin sClass ****************************/
			{
				Element eleClass = doc.select(
						"div.wxBaseinfo > p:contains(分类号：)").first();
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
				Element eleDOI = doc
						.select("div.wxBaseinfo > p:contains(DOI：)").first();
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
				Element elePage = doc
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(下载：)")
						.first();
				if (elePage != null) {
					down_cnt = elePage.text().trim();
				}

				if (down_cnt.startsWith("下载：")) {
					down_cnt = down_cnt.substring("下载：".length());
				}

				// System.out.println("down_cnt:" + down_cnt);
			}
			/**************************** end down_cnt ****************************/

			/**************************** begin pageline ****************************/
			{
				Element elePage = doc
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(页码：)")
						.first();
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
						.select("div.wxBaseinfo > div.dllink-down > div.info > div.total > span:contains(页数：)")
						.first();
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
				Element ele = doc.select(
						"div.wxInfo > div.wxsour > div.sourinfo > p.title > a")
						.first();
				if (ele != null) {
					name_c = ele.text().trim();
				}

				// System.out.println("name_c:" + name_c);
			}
			/**************************** end name_c ****************************/

			/**************************** begin name_c ****************************/
			{
				Element ele = doc
						.select("div.wxInfo > div.wxsour > div.sourinfo > p:nth-child(2) > a")
						.first();
				if (ele != null) {
					name_e = ele.text().trim();
				}

				// System.out.println("name_e:" + name_e);
			}
			/**************************** end name_c ****************************/

			/**************************** begin issn ****************************/
			{
				Element eleISSN = doc
						.select("div.wxInfo > div.wxsour > div.sourinfo > p:contains(ISSN：)")
						.first();
				if (eleISSN != null) {
					issn = eleISSN.text().trim();
				}

				if (issn.startsWith("ISSN：")) {
					issn = issn.substring("ISSN：".length());
				}

				// System.out.println("issn:" + issn);
			}
			/**************************** end issn ****************************/

			/**************************** begin if_html_fulltext ****************************/
			{
				Element eleISSN = doc.select(
						"#DownLoadParts > a:contains(HTML阅读)").first();
				if (eleISSN != null) {
					if_html_fulltext = "1";
				}

				// System.out.println("if_html_fulltext:" + if_html_fulltext);
			}
			/**************************** end if_html_fulltext ****************************/
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
				String pathfile = "/user/qhy/log/log_map/" + nowDate
						+ ".txt";
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

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {

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
				organ = "";
				remark_c = "";
				keyword_c = "";
				imburse = ""; // 基金
				doi = "";
				sClass = "";
				name_c = "";
				name_e = "";
				issn = "";
				pub1st = "0"; // 是否优先出版
				if_html_fulltext = "0"; // 是否有 html 全文
				down_cnt = "0"; // 下载量
				cited_cnt = "0"; // 被引量
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
			muinfo = mapJson.get("col");
			String detailHtml = mapJson.get("detail");
			String refcountText = mapJson.get("refcount");

			// 制止投毒事件
			if (detailHtml.toUpperCase().indexOf(rawid.toUpperCase()) < 0) {
				context.getCounter("map", "error: html not find filename")
						.increment(1);
				return;
			}

			if ((detailHtml.toLowerCase().indexOf("layer7") > -1)
					&& (detailHtml.toLowerCase().indexOf("ddos") > -1)) {
				context.getCounter("map", "error: ddos").increment(1);
				return;
			}

			parseDetail(detailHtml); // 解析详情页
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
			xObj.data.put("organ", organ);
			xObj.data.put("remark_c", remark_c);
			xObj.data.put("keyword_c", keyword_c);
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
			xObj.data.put("down_cnt", down_cnt);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("parse_time", (new SimpleDateFormat(
					"yyyy-MM-dd_kk:mm:ss")).format(new Date()));

			context.getCounter("map", "count").increment(1);
			if (pub1st.equals("1")) {
				context.getCounter("map", "pub1st").increment(1);
			}

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}

	public static class ProcessReducer extends
			Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values,
				Context context) throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable(); // 用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reduce", "count").increment(1);

			bOut.setCapacity(bOut.getLength()); // 将buffer设为实际长度

			context.write(key, bOut);
		}
	}
}
