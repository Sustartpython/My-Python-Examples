package simple.jobstream.mapreduce.user.walker.wf_qk_ref;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer4Ref;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
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

		private static ScriptEngineManager manager = null;
		private static ScriptEngine engine = null;

//		private static String lngid = "";		// lngid 在 reduce 中生成
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String title = "";
		private static String title_alt = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String raw_type = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_alt = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String publisher = "";
		private static String cited_id = "";
		private static String linked_id = "";
		private static String refer_text_raw = "";
		private static String refer_text_raw_alt = "";
		private static String refer_text_site = "";
		private static String refer_text_site_alt = "";
		private static String refer_text = "";
		private static String refer_text_alt = "";
		private static String source_name = "";
		private static String source_name_alt = "";
		private static String strtype = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			initScriptEngine(context);
		}
		
		// 初始化 jscode （格式化引文）
		private static void initScriptEngine(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/_rel_file/format_ref.js"));
			BufferedReader in = null;
			String line = "";
			String jsCode = "";
			manager = new ScriptEngineManager();
			engine = manager.getEngineByName("javascript");
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.replaceAll("\\s+$", ""); // 去掉右侧空白
					jsCode += line + "\n";
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			try {
				System.out.println("*******************jsCode:\n" + jsCode);
				engine.eval(jsCode);
			} catch (ScriptException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
		}

		// 获取 json 值，可能为一个或多个或没有此键
		static String getJsonValue(JsonObject jsonObject, String jsonKey) {
			String line = "";

			JsonElement jsonValueElement = jsonObject.get(jsonKey);
			if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
				line = "";
			} else if (jsonValueElement.isJsonArray()) {
				for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
					line += jEle.getAsString().trim() + ";";
				}
			} else {
				line = jsonValueElement.getAsString().trim();
			}
			line = line.replaceAll(";$", "").trim(); // 去掉末尾的分号

			return line;
		}

		// 获取原始网页展示的引文
		static String getRefertext(String jsonText) {
			String html = "";
			String refer_text_site = "";

			Invocable invoke = (Invocable) engine;
			try {
				html = (String) invoke.invokeFunction("joinRef", jsonText);
				refer_text_site = Jsoup.parse(html).text().trim();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

			return refer_text_site;
		}

		/*
		 * 
		 * { "Tutor": null, "Issue": "", "Periodical": null, "Degree": null,
		 * "TitleInfo": null, "Orgnization": null, "Symbol": "Unknown", "Title":
		 * "激光光谱学:基本概念和仪器手段", "AuthorInfo": null, "PeriodicalCode": null, "Page":
		 * "29-40", "Publisher": "{H}北京:科学出版社", "Volumn": "", "SerialNum": 11, "School":
		 * null, "Type": "M", "Year": "1989", "HasFulltext": false, "ArticaleId":
		 * "wlxb201403012^11", "Author": "戴姆特瑞德W%严光耀%沈珊雄%夏慧荣", "DOI": null }
		 */
		// 处理一个item，即一条引文
		public static void procOneItem(JsonElement refJsonElement) {
			{
//				lngid = "";			// lngid 在 reduce 中生成
				sub_db_id = "00004";
				product = "WANFANG";
				sub_db = "CSPD";
				provider = "WANFANG";
//				down_date = "";		// 无需重置
//				batch = "";			// 无需重置
				doi = "";
				title = "";
				title_alt = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				raw_type = "";
				author_1st = "";
				author = "";
				author_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				publisher = "";
//				cited_id = "";	// 无需重置
				linked_id = "";
				refer_text_raw = "";
				refer_text_raw_alt = "";
				refer_text_site = "";
				refer_text_site_alt = "";
				refer_text = "";
				refer_text_alt = "";
				source_name = "";
				source_name_alt = "";
				strtype = "";
			}
			System.out.println(refJsonElement);

			JsonObject refJsonObject = refJsonElement.getAsJsonObject();

			String Tutor = getJsonValue(refJsonObject, "Tutor");
			String Issue = getJsonValue(refJsonObject, "Issue");
			String Periodical = getJsonValue(refJsonObject, "Periodical");
			String Degree = getJsonValue(refJsonObject, "Degree");
			String TitleInfo = getJsonValue(refJsonObject, "TitleInfo");
			String Orgnization = getJsonValue(refJsonObject, "Orgnization");
			String Symbol = getJsonValue(refJsonObject, "Symbol");
			String Title = getJsonValue(refJsonObject, "Title");
			String AuthorInfo = getJsonValue(refJsonObject, "AuthorInfo");
			String PeriodicalCode = getJsonValue(refJsonObject, "PeriodicalCode");
			String Page = getJsonValue(refJsonObject, "Page");
			String Publisher = getJsonValue(refJsonObject, "Publisher");
			String Volumn = getJsonValue(refJsonObject, "Volumn");
			String SerialNum = getJsonValue(refJsonObject, "SerialNum");
			String School = getJsonValue(refJsonObject, "School");
			String Type = getJsonValue(refJsonObject, "Type");
			String Year = getJsonValue(refJsonObject, "Year");
			String HasFulltext = getJsonValue(refJsonObject, "HasFulltext");
			String ArticaleId = getJsonValue(refJsonObject, "ArticaleId");
			String Author = getJsonValue(refJsonObject, "Author");
			String DOI = getJsonValue(refJsonObject, "DOI");

			title = Title.trim();
			int idx = title.indexOf('%');
			if (idx > 0) {
				title = title.substring(0, idx).trim();
			}
			strtype = Type.trim();
			source_name = Periodical.trim();
			author = Author.replace('%', ';');
			pub_year = Year;
			vol = Volumn;
			num = Issue;
			String stryearvolnum = Year + "," + Volumn;
			if (Issue.length() > 0) {
				stryearvolnum += "(" + Issue + ")";
			}
			publisher = Publisher.trim();
			page_info = Page.trim();

			doi = DOI.trim();
			if (ArticaleId.indexOf('^') < 0) {
				linked_id = ArticaleId;
			}

			refer_text_raw = refJsonObject.toString().trim();
//			if (strtype.equals("J")) {
//				if ((source_name.length() < 1) && (publisher.length() > 0)) {
//					source_name = publisher;
//					publisher = "";
//				}
//				refer_text_site = Author.replace('%', ',') + "." + title + "[" + strtype + "].";
//				if (source_name.length() > 0) {
//					refer_text_site += source_name + ",";
//				}
//				refer_text_site += stryearvolnum;
//				if (Page.length() > 0) {
//					refer_text_site += ":" + Page;
//				}
//				if (doi.length() > 0) {
//					refer_text_site += ".doi:" + doi;
//				}
//				refer_text_site += ".";
//			} else {
//				refer_text_site = getRefertext(refer_text_raw).trim();
//			}
			refer_text_site = getRefertext(refer_text_raw).trim();

//			System.out.println("ArticaleId:" + ArticaleId);
//			System.out.println("cited_raw_id:" + cited_raw_id);
//			System.out.println("refer_text_site:" + refer_text_site);
//			System.out.println("strtitle:" + strtitle);
//			System.out.println("strtype:" + strtype);
//			System.out.println("source_name:" + source_name);
//			System.out.println("author:" + author);
//			System.out.println("stryearvolnum:" + stryearvolnum);
//			System.out.println("strpubwriter:" + strpubwriter);
//			System.out.println("strpages:" + strpages);
//			System.out.println("doi:" + doi);
//			System.out.println("linked_id:" + linked_id);
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>() {
			}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(text, type);

			// 下载日期
			down_date = mapField.get("down_date").getAsString().trim();
			// 参考文献所在文章ID
			String cited_raw_id = mapField.get("rawid").getAsString().trim();
			if (cited_raw_id.length() < 1) {
				context.getCounter("map", "cited_raw_id.length() < 1").increment(1);
				return;
			}
			cited_id = VipIdEncode.getLngid(sub_db_id, cited_raw_id, false) + "@" + cited_raw_id;

			JsonArray refJsonArray = mapField.get("ref").getAsJsonArray();

			for (JsonElement refJsonElement : refJsonArray) {
				procOneItem(refJsonElement);

				if (refer_text_site.length() < 1) {
					context.getCounter("map", "refer_text_site.length() < 1").increment(1);
					LogMR.log2HDFS4Mapper(context, logHDFSFile, "refer_text_site.length() < 1\n" + text);
					continue;
				}

				XXXXObject xObj = new XXXXObject();
				{
//					xObj.data.put("lngid", lngid);		// lngid 在 reduce 中生成
					xObj.data.put("sub_db_id", sub_db_id);
					xObj.data.put("product", product);
					xObj.data.put("sub_db", sub_db);
					xObj.data.put("provider", provider);
					xObj.data.put("down_date", down_date);
					xObj.data.put("batch", batch);
					xObj.data.put("doi", doi);
					xObj.data.put("title", title);
					xObj.data.put("title_alt", title_alt);
					xObj.data.put("page_info", page_info);
					xObj.data.put("begin_page", begin_page);
					xObj.data.put("end_page", end_page);
					xObj.data.put("jump_page", jump_page);
					xObj.data.put("raw_type", raw_type);
					xObj.data.put("author_1st", author_1st);
					xObj.data.put("author", author);
					xObj.data.put("author_alt", author_alt);
					xObj.data.put("pub_year", pub_year);
					xObj.data.put("vol", vol);
					xObj.data.put("num", num);
					xObj.data.put("publisher", publisher);
					xObj.data.put("cited_id", cited_id);
					xObj.data.put("linked_id", linked_id);
					xObj.data.put("refer_text_raw", refer_text_raw);
					xObj.data.put("refer_text_raw_alt", refer_text_raw_alt);
					xObj.data.put("refer_text_site", refer_text_site);
					xObj.data.put("refer_text_site_alt", refer_text_site_alt);
					xObj.data.put("refer_text", refer_text);
					xObj.data.put("refer_text_alt", refer_text_alt);
					xObj.data.put("source_name", source_name);
					xObj.data.put("source_name_alt", source_name_alt);
					xObj.data.put("strtype", strtype);
				}

				context.getCounter("map", "count").increment(1);

				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(cited_raw_id), new BytesWritable(bytes));
			}
		}
	}

}
