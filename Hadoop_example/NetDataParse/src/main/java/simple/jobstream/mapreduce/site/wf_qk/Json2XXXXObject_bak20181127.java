package simple.jobstream.mapreduce.site.wf_qk;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

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

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject_bak20181127 extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 100;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "wf_qk." + this.getClass().getSimpleName();
		
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

		private static int cnt = 0;
		private static String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

		private static String rawid = "";
		private static String pykm = "";
		private static String issn = "";
		private static String cnno = "";
		private static String title_c = "";
		private static String title_e = "";
		private static String remark_c = "";
		private static String remark_e = "";
		private static String doi = "";
		private static String author_c = "";
		private static String author_e = "";
		private static String organ = "";
		private static String name_c = "";
		private static String name_e = "";
		private static String years = "";
		private static String vol = "";
		private static String num = "";
		private static String sClass = "";
		private static String auto_class = "";
		private static String keyword_c = "";
		private static String keyword_e = "";
		private static String imburse = "";
		private static String pageline = ""; // 页码信息
		private static String pagecount = "";
//		private static String ref_cnt = "";
//		private static String cited_cnt = "";
		private static String muinfo = ""; // 栏目
		private static String pub1st = "0"; // 是否优先出版
		private static String src_db = "null";
		private static String down_date = "";	// 下载日期

		// 清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';'); // 全角转半角
			text = text.replaceAll("\\s*;\\s*", ";"); // 去掉分号前后的空白
			text = text.replaceAll("\\s*\\[\\s*", "["); // 去掉[前后的空白
			text = text.replaceAll("\\s*\\]\\s*", "]"); // 去掉]前后的空白
			text = text.replaceAll("[\\s;]+$", ""); // 去掉最后多余的空白和分号

			return text;
		}

		// 清理class，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanClass(String text) {
			text = text.replace("{", "").replace("}", "").trim();

			return text;
		}

		// 获取 json 值，可能为一个或多个或没有此键
		String getJsonValue(JsonObject articleJsonObject, String jsonKey) {
			String line = "";

			JsonElement jsonValueElement = articleJsonObject.get(jsonKey);
			if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
				line = "";
			} else if (jsonValueElement.isJsonArray()) {
				for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
					line += jEle.getAsString().trim() + ";";
				}
			} else {
				line = jsonValueElement.getAsString().trim();
			}
			line = line.replaceAll(";$", ""); // 去掉末尾的分号
			line = line.trim();

			return line;
		}

		// 解析作者和机构，得到对应关系
		void parseAuthorOrgan(JsonObject articleJsonObject) {
			// authors_name, authors_unit, authorsandunit
			// zhxxgb, 2018, 5 适合做样例
			
			author_c = organ = "";
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
						String unit = line.substring(idx+1).trim();
						mapAuthorOrgan.put(author, unit);
					}
				}
				
				int idx = 0;
				for (JsonElement jEle : authorsElement.getAsJsonArray()) {
					String author = jEle.getAsString().trim();					
					author_c += author;
					if (mapAuthorOrgan.containsKey(author)) {
						if (!mapOrganNo.containsKey(mapAuthorOrgan.get(author))) {
							++idx;
							organ += "[" + idx + "]" + mapAuthorOrgan.get(author) + ";";
							mapOrganNo.put(mapAuthorOrgan.get(author), "[" + idx + "]");
						}
						
						author_c += mapOrganNo.get(mapAuthorOrgan.get(author));
					}
					author_c += ";";					
				}
			}
			else {
				author_c = getJsonValue(articleJsonObject, "authors_name");
				organ = getJsonValue(articleJsonObject, "authors_unit");
			}	

			author_c = author_c.replaceAll(";$", "").trim();
			organ = organ.replaceAll(";$", "").trim();
		} 
		
		public boolean parseArticle(JsonElement articleJsonElement) {
			{
				rawid = "";
				pykm = "";
				issn = "";
				cnno = "";
				title_c = "";
				title_e = "";
				remark_c = "";
				remark_e = "";
				doi = "";
				author_c = "";
				author_e = "";
				organ = "";
				name_c = "";
				name_e = "";
				years = "";
				vol = "";
				num = "";
				sClass = "";
				auto_class = "";
				keyword_c = "";
				keyword_e = "";
				imburse = "";
				pageline = "";
				pagecount = "";
//				ref_cnt = "";
//				cited_cnt = "";
				pub1st = "0";
				src_db = "null";
			}

			JsonObject articleJsonObject = articleJsonElement.getAsJsonObject();

			rawid = getJsonValue(articleJsonObject, "article_id");
			pykm = getJsonValue(articleJsonObject, "perio_id");
			issn = getJsonValue(articleJsonObject, "issn");
			cnno = getJsonValue(articleJsonObject, "cn");
			title_c = getJsonValue(articleJsonObject, "title");
			if ((title_c.indexOf("<em>") > -1) && (title_c.indexOf("</em>") > -1)) {
				title_c = title_c.replaceAll("<em>", "").replaceAll("</em>", "");
			}
			
			title_e = getJsonValue(articleJsonObject, "trans_title");
			remark_c = getJsonValue(articleJsonObject, "summary");
			remark_e = getJsonValue(articleJsonObject, "trans_abstract");
			doi = getJsonValue(articleJsonObject, "doi");
//			author_c = getJsonValue(articleJsonObject, "authors_name");
			author_e = getJsonValue(articleJsonObject, "trans_authors");
//			organ = getJsonValue(articleJsonObject, "authors_unit");
			parseAuthorOrgan(articleJsonObject);			
			name_c = getJsonValue(articleJsonObject, "perio_title");
			name_e = getJsonValue(articleJsonObject, "perio_title_en");
			years = getJsonValue(articleJsonObject, "publish_year");
			vol = getJsonValue(articleJsonObject, ""); // TODO
			num = getJsonValue(articleJsonObject, "issue_num");
			sClass = getJsonValue(articleJsonObject, "orig_classcode").replace('%', ' ');
			auto_class = getJsonValue(articleJsonObject, "auto_classcode"); // 机标分类号
			keyword_c = getJsonValue(articleJsonObject, "keywords");
			keyword_e = getJsonValue(articleJsonObject, "trans_keys");
			imburse = getJsonValue(articleJsonObject, "fund_info");
			pageline = getJsonValue(articleJsonObject, "page_range");
			pagecount = getJsonValue(articleJsonObject, "page_cnt");
//			ref_cnt = getJsonValue(articleJsonObject, "refdoc_cnt");		// 与详情页不一致，靠不住
//			cited_cnt = getJsonValue(articleJsonObject, "cited_cnt");		// 与详情页不一致，靠不住
			muinfo = getJsonValue(articleJsonObject, "column_name");
			pub1st = getJsonValue(articleJsonObject, ""); // TODO
			src_db = getJsonValue(articleJsonObject, "source_db");

//			 System.out.println("article_id: " + rawid);
//			 System.out.println("pykm: " + pykm);
//			 System.out.println("title_c: " + title_c);
//			 System.out.println("title_e: " + title_e);
//			 System.out.println("remark_c: " + remark_c);
//			 System.out.println("remark_e: " + remark_e);
//			 System.out.println("doi: " + doi);
//			 System.out.println("author_c: " + author_c);
//			 System.out.println("author_e: " + author_e);
//			 System.out.println("organ: " + organ);
//			 System.out.println("name_c: " + name_c);
//			 System.out.println("name_e: " + name_e);
//			 System.out.println("years: " + years);
//			 System.out.println("vol: " + vol);
//			 System.out.println("num: " + num);
//			 System.out.println("sClass: " + sClass);
//			 System.out.println("auto_class: " + auto_class);
//			 System.out.println("keyword_c: " + keyword_c);
//			 System.out.println("keyword_e: " + keyword_e);
//			 System.out.println("imburse: " + imburse);
//			 System.out.println("pageline: " + pageline);
//			 System.out.println("muinfo: " + muinfo);
//			 System.out.println("pub1st: " + pub1st);
//			 System.out.println("src_db: " + src_db);
//			 System.out.println("*****************************\n");

			return true;
		}

		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
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

			JsonArray pageRowJsonArray = mapField.get("pageRow")
					.getAsJsonArray();

			for (JsonElement articleJsonElement : pageRowJsonArray) {
				parseArticle(articleJsonElement); // 解析一行json
				if ((title_c.length() < 1) && (title_e.length() < 1)) {
					context.getCounter("map", "error: no title").increment(1);
//					log2HDFSForMapper(context, "error: no title: " + rawid);
					LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
					continue;
				}
				
				context.getCounter("map", src_db).increment(1);
				// 多个时以分号分隔：WF;CNKI
				if (src_db.indexOf("WF") < 0) {
					continue;
				}
				
				{
					XXXXObject xObj = new XXXXObject();
					xObj.data.put("rawid", rawid);
					xObj.data.put("pykm", pykm);
					xObj.data.put("issn", issn);
					xObj.data.put("cnno", cnno);
					xObj.data.put("title_c", title_c);
					xObj.data.put("title_e", title_e);
					xObj.data.put("remark_c", remark_c);
					xObj.data.put("remark_e", remark_e);
					xObj.data.put("doi", doi);
					xObj.data.put("author_c", author_c);
					xObj.data.put("author_e", author_e);
					xObj.data.put("organ", organ);
					xObj.data.put("name_c", name_c);
					xObj.data.put("name_e", name_e);
					xObj.data.put("years", years);
					xObj.data.put("vol", vol);
					xObj.data.put("num", num);
					xObj.data.put("sClass", sClass);
					xObj.data.put("auto_class", auto_class);
					xObj.data.put("keyword_c", keyword_c);
					xObj.data.put("keyword_e", keyword_e);
					xObj.data.put("imburse", imburse);
					xObj.data.put("pageline", pageline);
					xObj.data.put("pagecount", pagecount);
					xObj.data.put("muinfo", muinfo);
					xObj.data.put("pub1st", pub1st);
					xObj.data.put("down_date", down_date);
					xObj.data.put("parse_time", (new SimpleDateFormat(
							"yyyy-MM-dd_kk:mm:ss")).format(new Date()));

					context.getCounter("map", "count").increment(1);
					

					byte[] bytes = VipcloudUtil.SerializeObject(xObj);
					context.write(new Text(rawid), new BytesWritable(bytes));
				}
			}
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
