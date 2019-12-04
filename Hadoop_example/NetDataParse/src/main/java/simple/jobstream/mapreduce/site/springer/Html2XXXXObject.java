package simple.jobstream.mapreduce.site.springer;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
//import org.apache.tools.ant.taskdefs.Length;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import org.junit.Test;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {

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
		
		static int cnt = 0;
		private static String batch = "";
		private static String rawid = "";
		private static String identifier_doi = "";
		private static String title = "";
		private static String identifier_pissn = "";
		private static String identifier_eissn = "";
		private static String creator = "";
		private static String creator_institution = "";
		private static String source = "";
		private static String publisher = "";
		private static String volume = "";
		private static String issue = "";
		private static String description = "";
		private static String subject = "";
		private static String firstPage = "";
		private static String lastPage = "";
		private static String date_created = "";
		private static String down_date = "";
		private static String journalId = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}

		// String doi, String jid,
		public void parseHtml(String htmlText) {

			Document doc = Jsoup.parse(htmlText.toString());
			// Element titleElement = doc.select("meta[name = citation_title]").first();
			Element titleElement = doc.select("meta[property = og:title]").first();
			Element firstPageElement = doc.select("meta[name = citation_firstpage]").first();
			Element lastPageElement = doc.select("meta[name = citation_lastpage]").first();
			Element sourceElement = doc.select("meta[name = citation_journal_title").first();
			Element volumeElement = doc.select("meta[name = citation_volume").first();
			Element issueElement = doc.select("meta[name = citation_issue").first();
			Element createdDateElement = doc.select("meta[name = citation_cover_date]").first();
			Element publisherElement = doc.select("meta[name = citation_publisher]").first();
			Elements subjectElements = doc.select("span[class = Keyword]");
			Element descriptionElement = doc.select("p[class = Para]").first();
			Element eissnElement = doc.select("span[id = electronic-issn]").first();
			Element pissnElement = doc.select("span[id = print-issn]").first();
			// Element authorElement = doc.select("div[class = content authors-affiliations
			// u-interface]").first();
			Element authorElement = doc.select("div.authors-affiliations.u-interface").first();
			Elements authorIndexElements = doc.select("li[data-affiliation^=#affiliation]");
			Element authorMetaElement = doc.select("meta[name = citation_author]").first();

			if (authorIndexElements != null) {
				for (Element e : authorIndexElements) {
					e.prepend("[");
					e.append("]");
				}
			}
			if (authorElement != null) {
				Elements creatorElements = authorElement.select("li:has(ul)");
				Element authorInstitutionElement = authorElement.select("ol[class =test-affiliations]").first();
				for (Element e : creatorElements) {
					creator = creator + e.text().replace("\\[", ",").replace("\\]", ",").replaceAll("(\\d+)", "[$1]") + ";";
					creator = creator.replace("Email author", "");
				}

				// 以前版本
//						if (authorInstitution != null) {
//							creator_institution = authorInstitution.text().trim();
//							creator_institution = creator_institution.replace("1.", "[1]").replace("2.", ";[2]")
//									.replace("3.", ";[3]").replace("4.", ";[4]").replace("5.", ";[5]")
//									.replace("6.", ";[6]").replace("7.", ";[7]").replace("8.", "[8]")
//									.replace("9.", "[9]");
//						}
//						if (creator != null) {
//							creator = creator.replace("1", "[1]").replace("2", "[2]").replace("3", ";[3]")
//									.replace("4", "[4]").replace("5", "[5]").replace("6", "[6]").replace("7", "[7]")
//									.replace("8", "[8]").replace("9", "[9]");
//							creator = creator.substring(0, creator.length() - 1);
//						}
				if (authorInstitutionElement != null) {

					Elements authorInstitutionElements = authorInstitutionElement.select("li[class =affiliation]");

					for (Element InstitutionElement : authorInstitutionElements) {
						creator_institution = creator_institution
								+ InstitutionElement.text().replace(";", ",").replaceAll("^(\\d+)\\.", "[$1]") + ";";

					}
					creator_institution = creator_institution.substring(0, creator_institution.length() - 1);

				}

				if (creator.length() > 0) {
					creator = creator.replaceAll("] \\[", ",");

					creator = creator.substring(0, creator.length() - 1);
				}

			}
			if (creator.length() == 0) {
				if (authorMetaElement != null) {
					creator = authorMetaElement.attr("content").trim();
				} else {
					creator = "";
				}
			}

			if (titleElement != null) {
				// title = titleElement.attr("content").trim();
				String title_raw = titleElement.attr("content").trim();
				title = title_raw.replaceAll("\\$\\$(?:(?!\\$\\$).)*?\\$\\$", "").replaceAll("</?[^>]+>", "");
				if (title.contains("$")) {
					title = title.replaceAll("\\$.*?\\$", "");
				}

				if (title.contains("<")) {
					title = title.replaceAll("</?[^>]+>", "");
				}
			} else {
				// 获取新节点，获取标题ArticleTitle
				Element pubtitleElement = doc.select("h1[class = ArticleTitle]").first();

				// 获取字符串

				if (pubtitleElement != null) {
					title = pubtitleElement.text().replaceAll("\\$\\$(?:(?!\\$\\$).)*?\\$\\$", "")
							.replaceAll("</?[^>]+>", "").replaceAll("\\$.*?\\$", "").replaceAll("</?[^>]+>", "");

				}

			}
			if (firstPageElement != null) {
				firstPage = firstPageElement.attr("content").trim();
				lastPage = firstPage;
			}
			if (lastPageElement != null) {
				lastPage = lastPageElement.attr("content").trim();
			}
			if (sourceElement != null) {
				source = sourceElement.attr("content").trim();
			}

			if (createdDateElement != null) {
				date_created = createdDateElement.attr("content").trim();
			}
			if (publisherElement != null) {
				publisher = publisherElement.attr("content").trim();
			}

			if (volumeElement != null) {
				volume = volumeElement.attr("content").trim();
			}
			if (issueElement != null) {
				issue = issueElement.attr("content").trim();
			}

			if (pissnElement != null) {
				identifier_pissn = pissnElement.text().trim();
			}
			if (eissnElement != null) {
				identifier_eissn = eissnElement.text().trim();
			}
			if (subjectElements != null) {
				for (Element e : subjectElements) {
					subject = subject + e.text().trim() + ";";
				}
				if (subject.length() > 1) {

					subject = subject.substring(0, subject.length() - 1);
				}
			}
			if (descriptionElement != null) {
				description = descriptionElement.text().trim();
			}

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				rawid = "";
				identifier_doi = "";
				title = "";
				identifier_pissn = "";
				identifier_eissn = "";
				creator = "";
				creator_institution = "";
				source = "";
				publisher = "";
				volume = "";
				issue = "";
				description = "";
				subject = "";
				firstPage = "";
				lastPage = "";
				date_created = "";
				down_date = "20181205";		// 因为有些历史数据没有将 down_date 写入json
				journalId = "";
			}

			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);

			identifier_doi = rawid = mapField.get("doi").toString();
			journalId = mapField.get("jid").toString();
			String htmlText = mapField.get("html").toString();
			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").toString();
			}
			

			if (!htmlText.toLowerCase().contains("citation_title")) {
				context.getCounter("map", "Error: citation_title").increment(1);
				return;
			}

			parseHtml(htmlText);
			
			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("identifier_doi", identifier_doi);
			xObj.data.put("title", title);
			xObj.data.put("identifier_pissn", identifier_pissn);
			xObj.data.put("identifier_eissn", identifier_eissn);
			xObj.data.put("creator", creator);
			xObj.data.put("creator_institution", creator_institution);
			xObj.data.put("source", source);
			xObj.data.put("publisher", publisher);
			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("description", description);
			xObj.data.put("subject", subject);
			xObj.data.put("firstPage", firstPage);
			xObj.data.put("lastPage", lastPage);
			xObj.data.put("date_created", date_created);
			xObj.data.put("journalId", journalId);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);

			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
