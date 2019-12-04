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

//import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject2_new extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 60;
	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "springerlink." + this.getClass().getSimpleName();

		job.setJobName(jobName);
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
//		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
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
//		job.setReducerClass(UniqXXXXObjectReducer.class);
		job.setReducerClass(ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
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
		String batchs = "";
//		public void setup(Context context) throws IOException, InterruptedException {
//			batchs = context.getConfiguration().get("batch");
//			context.getCounter("batch",batchs).increment(1);
//		}

		public HashMap<String, String> parseHtml(String doi, String jid, String htmlText) {
			HashMap<String, String> map = new HashMap<String, String>();
			String rawid = doi;
			String identifier_doi = "";
			String title = "";
			String identifier_pissn = "";
			String identifier_eissn = "";
			String creator = "";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String date = "";
			String volume = "";
			String issue = "";
			String description = "";
			String subject = "";
			String firstPage = "";
			String lastPage = "";
			String date_created = "";
//			String batch = batchs;
			String parse_time = (new SimpleDateFormat("yyyyMMdd")).format(new Date());

//			String journal_Id = journal_id;

			// 注意下次更新的时候，这个值应当是bigjson传入的
			String down_date = "20181204";

			String journalId = jid;

			if (htmlText.contains("citation_title")) {

				String text = htmlText;
				try {
					Document doc = Jsoup.parse(text.toString());
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
					Element journalIdElement = doc.select("a[class = test-cover-link]").first();
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
							creator = creator + e.text().replaceAll("(\\d+)", "[$1]") + ";";
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

							Elements authorInstitutionElements = authorInstitutionElement
									.select("li[class =affiliation]");

							for (Element InstitutionElement : authorInstitutionElements) {
								creator_institution = creator_institution
										+ InstitutionElement.text().replace(";", ",").replaceAll("^(\\d+)\\.", "[$1]")
										+ ";";

							}
							creator_institution = creator_institution.substring(0, creator_institution.length() - 1);

						}

						if (creator != null) {
							creator = creator.replaceAll("] [", ",");
						
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
//					else if (journalIdElement != null) {
//						journalId = journalIdElement.select("a").first().attr("href").replace("/journal/", "").trim();
//					}

				} catch (Exception e) {
					e.printStackTrace();
				}
//				if (title.length() == 0) {
//					return null;
//				}
				map.put("rawid", rawid);
				map.put("title", title);
				map.put("creator", creator);
				map.put("creator_institution", creator_institution);
				map.put("subject", subject);
				map.put("firstPage", firstPage);
				map.put("lastPage", lastPage);
				map.put("source", source);
				map.put("volume", volume);
				map.put("issue", issue);
				map.put("identifier_pissn", identifier_pissn);
				map.put("identifier_eissn", identifier_eissn);
				map.put("publisher", publisher);
				map.put("date_created", date_created);
				map.put("description", description);
				map.put("journalId", journalId);
				map.put("down_date", down_date);
				map.put("parse_time", parse_time);

//				map.put("journal_Id", journal_Id);

			} else {
				map = null;
				

			}
			return map;

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			String text = value.toString().trim();

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, Object>>() {
			}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);

			String rawid = mapField.get("doi").toString();
			String jid = mapField.get("jid").toString();
//			String journal_id = mapField.get("jid").toString();
			String htmlText = mapField.get("html").toString();
//			String down_date_bigjson = mapField.get("down_date").toString();

			HashMap<String, String> map = new HashMap<String, String>();
			map = parseHtml(rawid, jid, htmlText);

			if (map != null) {
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("identifier_doi", map.get("rawid"));
				xObj.data.put("title", map.get("title"));
				xObj.data.put("identifier_pissn", map.get("identifier_pissn"));
				xObj.data.put("identifier_eissn", map.get("identifier_eissn"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("creator_institution", map.get("creator_institution"));
				xObj.data.put("source", map.get("source"));
				xObj.data.put("publisher", map.get("publisher"));
				xObj.data.put("volume", map.get("volume"));
				xObj.data.put("issue", map.get("issue"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("subject", map.get("subject"));
				xObj.data.put("firstPage", map.get("firstPage"));
				xObj.data.put("lastPage", map.get("lastPage"));
				xObj.data.put("date_created", map.get("date_created"));
				xObj.data.put("journalId", map.get("journalId"));
				xObj.data.put("down_date", map.get("down_date"));
				xObj.data.put("parse_time", map.get("parse_time"));
//				xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));

				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));
				context.getCounter("map", "count").increment(1);
			} else {
				context.getCounter("map", "null").increment(1);

				return;
			}
		}
	}

	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {

			BytesWritable bOut = new BytesWritable(); // 用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) { // 选最大的一个
					bOut = item;
				}
			}

			context.getCounter("reduce", "count").increment(1);

			context.write(key, bOut);
		}
	}
}
