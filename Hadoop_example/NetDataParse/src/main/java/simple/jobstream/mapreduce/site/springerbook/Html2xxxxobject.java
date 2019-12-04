package simple.jobstream.mapreduce.site.springerbook;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2xxxxobject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "springerbook." + this.getClass().getSimpleName();

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

		public static HashMap<String, String> parseNewHtml(String htmlText) {

			String title = "";
			String title_sub = "";
			String title_series = "";
			String identifier_eisbn = "";
			String identifier_pisbn = "";
			String identifier_doi = "";
			String creator = "";
			String creator_institution = "";
			String source_fl = "";
			String publisher = "";
			String date = "";
			String volume = "";
			String description = "";
			String subject = "";
			String subject_dsa = "";
			String identifier_pissn = "";
			String identifier_eissn = "";
			String down_date ="20181208";
			
			String parse_time = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
		
			HashMap<String, String> map = new HashMap<String, String>();
			Document doc = Jsoup.parse(htmlText);
			identifier_doi = htmlText.split("★")[0];

			Element titleElement = doc.select("div[class = page-title]").first();
			if (titleElement != null) {
				Element h1 = titleElement.select("h1").first();
				title = h1.text();
			}

			Element subtitleElement = doc.select("h2[class = page-title__subtitle]").first();
			if (subtitleElement != null) {
				title_sub = subtitleElement.text().trim();
			}
			Element dateElement = doc.select("strong[class = conference-acronym").last();
			if (dateElement != null) {
				date = dateElement.text().trim();
				date = date.split(" ")[1];
			}
			Element series_titleElement = doc.select("a[class = gtm-book-series-link]").first();
			if (series_titleElement != null) {
				title_series = series_titleElement.text().trim();
			}

			Element descriptionElement = doc.select("div[id = book-description]").first();
			if (descriptionElement != null) {
				description = descriptionElement.text().trim();
			}

			Element pisbnElement = doc.select("span[id = print-isbn]").first();
			if (pisbnElement != null) {
				identifier_pisbn = pisbnElement.text().trim();
			}

			Element eisbnElement = doc.select("span[id = electronic-isbn]").first();
			if (eisbnElement != null) {
				identifier_eisbn = eisbnElement.text().trim();
			}

			Element pissnElement = doc.select("span[id = print-issn]").first();
			if (pissnElement != null) {
				identifier_pissn = pissnElement.text().trim();
			}

			Element eissnElement = doc.select("span[id = electronic-issn]").first();
			if (eissnElement != null) {
				identifier_eissn = eissnElement.text().trim();
			}
			Element volumeElement = doc.select("span[id = test-AbbreviationVolumeNumber]").first();
			if (volumeElement != null) {
				volume = volumeElement.text().trim();
				try {
					volume = volume.split(",")[1].replace("volume", "").replace(")", "").trim();
				} catch (Exception e) {
					volume = "";
				}

			}

			Element publisherElement = doc.select("span[id = publisher-name]").first();
			if (publisherElement != null) {
				publisher = publisherElement.text().trim();
			}

			Elements subjectElement = doc.select("span[class = Keyword]");
			if (subjectElement != null) {
				for (Element ele : subjectElement) {
					subject = subject + ele.text().trim() + ";";
				}

				if (subject.length() > 0) {
					subject = subject.substring(0, subject.length() - 1);
				}
			}

			Element authorElement = doc.select("div[class = authors-affiliations u-interface]").first();
			Element authorMetaElement = doc.select("meta[name = citation_author]").first();

			if (authorElement != null) {
				Elements creatorElements = authorElement.select("li:has(ul)");
				Element authorInstitution = authorElement.select("ol").first();
				for (Element e : creatorElements) {
					creator = creator + e.text().trim() + ";";
					creator = creator.replace("Email author", "");
				}
				if (authorInstitution != null) {
					creator_institution = authorInstitution.text().trim();
					creator_institution = creator_institution.replace("1.", "[1]").replace("2.", ";[2]")
							.replace("3.", ";[3]").replace("4.", ";[4]").replace("5.", ";[5]").replace("6.", ";[6]")
							.replace("7.", ";[7]").replace("8.", "[8]").replace("9.", "[9]");
				}
				if (creator.length() > 1) {
					creator = creator.replace("1", "[1]").replace("2", "[2]").replace("3", "[3]").replace("4", "[4]")
							.replace("5", "[5]").replace("6", "[6]").replace("7", "[7]").replace("8", "[8]")
							.replace("9", "[9]");
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
			if (map != null) {

				map.put("title", title);
				map.put("title_sub", title_sub);
				map.put("title_series", title_series);
				map.put("identifier_doi", identifier_doi);
				map.put("volume", volume);
				map.put("date", date);
				map.put("publisher", publisher);
				map.put("identifier_pisbn", identifier_pisbn);
				map.put("identifier_eisbn", identifier_eisbn);
				map.put("subject", subject);
				map.put("creator", creator);
				map.put("creator_institution", creator_institution);
				map.put("subject_dsa", subject_dsa);
				map.put("identifier_pissn", identifier_pissn);
				map.put("identifier_eissn", identifier_eissn);
				map.put("description", description);
				map.put("down_date", down_date);
				map.put("parse_time", parse_time);

			}
			return map;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			HashMap<String, String> map = new HashMap<String, String>();
			map = parseNewHtml(value.toString());
			if (map.get("identifier_doi").equals("")) {
				context.getCounter("map", "not find identifier_doi").increment(1);
				return;
			}
			if (map.get("title").equals("")) {
				context.getCounter("map", "not find title").increment(1);
				return;
			}

			if (map != null) {
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("title", map.get("title"));
				xObj.data.put("title_sub", map.get("title_sub"));
				xObj.data.put("title_series", map.get("title_series"));
				xObj.data.put("identifier_doi", map.get("identifier_doi"));
				xObj.data.put("volume", map.get("volume"));
				xObj.data.put("date", map.get("date"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("publisher", map.get("publisher"));
				xObj.data.put("identifier_pisbn", map.get("identifier_pisbn"));
				xObj.data.put("identifier_eisbn", map.get("identifier_eisbn"));
				xObj.data.put("subject", map.get("subject"));
				xObj.data.put("subject_dsa", map.get("subject_dsa"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("creator_institution", map.get("creator_institution"));
				xObj.data.put("identifier_pissn", map.get("identifier_pissn"));
				xObj.data.put("identifier_eissn", map.get("identifier_eissn"));
				xObj.data.put("down_date", map.get("down_date"));
				xObj.data.put("parse_time", map.get("parse_time"));
				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("identifier_doi")), new BytesWritable(bytes));
				context.getCounter("map", "count").increment(1);
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
