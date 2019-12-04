package simple.jobstream.mapreduce.site.aps;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.AuthorOrgan;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "Html2XXXXObject";
		if (testRun) {
			jobName = "test_" + jobName;
		}
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
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	// 继承Mapper接口,设置map的输入类型为<Object,Text>
	// 输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public static boolean isNumeric(String str) {
			Pattern pattern = Pattern.compile("[0-9]+");
			return pattern.matcher(str).matches();
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();

			String url = mapJson.get("url").trim();
			String vol = mapJson.get("vol").trim();
			String issue = mapJson.get("issue").trim();
			String html = mapJson.get("detail").trim();
			String authorhtml = mapJson.get("authors").trim();

			Document doc = Jsoup.parse(html);
			Document authordoc = Jsoup.parse(authorhtml);

			String creator = "";
			String title = "";
			String date_created = "";
			String provider_subject = "";
			String description = "";
			String doi = "";
			String beginpage = "";
			String endpage = "";
			String insitution = "";
			String source = "";
			String subject = "";
			String eissn = "";
			String pissn = "";

//            Element titleTag = doc.select("meta[name*=citation_title]").first();
			Element titleTag = doc.select("div.medium-9.columns > h3").first();

			if (titleTag != null) {
				title = titleTag.text();
			}
			Element dateTag = doc.select("meta[name*=citation_date]").first();
			if (dateTag != null) {
				date_created = dateTag.attr("content").replace("/", "");
			}
			Element bgPageTag = doc.select("meta[name*=citation_firstpage]").first();
			if (bgPageTag != null) {
				beginpage = bgPageTag.attr("content");
			}
			Element edPageTag = doc.select("meta[name*=citation_lastpage]").first();
			if (edPageTag != null) {
				endpage = edPageTag.attr("content");
			}
			Element doiTag = doc.select("meta[name*=citation_doi]").first();
			if (doiTag != null) {
				doi = doiTag.attr("content");
			}
			Element sourceTag = doc.select("meta[name*=citation_journal_title]").first();
			if (sourceTag != null) {
				source = sourceTag.attr("content");
			}

			Element descriptionTag = doc.select("section.article.open.abstract > div > p").first();
			if (descriptionTag != null) {
				description = descriptionTag.text();
			}
			if (description.startsWith("DOI:")) {
				description = "";
			}

			for (Element authorTag : authordoc.select("a")) {
				String author = authorTag.text();
				String supstring = "";
				String supf = "";
				Element supTag = authorTag.nextElementSibling();
				if (supTag != null) {
					if (supTag.tagName().equals("sup")) {
						supstring = supTag.text();
						for (String string : supstring.split(",")) {
							if (isNumeric(string)) {
								supf = supf + string + ",";
							}
						}
						if (supf.length() >= 1) {
							supf = supf.substring(0, supf.length() - 1);
							author = author + "[" + supf + "]";
						}
					}
				}
				creator = creator + author + ";";
			}

			if (creator.length() >= 1) {
				creator = creator.substring(0, creator.length() - 1);
				creator = creator.replace(";[", "[");
			}

			Elements institutionTags = authordoc.select("ul");
			for (Element institutionTag : institutionTags) {
				if (institutionTag.attr("class").equals("no-bullet contrib-notes")) {
					continue;
				}
				for (Element affTag : institutionTag.select("li")) {
					Element supTag = affTag.select("sup").first();
					String supstring = "";
					if (supTag != null) {
						supstring = supTag.text();
						if (!supstring.equals("")) {
							supstring = "[" + supstring + "]";
						}

					}

					affTag.select("sup").remove();
					insitution = insitution + supstring + affTag.text() + ";";
				}
			}
			if (insitution.length() >= 1) {
				insitution = insitution.substring(0, insitution.length() - 1);
			}

			for (Element aTag : doc.select("div > div.medium-9.columns > a")) {
				subject = subject + aTag.text() + ";";
			}
			if (subject.length() >= 1) {
				subject = subject.substring(0, subject.length() - 1);
			}

			Element issnTag = doc.select("body > footer > div > div > p").first();
			if (issnTag != null) {
				String isString = issnTag.text();
				Matcher eissnm = Pattern.compile("(\\d{4}-\\d{4}) \\(online\\)").matcher(isString);
				Matcher pissnm = Pattern.compile("(\\d{4}-\\d{4}) \\(print\\)").matcher(isString);
				if (eissnm.find()) {
					eissn = eissnm.group(1);
				}
				if (pissnm.find()) {
					pissn = pissnm.group(1);
				}
			}

			if (!creator.contains("[")) {
				int count = 0;
				for (Element authorsTag : authordoc.select("p")) {
					if (authorsTag.text().length() == 0) {
						continue;
					}
					count++;
					String[] str = { authorsTag.text() };
					String authors = "";
					for (Element auT : authorsTag.select("a")) {
						authors = auT.text();
						if (authors.equals("")) {
							continue;
						}
						try {
							Element institutionTagss = doc.select("ul.no-bullet>li").get(count - 1);
							insitution = institutionTagss.text();
						} catch (Exception e) {
							insitution = "";
						}
						mapcre_ins.put(authors, insitution);
						String[] result = AuthorOrgan.numberByMap(mapcre_ins);
						creator = result[0];
						creator = creator.replace(";[", "[");
						insitution = result[1];
					}
				}
			}

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("title", title);
			xObj.data.put("creator", creator);
			xObj.data.put("beginpage", beginpage);
			xObj.data.put("endpage", endpage);
			xObj.data.put("date_created", date_created);
			xObj.data.put("subject", subject);
			xObj.data.put("insitution", insitution);
			xObj.data.put("description", description);
			xObj.data.put("url", url);
			xObj.data.put("vol", vol);
			xObj.data.put("issue", issue);
			xObj.data.put("source", source);
			xObj.data.put("eissn", eissn);
			xObj.data.put("pissn", pissn);
			xObj.data.put("doi", doi);

			// String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s",
			// title,creator,class_,keyword,description,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			// context.write(new Text(textString), NullWritable.get());
//			if (mjid.equals("")) {
//				context.getCounter("map", "errorcount").increment(1);
//			}
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(doi), new BytesWritable(bytes));

		}

	}

	// 继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	// 输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
				throws IOException, InterruptedException {
			/*
			 * Text out = new Text() for (BytesWritable val:values){
			 * VipcloudUtil.DeserializeObject(val.getBytes(), out); context.write(key,out);
			 * } //context.getCounter("reduce", "count").increment(1);
			 */
			BytesWritable bOut = new BytesWritable();
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}

			context.getCounter("reduce", "count").increment(1);

			bOut.setCapacity(bOut.getLength());

			context.write(key, bOut);

		}
	}
}