package simple.jobstream.mapreduce.site.jamajournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Map;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 100;

	private static String inputHdfsPath = "";
	private static String outputHdfsPath = "";
	
	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
	
//		String jobName = "jamajournal." + this.getClass().getSimpleName();
//		if (testRun) {
//			jobName = "test_" + jobName;
//		}
//		job.setJobName(jobName);
//
//		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
//		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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
//		job.setReducerClass(ProcessReducer.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

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
		
		private static String batch = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();
			return text;
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {

			Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String down_date = mapJson.get("down_date").trim();
			String rawid = mapJson.get("rawid").trim().split("/")[1];
			String html = mapJson.get("html").trim();

			Document doc = Jsoup.parse(html);

			// title
			String title = "";
			Element titleElement = doc.select("meta[name=citation_title]").first();
			if (titleElement != null) {
				title = titleElement.attr("content").trim();
			}

			// 文章栏目
			String title_series = "";
			Element articleTypeElement = doc.select("div.meta-article-type.thm-col").first();
			if (articleTypeElement != null) {
				title_series = articleTypeElement.text().trim();
			}

			// issn
			String identifier_pissn = "";
			Element issnElement = doc.select("meta[name=citation_issn]").first();
			if (issnElement != null) {
				identifier_pissn = issnElement.attr("content").trim();
			}

			// identifier_doi
			String identifier_doi = "";
			Element doiElement = doc.select("meta[name=citation_doi]").first();
			if (doiElement != null) {
				identifier_doi = doiElement.attr("content").trim();
			}

			// 作者
			String creator = "";
			if (doc.select("span.wi-fullname.brand-fg").size() > 0) {
				for (Element ele : doc.select("span.wi-fullname.brand-fg")) {
					// ab1,2,3-->ab[1,2,3]
					String supresult = "";
					if (ele.select("a > sup").first() != null) {
						supresult = ele.select("a > sup").first().text().trim();
					}
//					String supresult = ele.select("a > sup").first().text().trim();
					String authorresult = ele.text().trim();
					if (!supresult.equals("")) {
						// sup里面包含值,即作者的单位
						String substr = authorresult.split(supresult)[0];
						creator += substr + "[" + supresult + "]" + ";";
					} else {
						creator += authorresult + ";";
					}
				}
				creator = creator.replaceAll(";+$", "");
			}

			// author_affiliations
			String creator_institution = "";
			if (doc.select("li.meta-author-affiliation").size() > 0) {
				for (Element ele : doc.select("li.meta-author-affiliation")) {
					// 1ab;2cd-->[1]ab;[2]cd
					String result = ele.text().trim();
					String subresult = result.substring(0, 1);
					if (subresult.compareTo("9") > 0) {
						// 作者单位开头不是数字
						creator_institution += result + ";";
					} else {
						// 作者单位以数字开头,拆分并拼接 [+9+]+xxx+;[]...
						creator_institution += "[" + subresult + "]" + result.split(subresult)[1] + ";";
					}
				}
				creator_institution = creator_institution.replaceAll(";+$", "");
			}

			// journal_title
			String source = "";
			Element jtElement = doc.select("meta[name=citation_journal_title]").first();
			if (jtElement != null) {
				source = jtElement.attr("content").trim();
			}

			// year
			// span取不到的话从meta里面取
			String date = "";
			Element dateElement = doc.select("span.year").first();
			if (dateElement != null) {
				date = dateElement.text().trim();

			} else {
				Element date2Element = doc.select("div.meta-date").first();
				date = date2Element.text().trim();
				date = date.substring(date.length() - 4);
			}

			// volume
			String volume = "";
			Element volElement = doc.select("meta[name=citation_volume]").first();
			if (volElement != null) {
				volume = volElement.attr("content").trim();

			}

			// issue
			String issue = "";
			Element issueElement = doc.select("meta[name=citation_issue]").first();
			if (issueElement != null) {
				issue = issueElement.attr("content").trim();

			}

			// abstracts
			// 头标签有就取头标签,没有就取full-text里的全部内容
			String description = "";
			Element absElement = doc.select("meta[name='citation_abstract']").first();
			if (absElement != null) {
				description = absElement.attr("content").trim();
				description = Jsoup.parse(description).text().trim();
			} else {
				Element absElement2 = doc.select("div.article-full-text").first();
				if (absElement2 != null) {
					description = absElement2.text().trim();
				}
			}

			// key_word
			String subject = "";
			Elements kwElements = doc.select("meta[name=citation_keyword]");
			if (kwElements.size() > 0) {
				for (Element ele : kwElements) {
					subject += ele.attr("content") + ";";
				}
				subject = subject.replaceAll(";+$", "");
			}

			// beginpage
			String beginpage = "";
			Element fpElement = doc.select("meta[name=citation_firstpage]").first();
			if (fpElement != null) {
				beginpage = fpElement.attr("content").trim();
			}

			// endpage
			String endpage = "";
			Element ltElement = doc.select("meta[name=citation_lastpage]").first();
			if (ltElement != null) {
				endpage = ltElement.attr("content").trim();
			}

			// all_pages
			String page = "";
			page = beginpage + "-" + endpage;

//			// pagecount
//			Integer pagecount = 0;
//			pagecount = Integer.valueOf(endpage) - Integer.valueOf(beginpage) + 1;
			String pagecount = "";

			// publication_date
			String date_created = "";
			Element pubtimeElement = doc.select("meta[name=citation_publication_date]").first();
			if (pubtimeElement != null) {
				date_created = pubtimeElement.attr("content").trim().replace("/", "");
			}

			// citation_publisher
			String publisher = "";
			Element pubElement = doc.select("meta[name=citation_publisher]").first();
			if (pubElement != null) {
				publisher = pubElement.attr("content").trim();
			}

			// views
			String views = "";
			Element viewElement = doc.select("div.artmet-item.artmet-views > a > span.artmet-number").first();
			if (viewElement != null) {
				views = viewElement.text().trim();
			} else {
				Element viewElement2 = doc.select("div.artmet-item.artmet-views > span.artmet-number").first();
				if (viewElement2 != null) {
					views = viewElement2.text().trim();
				}
			}

			// citations
			String citations = "";
			Element citationElement = doc
					.select("div.artmet-item.artmet-citations > a.artmet-citations-link >span.artmet-number").first();
			if (citationElement != null) {
				citations = citationElement.text().trim();
			} else {
				Element citationElement2 = doc.select("div.artmet-item.artmet-citations > span.artmet-number").first();
				if (citationElement2 != null) {
					citations = citationElement2.text().trim();
				}
			}

			// altmetric
			String altmetrics = "";
			Element altmetricElement = doc.select(
					"div.widget-AltmetricLink.widget-instance-AMAv2_ArticleLevelMetrics_AltmetricLinkSummary > div > a >span")
					.first();
			if (altmetricElement != null) {
				altmetrics = altmetricElement.text().trim();

			}

			// comments
			String comments = "";
			Element commentElement = doc
					.select("div.artmet-item.artmet-comments > a.artmet-comments-link > span.artmet-number").first();
			if (commentElement != null) {
				comments = commentElement.text().trim();
			} else {
				Element commentElement2 = doc.select("div.artmet-item.artmet-citations > span.artmet-number").first();
				if (commentElement2 != null) {
					comments = commentElement2.text().trim();
				}
			}

			// provider_url
			String provider_url = "";
			Element prourlElment = doc.select("meta[property=og:url]").first();
			if (prourlElment != null) {
				provider_url = prourlElment.attr("content").trim();
			}

			// gch_id
			String gch_id = provider_url.split("/")[4];

			String parse_time = DateTimeHelper.getNowTimeAsParseTime();

			XXXXObject xObj = new XXXXObject();

			xObj.data.put("rawid", rawid);
			xObj.data.put("down_date", down_date);
			xObj.data.put("identifier_doi", identifier_doi);
			xObj.data.put("title", title);
			xObj.data.put("title_series", title_series);
			xObj.data.put("identifier_pissn", identifier_pissn);
			xObj.data.put("creator", creator);
			xObj.data.put("creator_institution", creator_institution);
			xObj.data.put("publisher", publisher);
			xObj.data.put("date", date);
			xObj.data.put("description", description);
			xObj.data.put("subject", subject);
			xObj.data.put("volume", volume);
			xObj.data.put("issue", issue);
			xObj.data.put("date_created", date_created);
			xObj.data.put("page", page);
			xObj.data.put("beginpage", beginpage);
			xObj.data.put("endpage", endpage);
			xObj.data.put("pagecount", String.valueOf(pagecount));
			xObj.data.put("source", source);
			xObj.data.put("parse_time", parse_time);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("gch_id", gch_id);
			xObj.data.put("batch", batch);

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}

	}

	// 继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	// 输出类型为<Text,IntWritable>
//	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
//		public void reduce(Text key, Iterable<BytesWritable> values, Context context)
//				throws IOException, InterruptedException {
//			/*
//			 * Text out = new Text() for (BytesWritable val:values){
//			 * VipcloudUtil.DeserializeObject(val.getBytes(), out); context.write(key,out);
//			 * } //context.getCounter("reduce", "count").increment(1);
//			 */
//			BytesWritable bOut = new BytesWritable();
//			for (BytesWritable item : values) {
//				if (item.getLength() > bOut.getLength()) {
//					bOut.set(item.getBytes(), 0, item.getLength());
//				}
//			}
//
//			context.getCounter("reduce", "count").increment(1);
//
//			bOut.setCapacity(bOut.getLength());
//
//			context.write(key, bOut);
//
//		}
//	}
}