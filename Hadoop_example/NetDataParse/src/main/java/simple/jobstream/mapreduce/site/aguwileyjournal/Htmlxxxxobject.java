package simple.jobstream.mapreduce.site.aguwileyjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
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
public class Htmlxxxxobject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "aguwileyjournal." + this.getClass().getSimpleName();

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

		public static HashMap<String, String> parseHtml(String article_id, String journal_title, String authors,
				String pages, String bepage, String enpage, String issue_id, String date_create, String jid,
				String year, String mouth, String vol, String num, String jname, String eissn, String htmlText) {
			HashMap<String, String> map = new HashMap<String, String>();
			String rawid = article_id.replace("/doi/", "");
			String identifier_doi = "";
			String title = journal_title;
			String identifier_eissn = eissn;
			String creator = authors;
			String source = jname;
			String date = year;
			String volume = vol;
			String issue = num;
			String description = "";

			String page = pages;
			String beginpage = bepage;
			String endpage = enpage;
			String subject = "";
			String description_fund = "";
			String cited_cnt = "";
			String date_created = date_create;
			String journalId = jid;
			if (htmlText.contains("row article-row")) {
				String text = htmlText;
				try {

					Document doc = Jsoup.parse(htmlText);
					Element identifier_doiElement = doc.select("a[class='epub-doi']").first();

					if (identifier_doiElement != null) {

						identifier_doi = identifier_doiElement.attr("href").replace("https://doi.org/", "");
					
					}

					Element cited_cntElement = doc.select("div[class='epub-section cited-by-count']").first();

					if (cited_cntElement != null) {

						cited_cnt = cited_cntElement.text().trim().replace("Cited by:", "").replace(" ", "");
				
					}
					Element descriptionElement = doc.select("div[class='article-section__content en main']").first();

					if (descriptionElement != null) {

						description = descriptionElement.text().trim();
						

					}

					Elements description_fundsElement = doc.select("ul[class='unordered-list']");

					for (Element description_funds : description_fundsElement) {

						String description_fund_str = description_fundsElement.select("li").first().text().trim();

						description_fund = description_fund + description_fund_str + ";";
					}
		
					Elements subjectsElement = doc.select("meta[name='citation_keywords']");
		
					for (Element subjectElement : subjectsElement) {

						// 获取文本
						String subject_str = subjectElement.attr("content").trim();

						subject = subject + subject_str + ";";

					}
					if (subject.length() != 0 && subject.endsWith(";")) {
						subject = subject.substring(0, subject.length() - 1);
		
					}
				} catch (Exception e) {
					e.printStackTrace();
				}
				if (title.length() == 0) {
					return null;
				}
				if (rawid != null) {
					map.put("rawid", rawid);
				} else {
					map.put("rawid", "");
				}
				map.put("title", title);
				map.put("identifier_doi", identifier_doi);
				map.put("creator", creator);
				map.put("date", date);
				map.put("source", source);
				map.put("volume", volume);
				map.put("issue", issue);
				map.put("identifier_eissn", identifier_eissn);
				map.put("beginpage", beginpage);
				map.put("endpage", endpage);
				map.put("subject", subject);
				map.put("cited_cnt", cited_cnt);
				map.put("date_created", date_created);
				map.put("description", description);
				map.put("journalId", journalId);
				map.put("page", page);

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

			String article_id = mapField.get("doi").toString();
			String journal_title = mapField.get("title").toString();
			String authors = mapField.get("author").toString();
			String pages = mapField.get("page").toString();
			String bepage = mapField.get("beginpage").toString();
			String issue_id = mapField.get("issue_id").toString();
			String enpage = mapField.get("endpage").toString();
			String date_create = mapField.get("date_created").toString();
			
			String jid = mapField.get("jid").toString();
			//String year = mapField.get("year").toString();
			String mouth = mapField.get("mouth").toString();
			String vol = mapField.get("volume").toString();
			String num = mapField.get("issue").toString();
			String jname = mapField.get("jname").toString();
			String eissn = mapField.get("eissn").toString();
			String htmlText = mapField.get("html").toString();		
			if(date_create.length() <1){				
							date_create = "19000000";	
					}
			String year = date_create.substring(0,4);
			

			HashMap<String, String> map = new HashMap<String, String>();
			map = parseHtml(article_id, journal_title, authors, pages, bepage, enpage, issue_id, date_create, jid, year,
					mouth, vol, num, jname, eissn, htmlText);
			
			

			if (map == null) {
				context.getCounter("map", "map null").increment(1);
				return;
			}
			if ("".equals(map.get("rawid"))) {
				context.getCounter("map", "not find rawid").increment(1);
				return;
			}
			if (map.get("title").equals("")) {
				context.getCounter("map", "not find title").increment(1);
				return;
			}

			if (map != null) {
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawid", map.get("rawid"));
				xObj.data.put("title", map.get("title"));
				xObj.data.put("identifier_doi", map.get("identifier_doi"));
				xObj.data.put("creator", map.get("creator"));
				xObj.data.put("date", map.get("date"));
				xObj.data.put("source", map.get("source"));
				xObj.data.put("volume", map.get("volume"));
				xObj.data.put("issue", map.get("issue"));
				xObj.data.put("identifier_eissn", map.get("identifier_eissn"));
				xObj.data.put("date_created", map.get("date_created"));
				xObj.data.put("description", map.get("description"));
				xObj.data.put("journalId", map.get("journalId"));
				xObj.data.put("page", map.get("page"));
				xObj.data.put("beginpage", map.get("beginpage"));
				xObj.data.put("endpage", map.get("endpage"));
				xObj.data.put("cited_cnt", map.get("cited_cnt"));
				xObj.data.put("subject", map.get("subject"));

				byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(map.get("rawid")), new BytesWritable(bytes));
				context.getCounter("map", "count").increment(1);
			} else {
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
