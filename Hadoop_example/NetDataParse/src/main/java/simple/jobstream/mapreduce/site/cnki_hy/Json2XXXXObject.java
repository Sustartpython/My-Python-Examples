
package simple.jobstream.mapreduce.site.cnki_hy;

import java.io.IOException;
import java.time.Month;
import java.time.Year;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Set;

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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cnkiccndpaper.Json2XXXXObject.ProcessMapper;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "XXXXObjectHY";
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
		private static String lngid = "";
		private static String rawid = "";
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String source_type = "";
		private static String provider_url = "";
		private static String title = "";
		private static String title_alt = "";
		private static String title_sub = "";
		private static String keyword = "";
		private static String keyword_alt = "";
		private static String keyword_machine = "";
		private static String clc_no_1st = "";
		private static String clc_no = "";
		private static String clc_machine = "";
		private static String subject_edu = "";
		private static String subject = "";
		private static String abstract_ = "";
		private static String abstract_alt = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String accept_date = "";
		private static String pub_date = "";
		private static String pub_date_alt = "";
		private static String pub_place = "";
		private static String page_cnt = "";
		private static String pdf_size = "";
		private static String fulltext_addr = "";
		private static String fulltext_type = "";
		private static String fund = "";
		private static String author_id = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_alt = "";
		private static String corr_author = "";
		private static String corr_author_id = "";
		private static String research_field = "";
		private static String author_intro = "";
		private static String organ_id = "";
		private static String organ_1st = "";
		private static String organ = "";
		private static String organ_alt = "";
		private static String host_organ = "";
		private static String host_organ_id = "";
		private static String sponsor = "";
		private static String organ_area = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String is_suppl = "";
		private static String issn = "";
		private static String eissn = "";
		private static String cnno = "";
		private static String publisher = "";
		private static String meeting_name = "";
		private static String meeting_name_alt = "";
		private static String meeting_record_name = "";
		private static String meeting_record_name_alt = "";
		private static String meeting_place = "";
		private static String meeting_counts = "";
		private static String meeting_code = "";
		private static String is_oa = "";
		private static String country = "";
		private static String language = "";
		private static String date_created = "";
		private static String month = "";
		private static String year = "";
		private static String day = "";
		static int cnt = 0;
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}

		public void parseHtml(String htmlText) {
			lngid = "";
			rawid = "";
			sub_db_id = "";
			product = "";
			sub_db = "";
			provider = "";
			down_date = "";
			batch = "";
			doi = "";
			source_type = "";
			provider_url = "";
			title = "";
			title_alt = "";
			title_sub = "";
			keyword = "";
			keyword_alt = "";
			keyword_machine = "";
			clc_no_1st = "";
			clc_no = "";
			clc_machine = "";
			subject_edu = "";
			subject = "";
			abstract_ = "";
			abstract_alt = "";
			begin_page = "";
			end_page = "";
			jump_page = "";
			accept_date = "";
			pub_date = "";
			pub_date_alt = "";
			pub_place = "";
			page_cnt = "";
			pdf_size = "";
			fulltext_addr = "";
			fulltext_type = "";
			fund = "";
			author_id = "";
			author_1st = "";
			author = "";
			author_alt = "";
			corr_author = "";
			corr_author_id = "";
			research_field = "";
			author_intro = "";
			organ_id = "";
			organ_1st = "";
			organ = "";
			organ_alt = "";
			host_organ = "";
			host_organ_id = "";
			sponsor = "";
			organ_area = "";
			pub_year = "";
			vol = "";
			num = "";
			is_suppl = "";
			issn = "";
			eissn = "";
			cnno = "";
			publisher = "";
			meeting_name = "";
			meeting_name_alt = "";
			meeting_record_name = "";
			meeting_record_name_alt = "";
			meeting_place = "";
			meeting_counts = "";
			meeting_code = "";
			is_oa = "";
			country = "CN";
			language = "ZH";
			date_created = "";
			year = "";
			month = "";
			day = "";
			int idx = htmlText.indexOf('★');
			rawid = htmlText.substring(0, idx);
			Document doc = Jsoup.parse(htmlText);

			// title
			Element ElementTitle = doc.select("h1").first();
			if (ElementTitle != null) {
				title = ElementTitle.text().trim();

			}
			// 获取
			Element div = doc.select("div[class=summary pad10]").first();
			if (div != null) {

				Elements divs = div.select("p");
				for (Element data : divs) {

					if (data.text().startsWith("【作者】")) {
						author = data.text().replace("【作者】", "").replace("； ", ";").replaceAll("；", ";")
								.replaceAll(";$", "").trim();

					} else if (data.text().startsWith("【机构】")) {
						organ = data.text().replace("【机构】", "").replace(" ", "").replace("；", ";").replaceAll(";$", "")
								.trim();

					}

					else if (data.text().startsWith("【摘要】")) {
						abstract_ = data.text().replace("【摘要】", "").trim().replaceAll("^<正>~~", "").replaceAll("^<正>", "")
								.trim();

					}
				}
			}

			Elements Elementword = doc.select("div[class = keywords]");
			for (Element word : Elementword) {

//				System.out.println(word.text());
				if (word.text().contains("【关键词】")) {
					keyword = word.text();
					keyword = keyword.replace("【关键词】", "").replace("；", ";").replaceAll(";$", "").trim()
							.replace("；", ";").replaceAll(";$", "");

				}
				else if (word.text().contains("【基金】")) {
					fund = word.text();
					fund = fund.replace("【基金】", "").replace("；", ";").replaceAll(";$", "").trim();

				}

			}

			Element Elementsummary = doc.select("div[class = summary]").first();
			if (Elementsummary != null) {
				Elements lis = Elementsummary.select("li");
				for (Element li : lis) {

					if (li.text().contains("【会议录名称】")) {
						meeting_record_name = li.text();
						meeting_record_name = meeting_record_name.replace("【会议录名称】", "");

					}
					else if  (li.text().contains("【会议名称】")) {
						meeting_name = li.text();
						meeting_name = meeting_name.replace("【会议名称】", "");

					}
					else if  (li.text().contains("【会议时间】")) {
						String[] list =li.text().replace("【会议时间】", "").split("-");
						if (list.length == 3) {
							year = list[0];
							month = list[1];
							day = list[2];
						} else if (list.length == 2) {
							year = list[0];
							month = list[1];
							day = "00";
						} else if (list.length == 1) {
							year = list[0];
							if (year.equals("")) {
								year = "1900";
							}
							month = "00";
							day = "00";
						} else if (list.length == 0) {
							year = "1900";
							month = "00";
							day = "00";
						}
						if (month.length() == 1) {
							month = "0" + month;
						}
						if (day.length() == 1) {
							day = "0" + day;
						}
						date_created = year + month + day;
						if (date_created.length() > 8) {
							date_created = date_created.substring(0, 8);
						}
						if (date_created.length() < 4) {
							date_created = "19000000";
						}

						pub_date = date_created;
						pub_date = date_created.replace("【会议时间】", "");
						accept_date = pub_date;
						pub_year = pub_date.substring(0, 4);

					}
					else if  (li.text().contains("【会议地点】")) {
						meeting_place = li.text();
						meeting_place = meeting_place.replace("【会议地点】", "");
			
					}
					else if(li.text().contains("【分类号】")) {
						clc_no = li.text();
						clc_no = clc_no.replace("【分类号】", "").replace(" ", ";").replace(" ", ";").replace("+", "").trim()
								.replace("+", "");
				
					}
					else if (li.text().contains("【主办单位】")) {
						host_organ = li.text();
						host_organ = host_organ.replace("【主办单位】", "");
		
					}
				}

			}
			lngid = VipIdEncode.getLngid("00090", rawid, false);
			provider_url = "http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CIPD&filename=" + rawid;

			if (clc_no.contains(";")) {
				clc_no_1st = clc_no.split(";")[0];
			} else {
				if (clc_no.length() > 0) {
					clc_no_1st = clc_no;
				}
			}

			// 处理第一机构
			if (organ.contains(";")) {
				if (organ.split(";").length < 1) {
					organ = "";
					organ_1st = "";

				} else {
					String news = "";
					List<String> list = Arrays.asList(organ.split(";"));
					Set<String> set = new HashSet<String>(list);
					List<String> result = new ArrayList<String>(set);
					for (int i = 0; i < result.size(); i++) {
						news = news + ";" + result.get(i);
					}
					organ_1st = organ.split(";")[0];
					organ = news.replaceAll("^;", "").trim();
					organ_1st = organ.split(";")[0];
				}
			} else {
				if (organ.length() > 0) {
					organ_1st = organ;
				}
			}

			// 处理第一作者
			if (author.contains(";")) {
				author_1st = author.split(";")[0];
			} else {
				if (author.length() > 0) {
					author_1st = author;
				}
			}
			// 去除关键字中多余的;号
			subject = StringHelper.cleanSemicolon(subject);

			// 处理作者问题
			author = author.replaceAll("； ", ";").replaceAll("；", ";").replaceAll("；$", "").replaceAll(";$", "");

		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {

			}

			String text = value.toString();
			int idx = text.indexOf('★');
			if (idx < 1) {
				context.getCounter("map", "not find ★").increment(1);
				return;
			}

			parseHtml(text);

			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", lngid);
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00090");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "CPFD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", "20190619");
				xObjOut.data.put("batch", batch);
				xObjOut.data.put("doi", doi);
				xObjOut.data.put("source_type", "6");
				xObjOut.data.put("provider_url", provider_url);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alt", title_alt);
				xObjOut.data.put("title_sub", title_alt);
				xObjOut.data.put("keyword", keyword);
				xObjOut.data.put("keyword_alt", keyword_alt);
				xObjOut.data.put("keyword_machine", keyword_machine);
				xObjOut.data.put("clc_no_1st", clc_no_1st);
				xObjOut.data.put("clc_no", clc_no);
				xObjOut.data.put("clc_machine", clc_machine);
				xObjOut.data.put("subject_edu", subject_edu);
				xObjOut.data.put("subject", subject);
				xObjOut.data.put("abstract", abstract_);
				xObjOut.data.put("abstract_alt",abstract_alt);
				xObjOut.data.put("begin_page", begin_page);
				xObjOut.data.put("end_page",end_page);
				xObjOut.data.put("jump_page",jump_page);
				xObjOut.data.put("accept_date", date_created);
				xObjOut.data.put("pub_date", date_created);
				xObjOut.data.put("pub_date_alt", pub_date_alt);
				xObjOut.data.put("pub_place", pub_place);
				xObjOut.data.put("page_cnt",page_cnt);
				xObjOut.data.put("pdf_size",pdf_size);
				xObjOut.data.put("fulltext_addr", fulltext_addr);
				xObjOut.data.put("fulltext_type", fulltext_type);
				xObjOut.data.put("fund", fund);
				xObjOut.data.put("author_id", author_id);
				xObjOut.data.put("author_1st", author_1st);
				xObjOut.data.put("author", author);
				xObjOut.data.put("author_alt", author_alt);
				xObjOut.data.put("corr_author", corr_author);
				xObjOut.data.put("corr_author_id", corr_author_id);
				xObjOut.data.put("research_field", research_field);
				xObjOut.data.put("author_intro", author_intro);
				xObjOut.data.put("organ_id", organ_id);
				xObjOut.data.put("organ_1st", organ_1st.replaceAll("；$", ""));
				xObjOut.data.put("organ", organ.replace("； ", ";").replaceAll("；$", ""));
				xObjOut.data.put("organ_alt", organ_alt);
				xObjOut.data.put("host_organ", host_organ.replace("、", ";").replaceAll(";$", "").trim());
				xObjOut.data.put("host_organ_id", host_organ_id);
				xObjOut.data.put("sponsor", sponsor);
				xObjOut.data.put("organ_area", organ_area);
				xObjOut.data.put("pub_year", pub_year);
				xObjOut.data.put("vol", vol);
				xObjOut.data.put("num", num);
				xObjOut.data.put("is_suppl", is_suppl);
				xObjOut.data.put("issn", issn);
				xObjOut.data.put("eissn", eissn);
				xObjOut.data.put("cnno", cnno);
				xObjOut.data.put("publisher", publisher);
				xObjOut.data.put("meeting_name", meeting_name);
				xObjOut.data.put("meeting_name_alt", meeting_name_alt);
				xObjOut.data.put("meeting_record_name", meeting_record_name);
				xObjOut.data.put("meeting_record_name_alt", meeting_record_name_alt);
				xObjOut.data.put("meeting_place", meeting_place);
				xObjOut.data.put("meeting_counts", meeting_counts);
				xObjOut.data.put("meeting_code", meeting_code);
				xObjOut.data.put("is_oa", is_oa);
				xObjOut.data.put("country", country);
				xObjOut.data.put("language", language);
			}

			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}
	}
}
