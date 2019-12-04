package simple.jobstream.mapreduce.site.cnkibs;

import java.io.IOException;
import java.lang.reflect.Type;
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
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cnkiccndpaper.Json2XXXXObject.ProcessMapper;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2xxxxobjectA extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = ""; // 这个目录会被删除重建

	public void pre(Job job) {
		String jobName = "cnkithesis.Html2XXXXObject";

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
		private static String title = "";
		private static String creator = "";
		private static String creator_degree = "";
		private static String creator_descipline = "";
		private static String creator_institution = "";
		private static String contributor = "";
		private static String description = "";
		private static String subject_clc = "";
		private static String subject = "";
		private static String type = "";
		private static String language = "";
		private static String country = "";
		private static String provider = "";
		private static String batch = "";
		private static String provider_url = "";
		private static String medium = "";
		private static String date = "";
		private static String provider_id = "";
		private static String date_created = "";
		private static String description_fund = "";
		private static String dbcode = "";
		private static String owner = "";
		private static String author_1st = "";
		private static String clc_no_1st = "";
		private static String tempString = "";

		static int cnt = 0;

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");

		}

		public void parseHtml(String htmlText) {

			if (htmlText.split("★").length == 2) {
				String filename = htmlText.split("★")[0];
				lngid = "";
				rawid = filename;
				title = "";
				subject_clc = "";
				subject = "";
				creator = "";
				creator_descipline = "";
				date = "";
				creator_degree = "";
				creator_institution = "";
				description = "";
				contributor = "";
				language = "ZH";
				country = "CN";
				provider = "cnkithesis";
				provider_url = "";
				provider_id = "";
				type = "4";
				medium = "2";
				batch = "";
				dbcode = "";
				owner = "cqu";
				date_created = "";
				description_fund = "";
				author_1st = "";
				clc_no_1st = "";
				tempString = "";

				Document doc = Jsoup.parse(htmlText);

				// 老版本解析代码
				Element titleElement = doc.select("span[id = chTitle]").first();
				Element showwriterElement = doc.select("p:contains(【作者】)").first();
				Element authorInforElement = doc.select("p:contains(【作者基本信息】)").first();
				// Element showorganElement = doc.select("div[class = orgn]").first();
				Element remark_cElement = doc.select("p:contains(摘要)").first();
				Element keyword_cElement = doc.select("span[id = ChDivKeyWord").first();
				Element bstutorsnameElement = doc.select("p:contains(导师)").first();
				Element classFidElement = doc.select("li:contains(【分类号】)").first();
				Element description_fundElement = doc.select("div[class = keywords]:contains(【基金】").first();

				if (titleElement != null) {
					title = titleElement.text();
				}

				if (showwriterElement != null) {
					creator = showwriterElement.text().replace("【作者】 ", "").replace("；", ";").trim();
					if (creator.endsWith(";")) {
						creator = creator.substring(0, creator.length() - 1);
					}
				}
				if (remark_cElement != null) {
					description = remark_cElement.text().replace("【摘要】", "").trim();
				}
				if (keyword_cElement != null) {
					subject = keyword_cElement.text().replace("；", ";").trim();
					if (subject.endsWith(";")) {
						subject = subject.substring(0, subject.length() - 1);
					}
				}
				if (bstutorsnameElement != null) {
					contributor = bstutorsnameElement.text().replace("【导师】", "").replace("；", ";").trim();
					if (contributor.endsWith(";")) {
						contributor = contributor.substring(0, contributor.length() - 1);
					}
				}
				if (classFidElement != null) {
					subject_clc = classFidElement.text().replace("【分类号】", "");

				}
				if (authorInforElement != null) {
					tempString = authorInforElement.text().trim().replace("【作者基本信息】 ", "");
					String[] tempStringarray = tempString.split("，");
					if (tempStringarray.length == 4) {
						creator_institution = tempStringarray[0].trim();
						creator_descipline = tempStringarray[1].trim();
						date = tempStringarray[2].trim();
						creator_degree = tempStringarray[3].trim();
					}

					if (creator_degree.equals("博士")) {
						dbcode = "CDFD";
					} else if (creator_degree.equals("硕士")) {
						dbcode = "CMFD";
					}

				}
				if (description_fundElement != null) {
					description_fund = description_fundElement.text().replace("【基金】", "").trim();
				}

				if (description_fund.endsWith(";")) {
					description_fund = description_fund.substring(0, description_fund.length() - 1);
				}

			}
			// 处理没有dbcode的情况
			if (dbcode.length() < 1) {
				dbcode = "CMFD";
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			cnt += 1;
			if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}

			HashMap<String, String> map = new HashMap<String, String>();

			parseHtml(value.toString());

			// 处理相关字段
			if (description_fund.endsWith(";")) {
				description_fund = description_fund.substring(0, description_fund.length() - 1);
			}
			if (date.length() == 4) {
				date_created = date + "0000";
			} else {
				date = "1900";
				date_created = date + "0000";
			}

			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			// 处理第一作者
			if (creator.contains(";")) {
				author_1st = creator.split(";")[0];
			} else {
				if (creator.length() > 1) {
					author_1st = creator;
				}
			}
			if (subject_clc.endsWith(";")) {
				subject_clc = subject_clc.replaceAll(";$", "");
				context.getCounter("map", "end with ;号").increment(1);
			}
			// 处理第一中图分类号
			if (subject_clc.contains(";")) {
				clc_no_1st = subject_clc.split(";")[0];
			} else {
				if (subject_clc.length() > 0) {
					clc_no_1st = subject_clc;
				}
			}
			context.getCounter("map", "count num").increment(1);
			if (title.length() < 1) {
				context.getCounter("map", "no title").increment(1);
				return;
			}
			if (rawid.length() > 1) {
				lngid = VipIdEncode.getLngid("00075", rawid, false);
			}
			// 去除关键字中多余的;号
			subject = StringHelper.cleanSemicolon(subject);

			// 处理学位为空的时候，或者不是博士，硕士的时候|| !creator_degree.contains("博士")
			// ||!creator_degree.contains("硕士")) {
//			if (creator_degree.trim().length() < 1) {
//				context.getCounter("map", "not len <1").increment(1);
//				creator_degree = "";
//			} 
			if (!creator_degree.contains("博士") && !creator_degree.contains("硕士")) {
				context.getCounter("map", "not 博士 and 硕士").increment(1);
				creator_degree = "";
			}
//		else if (!creator_degree.contains("硕士") ) {
//			context.getCounter("map", "not 硕士").increment(1);
//			creator_degree ="";
//		}

			XXXXObject xObjOut = new XXXXObject();
			{
				xObjOut.data.put("lngid", VipIdEncode.getLngid("00075", rawid, false));
				xObjOut.data.put("rawid", rawid);
				xObjOut.data.put("sub_db_id", "00075");
				xObjOut.data.put("product", "CNKI");
				xObjOut.data.put("sub_db", "CDMD");
				xObjOut.data.put("provider", "CNKI");
				xObjOut.data.put("down_date", "20190702");
				xObjOut.data.put("batch", batch);
				xObjOut.data.put("doi", "");
				xObjOut.data.put("source_type", "4");
				xObjOut.data.put("provider_url",
						"http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=" + dbcode + "&filename=" + rawid);
				xObjOut.data.put("title", title);
				xObjOut.data.put("title_alt", "");
				xObjOut.data.put("title_sub", "");
				xObjOut.data.put("title_series", "");
				xObjOut.data.put("keyword", subject);
				xObjOut.data.put("keyword_alt", "");
				xObjOut.data.put("keyword_machine", "");
				xObjOut.data.put("clc_no_1st", clc_no_1st);
				xObjOut.data.put("clc_no", subject_clc);
				xObjOut.data.put("clc_machine", "");
				xObjOut.data.put("subject_word", "");
				xObjOut.data.put("subject_edu", "");
				xObjOut.data.put("subject", "");
				xObjOut.data.put("abstract", description);
				xObjOut.data.put("abstract_alt", "");
				xObjOut.data.put("abstract_type", "");
				xObjOut.data.put("abstract_alt_type", "");
				xObjOut.data.put("page_info", "");
				xObjOut.data.put("begin_page", "");
				xObjOut.data.put("end_page", "");
				xObjOut.data.put("jump_page", "");
				xObjOut.data.put("doc_code", "");
				xObjOut.data.put("doc_no", "");
				xObjOut.data.put("raw_type", "");
				xObjOut.data.put("recv_date", "");
				xObjOut.data.put("accept_date", "");
				xObjOut.data.put("revision_date", "");
				xObjOut.data.put("pub_date", "");
				xObjOut.data.put("pub_date_alt", "");
				xObjOut.data.put("pub_place", "");
				xObjOut.data.put("page_cnt", "");
				xObjOut.data.put("pdf_size", "");
				xObjOut.data.put("fulltext_txt", "");
				xObjOut.data.put("fulltext_addr", "");
				xObjOut.data.put("fulltext_type", "");
				xObjOut.data.put("column_info", "");
				xObjOut.data.put("fund", description_fund);
				xObjOut.data.put("fund_alt", "");
				xObjOut.data.put("author_id", "");
				xObjOut.data.put("author_1st", author_1st);
				xObjOut.data.put("author", creator);
				xObjOut.data.put("author_raw", "");
				xObjOut.data.put("author_alt", "");
				xObjOut.data.put("corr_author", "");
				xObjOut.data.put("corr_author_id", "");
				xObjOut.data.put("email", "");
				xObjOut.data.put("subject_dsa", creator_descipline);
				xObjOut.data.put("research_field", "");
				xObjOut.data.put("contributor", contributor);
				xObjOut.data.put("contributor_id", "");
				xObjOut.data.put("contributor_alt", "");
				xObjOut.data.put("author_intro", "");
				xObjOut.data.put("organ_id", "");
				xObjOut.data.put("organ_1st", "");
				xObjOut.data.put("organ", creator_institution);
				xObjOut.data.put("organ_alt", "");
				xObjOut.data.put("preferred_organ", "");
				xObjOut.data.put("host_organ_id", "");
				xObjOut.data.put("organ_area", "");
				xObjOut.data.put("journal_raw_id", "");
				xObjOut.data.put("journal_name", "");
				xObjOut.data.put("journal_name_alt", "");
				xObjOut.data.put("pub_year", date);
				xObjOut.data.put("vol", "");
				xObjOut.data.put("num", "");
				xObjOut.data.put("is_suppl", "");
				xObjOut.data.put("issn", "");
				xObjOut.data.put("eissn", "");
				xObjOut.data.put("cnno", "");
				xObjOut.data.put("publisher", "");
				xObjOut.data.put("cover_path", "");
				xObjOut.data.put("is_oa", "");
				xObjOut.data.put("country", "CN");
				xObjOut.data.put("language", "ZH");
				xObjOut.data.put("ref_cnt", "");
				xObjOut.data.put("ref_id", "");
				xObjOut.data.put("cited_id", "");
				xObjOut.data.put("cited_cnt", "");
				xObjOut.data.put("down_cnt", "");
				xObjOut.data.put("is_topcited", "");
				xObjOut.data.put("is_hotpaper", "");
				xObjOut.data.put("degree", creator_degree);
			}
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObjOut);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}
	}

}
