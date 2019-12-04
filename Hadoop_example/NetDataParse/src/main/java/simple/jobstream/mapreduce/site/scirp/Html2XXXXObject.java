package simple.jobstream.mapreduce.site.scirp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.regex.Matcher;
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
import org.hamcrest.core.Is;
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
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

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
		private static String title_series = "";
		private static String keyword = "";
		private static String keyword_alt = "";
		private static String keyword_machine = "";
		private static String clc_no_1st = "";
		private static String clc_no = "";
		private static String clc_machine = "";
		private static String subject_word = "";
		private static String subject_edu = "";
		private static String subject = "";
		private static String abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
		private static String abstract_alt = "";
		private static String abstract_type = "";
		private static String abstract_alt_type = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String doc_code = "";
		private static String doc_no = "";
		private static String raw_type = "";
		private static String recv_date = "";
		private static String accept_date = "";
		private static String revision_date = "";
		private static String pub_date = "";
		private static String pub_date_alt = "";
		private static String pub_place = "";
		private static String page_cnt = "";
		private static String pdf_size = "";
		private static String fulltext_txt = "";
		private static String fulltext_addr = "";
		private static String fulltext_type = "";
		private static String column_info = "";
		private static String fund = "";
		private static String fund_id = "";
		private static String fund_alt = "";
		private static String author_id = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_raw = "";
		private static String author_alt = "";
		private static String corr_author = "";
		private static String corr_author_id = "";
		private static String email = "";
		private static String subject_dsa = "";
		private static String research_field = "";
		private static String contributor = "";
		private static String contributor_id = "";
		private static String contributor_alt = "";
		private static String author_intro = "";
		private static String organ_id = "";
		private static String organ_1st = "";
		private static String organ = "";
		private static String organ_alt = "";
		private static String preferred_organ = "";
		private static String host_organ_id = "";
		private static String organ_area = "";
		private static String journal_raw_id = "";
		private static String journal_name = "";
		private static String journal_name_alt = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String is_suppl = "";
		private static String issn = "";
		private static String eissn = "";
		private static String cnno = "";
		private static String publisher = "";
		private static String cover_path = "";
		private static String is_oa = "";
		private static String country = "";
		private static String language = "";
		private static String ref_cnt = "";
		private static String ref_id = "";
		private static String cited_id = "";
		private static String cited_cnt = "";
		private static String down_cnt = "";
		private static String is_topcited = "";
		private static String is_hotpaper = "";
		private static String if_html_fulltex = "";
		private static String if_pdf_fulltext = "";

		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			context.getCounter("batch", batch).increment(1);
		}

		// String doi, String jid,
		public void parseHtml(String htmlText) {

			{
				Document doc = Jsoup.parse(htmlText.toString());
				// 获取卷
				Element volumeElement = doc.select("meta[name = prism.volume]").first();
				if (volumeElement != null) {
					vol = volumeElement.attr("content").trim();
					if (vol.startsWith("0")) {
						vol = vol.replace("0", "");
					}
				}
				// 获取期
				Element issueElement = doc.select("meta[name = prism.number]").first();
				if (issueElement != null) {
					String issue = issueElement.attr("content").trim();
					if (issue.startsWith("0")) {
						num = issue.replace("0", "");
					}
				}
				// 如果请求头中没有issue与num，从页面中获取
				Element VolNumElement = doc.select("div[style =float:left; width:280px; margin-top:5px;] >a").first();
				if (VolNumElement != null) {
					String VolNum = VolNumElement.text().trim();
					if (vol.length() < 1) {
						Pattern p = Pattern.compile("Vol\\.(\\d+)");
						Matcher m = p.matcher(VolNum);
						if (m.find()) {
							vol = m.group(1);
						}
					}
					if (num.length() < 1) {
						Pattern numRe = Pattern.compile("No\\.(\\d+)");
						Matcher m = numRe.matcher(VolNum);
						if (m.find()) {
							num = m.group(1);
						}
					}
				}

				// 获取关键字
				Elements keywordsElements = doc.select("div[id=JournalInfor_div_showkeywords] >div[align=justify] >a");
				for (Element keywordElement : keywordsElements) {

					String KeyStr = keywordElement.text().trim().replace(";", ",");
					keyword += KeyStr + ";";

				}
				// 处理keyword中的；
				keyword = keyword.replaceAll(";$", "");
				// 获取出版社citation_publisher
				Element publisherElement = doc.select("meta[name =citation_publisher]").first();
				if (publisherElement != null) {
					publisher = publisherElement.attr("content").trim();

				}
				// 获取摘要 citation_abstract
				Element abstractElement = doc.select("meta[name =citation_abstract]").first();
				if (abstractElement != null) {
					abstract_ = abstractElement.attr("content").trim();
				}
				// 判断是否有html或者全文
				Element htmlElement = doc.select("meta[name =citation_fulltext_html_url").first();

				if (htmlElement != null) {
					String IsHtml = htmlElement.attr("content").trim();
					if (!IsHtml.equals("")) {
						if_html_fulltex = "1";
					}
				}
				// 判断是否有pdf全文
				Element pdfElement = doc.select("meta[name =citation_pdf_url").first();
				if (pdfElement != null) {
					String Ispdf = pdfElement.attr("content").trim();
					if (!Ispdf.equals("")) {

						if_pdf_fulltext = "1";
					}
				}
				// 获取下载量，被引量
				Elements NumElements = doc.select("[style=font-weight: bold; color: Red;]");

				if (NumElements != null && NumElements.size() == 2) {
					String down_num = NumElements.get(0).text().trim();
					down_cnt = down_num.replace(",", "") + "@" + down_date;
//					String cited_num = NumElements.get(1).text().trim();
//					cited_cnt = cited_num +"@" +down_date;
				}
				// 获取页码
				Element pageElement = doc.select("[id=JournalInfor_div_paper] > [style=margin-top: 10px;]").first();
				if (pageElement != null) {
					String RawTagStr = pageElement.text().trim();
					Pattern p = Pattern.compile(".*.\\s.*?(\\d*-\\d*)");
					Matcher m = p.matcher(RawTagStr);
					if (m.find()) {
						page_info = m.group(1);
					}
					// 获取pdf大小
					Pattern PdfRe = Pattern.compile("\\(Size:(.*)KB\\)");
					Matcher result = PdfRe.matcher(RawTagStr);
					if (result.find()) {
						pdf_size= result.group(1);
					}
					if (RawTagStr.contains("PDF")) {
						fulltext_type = fulltext_type.replaceAll(";$", "") + ";" + "PDF";
					}

				}
				// 处理开视页，结束页
				if (!page_info.equals("")) {
					begin_page = page_info.split("-")[0];
					end_page = page_info.split("-")[1];
				}

				// 获取所有的引文数量
				Elements AllCntElement = doc.select("[style=text-align: justify;]").first().select("a[target=_blank]");
				if (AllCntElement.size() != 0) {

					ref_cnt = Integer.toString(AllCntElement.size());

				}
				// 获取作者
				Elements AllAuthorElement = doc.select("div[style=float: left; width: 100%; margin-top: 10px;] > a");
				for (Element authorElement : AllAuthorElement) {
					// 获取作者
					String Author = authorElement.text().trim().replace(";", "");

					// 获取当前标签下，编号标签
					Element SupElement = authorElement.nextElementSibling();

					if (Author.length() > 0) {
						if (SupElement == null || !SupElement.tagName().equals("sup")
								|| SupElement.text().trim().equals("")) {
							author += Author + ";";

						} else {
							// 获取当前标签的内容
							String sup = SupElement.text().trim().replace("*", "").replaceAll(",$", "");

							String supnum = "[" + sup + "]";
							if (supnum.equals("[]")) {
								supnum = "";
							}
							author += Author + supnum + ";";
						}
					}
				}
				// 获取机构
				Elements AllOrganElement = doc
						.select("div[id=JournalInfor_div_affs] >div[align=justify] >a[target=_blank]");
				for (Element OrganElement : AllOrganElement) {
					// 获取机构
					String Organ = OrganElement.text().trim();
					if (Organ.length() > 0) {
						// 获取当前节点的上一个兄弟节点
						Element brotherElement = OrganElement.previousElementSibling();

						if (brotherElement == null || !brotherElement.tagName().equals("sup")
								|| brotherElement.text().trim().equals("")) {
							if (Organ.length() > 0) {
								organ += Organ + ";";
							}
						} else {
							// 获取当前标签的内容
							String OrganSup = brotherElement.text().trim().replaceAll(",$", "");
							String OrganSupNum = "[" + OrganSup + "]";

							organ += OrganSupNum + Organ + ";";
						}

					}

				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{

				lngid = "";
				rawid = "";
				sub_db_id = "00045";
				product = "SCIRP";
				sub_db = "QK";
				provider = "SCIRP";
				down_date = "20190305";
//					batch = "";			// 已在 setup 内初始化，无需再改
				doi = "";
				source_type = "3";
				provider_url = "";
				title = "";
				title_alt = "";
				title_sub = "";
				title_series = "";
				keyword = "";
				keyword_alt = "";
				keyword_machine = "";
				clc_no_1st = "";
				clc_no = "";
				clc_machine = "";
				subject_word = "";
				subject_edu = "";
				subject = "";
				abstract_ = "";
				abstract_alt = "";
				abstract_type = "";
				abstract_alt_type = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				doc_code = "";
				doc_no = "";
				raw_type = "";
				recv_date = "";
				accept_date = "";
				revision_date = "";
				pub_date = "";
				pub_date_alt = "";
				pub_place = "";
				page_cnt = "";
				pdf_size = "";
				fulltext_txt = "";
				fulltext_addr = "";
				fulltext_type = "";
				column_info = "";
				fund = "";
				fund_id = "";
				fund_alt = "";
				author_id = "";
				author_1st = "";
				author = "";
				author_raw = "";
				author_alt = "";
				corr_author = "";
				corr_author_id = "";
				email = "";
				subject_dsa = "";
				research_field = "";
				contributor = "";
				contributor_id = "";
				contributor_alt = "";
				author_intro = "";
				organ_id = "";
				organ_1st = "";
				organ = "";
				organ_alt = "";
				preferred_organ = "";
				host_organ_id = "";
				organ_area = "";
				journal_raw_id = "";
				journal_name = "";
				journal_name_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				is_suppl = "";
				issn = "";
				eissn = "";
				cnno = "";
				publisher = "";
				cover_path = "";
				is_oa = "";
				country = "US";
				language = "EN";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				is_topcited = "";
				is_hotpaper = "";
				if_html_fulltex = "";
				if_pdf_fulltext = "";

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
			rawid = mapField.get("article_id").toString();
			doi = mapField.get("doi").toString();
			title = mapField.get("title").toString();
			eissn = mapField.get("eissn").toString();
			issn = mapField.get("pissn").toString();
			journal_name = mapField.get("jname").toString();
			pub_date = mapField.get("date_created").toString();
			is_oa = mapField.get("is_oa").toString();
			fulltext_type = mapField.get("fulltext_type").toString();
			journal_raw_id = mapField.get("jid").toString();
			subject = mapField.get("subject").toString();
			String htmlText = mapField.get("html").toString();
			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").toString();
			}

			if (!htmlText.toLowerCase().contains("journalinfor_div_paper")) {
				context.getCounter("map", "Error: journalinfor_div_paper").increment(1);
				return;
			}

			parseHtml(htmlText);

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}
			if (rawid.length() < 1) {

				context.getCounter("map", "Error: no rawid").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no rawid " + rawid);
				return;

			}
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			// 生成provider_url
			provider_url = "https://www.scirp.org/Journal/PaperInformation.aspx?PaperID=" + rawid;

			// 处理date
			if (pub_date.length() < 1) {
				pub_date = "1900000";
				pub_year = "1900";

			} else {
				pub_year = pub_date.substring(0, 4);
			}
			// 处理作者机构末尾;号
			author = author.replaceAll(";$", "");
			organ = organ.replaceAll(";$", "");
			subject = subject.replaceAll(";$", "");

			// 切分第一作者，第一机构
			if (author.contains(";")) {
				author_1st = author.split(";")[0];
			} else {
				author_1st = author;
			}
			if (organ.contains(";")) {

				if (organ.length() > 1) {
					organ_1st = organ.split(";")[0];
				}

			} else {
				organ_1st = organ;
			}
			organ_1st = organ_1st.replaceAll("^\\[.*?\\]", "");
			author_1st = author_1st.replaceAll("\\[.*?\\]$", "");
			fulltext_type = fulltext_type.toLowerCase();

			XXXXObject xObj = new XXXXObject();
			{
				xObj.data.put("lngid", lngid);
				xObj.data.put("rawid", rawid);
				xObj.data.put("sub_db_id", sub_db_id);
				xObj.data.put("product", product);
				xObj.data.put("sub_db", sub_db);
				xObj.data.put("provider", provider);
				xObj.data.put("down_date", down_date);
				xObj.data.put("batch", batch);
				xObj.data.put("doi", doi);
				xObj.data.put("source_type", source_type);
				xObj.data.put("provider_url", provider_url);
				xObj.data.put("title", title);
				xObj.data.put("title_alt", title_alt);
				xObj.data.put("title_sub", title_sub);
				xObj.data.put("title_series", title_series);
				xObj.data.put("keyword", keyword);
				xObj.data.put("keyword_alt", keyword_alt);
				xObj.data.put("keyword_machine", keyword_machine);
				xObj.data.put("clc_no_1st", clc_no_1st);
				xObj.data.put("clc_no", clc_no);
				xObj.data.put("clc_machine", clc_machine);
				xObj.data.put("subject_word", subject_word);
				xObj.data.put("subject_edu", subject_edu);
				xObj.data.put("subject", subject);
				xObj.data.put("abstract", abstract_);
				xObj.data.put("abstract_alt", abstract_alt);
				xObj.data.put("abstract_type", abstract_type);
				xObj.data.put("abstract_alt_type", abstract_alt_type);
				xObj.data.put("page_info", page_info);
				xObj.data.put("begin_page", begin_page);
				xObj.data.put("end_page", end_page);
				xObj.data.put("jump_page", jump_page);
				xObj.data.put("doc_code", doc_code);
				xObj.data.put("doc_no", doc_no);
				xObj.data.put("raw_type", raw_type);
				xObj.data.put("recv_date", recv_date);
				xObj.data.put("accept_date", accept_date);
				xObj.data.put("revision_date", revision_date);
				xObj.data.put("pub_date", pub_date);
				xObj.data.put("pub_date_alt", pub_date_alt);
				xObj.data.put("pub_place", pub_place);
				xObj.data.put("page_cnt", page_cnt);
				xObj.data.put("pdf_size", pdf_size);
				xObj.data.put("fulltext_txt", fulltext_txt);
				xObj.data.put("fulltext_addr", fulltext_addr);
				xObj.data.put("fulltext_type", fulltext_type);
				xObj.data.put("column_info", column_info);
				xObj.data.put("fund", fund);
				xObj.data.put("fund_id", fund_id);
				xObj.data.put("fund_alt", fund_alt);
				xObj.data.put("author_id", author_id);
				xObj.data.put("author_1st", author_1st);
				xObj.data.put("author", author);
				xObj.data.put("author_raw", author_raw);
				xObj.data.put("author_alt", author_alt);
				xObj.data.put("corr_author", corr_author);
				xObj.data.put("corr_author_id", corr_author_id);
				xObj.data.put("email", email);
				xObj.data.put("subject_dsa", subject_dsa);
				xObj.data.put("research_field", research_field);
				xObj.data.put("contributor", contributor);
				xObj.data.put("contributor_id", contributor_id);
				xObj.data.put("contributor_alt", contributor_alt);
				xObj.data.put("author_intro", author_intro);
				xObj.data.put("organ_id", organ_id);
				xObj.data.put("organ_1st", organ_1st);
				xObj.data.put("organ", organ);
				xObj.data.put("organ_alt", organ_alt);
				xObj.data.put("preferred_organ", preferred_organ);
				xObj.data.put("host_organ_id", host_organ_id);
				xObj.data.put("organ_area", organ_area);
				xObj.data.put("journal_raw_id", journal_raw_id);
				xObj.data.put("journal_name", journal_name);
				xObj.data.put("journal_name_alt", journal_name_alt);
				xObj.data.put("pub_year", pub_year);
				xObj.data.put("vol", vol);
				xObj.data.put("num", num);
				xObj.data.put("is_suppl", is_suppl);
				xObj.data.put("issn", issn);
				xObj.data.put("eissn", eissn);
				xObj.data.put("cnno", cnno);
				xObj.data.put("publisher", publisher);
				xObj.data.put("cover_path", cover_path);
				xObj.data.put("is_oa", is_oa);
				xObj.data.put("country", country);
				xObj.data.put("language", language);
				xObj.data.put("ref_cnt", ref_cnt);
				xObj.data.put("ref_id", ref_id);
				xObj.data.put("cited_id", cited_id);
				xObj.data.put("cited_cnt", cited_cnt);
				xObj.data.put("down_cnt", down_cnt);
				xObj.data.put("is_topcited", is_topcited);
				xObj.data.put("is_hotpaper", is_hotpaper);
				xObj.data.put("if_html_fulltex", if_html_fulltex);
				xObj.data.put("if_pdf_fulltext", if_pdf_fulltext);
			}

			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
