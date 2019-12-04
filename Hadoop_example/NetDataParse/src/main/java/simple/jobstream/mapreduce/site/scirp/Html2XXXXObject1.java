package simple.jobstream.mapreduce.site.scirp;

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
import simple.jobstream.mapreduce.site.scirp.Html2XXXXObject.ProcessMapper;

//import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject1 extends InHdfsOutHdfsJobInfo {

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
		private static String beginpage = "";
		private static String endpage = "";
		private static String date_created = "";
		private static String down_date = "";
		private static String journalId = "";
		private static String ref_cnt = "";
		private static String provider_subject = "";
		private static String if_html_fulltex = "0";
		private static String if_pdf_fulltext = "0";
		private static String cited_cnt = "";
		private static String down_cnt = "";
		private static String is_oa = "";
		private static String page = "";
		private static String date = "";
		private static String lngid = "";
		private static String type1 = "3";
		private static String medium = "2";
		private static String language = "EN";
		private static String country = "US";	
		private static String gch="";
		private static String provider = "scirpjournal";
		private static String provider_url = "";
		private static String provider_id = "";
		private static String sud_db_id = "00045";

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
					volume = volumeElement.attr("content").trim();
					if (volume.startsWith("0")) {
						volume = volume.replace("0", "");
					}
				}
				// 获取期
				Element issueElement = doc.select("meta[name = prism.number]").first();
				if (issueElement != null) {
					issue = issueElement.attr("content").trim();
					if (volume.startsWith("0")) {
						issue = issue.replace("0", "");
					}
				}
				// 获取关键字
				Element keywordElement = doc.select("meta[name =keywords]").first();
				if (keywordElement != null) {
					subject = keywordElement.attr("content").trim().replaceAll(",", ";");
				}
				// 获取出版社citation_publisher
				Element publisherElement = doc.select("meta[name =citation_publisher]").first();
				if (publisherElement != null) {
					publisher = publisherElement.attr("content").trim();

				}
				// 获取摘要 citation_abstract
				Element abstractElement = doc.select("meta[name =citation_abstract]").first();
				if (abstractElement != null) {
					description = abstractElement.attr("content").trim();
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
					down_cnt = NumElements.get(0).text().trim();
					cited_cnt = NumElements.get(1).text().trim();
				}
				// 获取页码
				Element pageElement = doc.select("[id=JournalInfor_div_paper] > [style=margin-top: 10px;]").first();
				if (pageElement != null) {
					String RawTagStr = pageElement.text().trim();
					Pattern p = Pattern.compile(".*.\\s.*?(\\d*-\\d*)");
					Matcher m = p.matcher(RawTagStr);
					if (m.find()) {
						page = m.group(1);
					}
					// 获取pdf大小
					Pattern PdfRe = Pattern.compile("\\(Size:(.*)KB\\)");
					Matcher result = PdfRe.matcher(RawTagStr);
					if (m.find()) {
						page = m.group(1);
					}
//					if (RawTagStr.contains("PDF")) {
//						fulltext_type = fulltext_type + ";" + "PDF";
//					}

				}
				// 处理开视页，结束页
				if (!page.equals("")) {
					beginpage = page.split("-")[0];
					endpage = page.split("-")[1];
				}

				// 获取所有的引文
				Elements AllCntElement = doc.select("[style=text-align: justify;]").first().select("a[target=_blank]");
				if (AllCntElement.size() != 0) {
					
					ref_cnt = Integer.toString(AllCntElement.size());

				}
				// 获取作者
				Elements AllAuthorElement = doc.select("div[style=float: left; width: 100%; margin-top: 10px;] > a");
				for (Element authorElement : AllAuthorElement) {
					// 获取作者
					String Author = authorElement.text().trim();

					// 获取当前标签下，编号标签
					Element SupElement = authorElement.nextElementSibling();

					if (SupElement == null || !SupElement.tagName().equals("sup")
							|| SupElement.text().trim().equals("")) {
						creator += Author + ";";

					} else {
						// 获取当前标签的内容
						String sup = SupElement.text().trim().replace("*", "").replaceAll(",$", "");

						String supnum = "[" + sup + "]";
						if (supnum.equals("[]")) {
							supnum = "";
						}
						creator += Author + supnum + ";";
					}
				}
				// 获取机构
				Elements AllOrganElement = doc
						.select("div[id=JournalInfor_div_affs] >div[align=justify] >a[target=_blank]");
				for (Element OrganElement : AllOrganElement) {
					// 获取机构
					String Organ = OrganElement.text().trim();

					// 获取当前节点的上一个兄弟节点
					Element brotherElement = OrganElement.previousElementSibling();

					if (brotherElement == null || !brotherElement.tagName().equals("sup")
							|| brotherElement.text().trim().equals("")) {

						creator_institution += Organ + ";";
					} else {
						// 获取当前标签的内容
						String OrganSup = brotherElement.text().trim().replaceAll(",$", "");
						String OrganSupNum = "[" + OrganSup + "]";

						creator_institution += OrganSupNum + Organ + ";";
					}

				}
			}
		}

		public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
			{
				rawid = "";
				identifier_doi = "";
				title = "";
				identifier_pissn = "";
				identifier_eissn = "";
				source = "";
				date_created = "";
				provider_subject = "";
				is_oa = "";
				down_date = "20190310"; // 因为有些历史数据没有将 down_date 写入json
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
			rawid = mapField.get("article_id").toString();
			identifier_doi = mapField.get("doi").toString();
			title = mapField.get("title").toString();
			identifier_eissn = mapField.get("eissn").toString();
			identifier_pissn = mapField.get("pissn").toString();
			source = mapField.get("jname").toString();
			date_created = mapField.get("date_created").toString();
			is_oa = mapField.get("is_oa").toString();
			provider_subject = mapField.get("subject").toString();
			journalId = mapField.get("jid").toString();
			String htmlText = mapField.get("html").toString();
			if (mapField.containsKey("down_date")) {
				down_date = mapField.get("down_date").toString();
			}

//			if (!htmlText.toLowerCase().contains("Journalinfor_div_paper")) {
//				context.getCounter("map", "Error: journalinfor_div_paper").increment(1);
//				return;
//			}

			parseHtml(htmlText);

			if (title.length() < 1) {
				context.getCounter("map", "Error: no title").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no title: " + rawid);
				return;
			}
			if(rawid.length() <1) {
				
				context.getCounter("map", "Error: no rawid").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "error: no rawid " + rawid);
				return;
				
			}
			lngid = VipIdEncode.getLngid(sud_db_id, rawid, false);
			
			// 生成gch
			gch = provider +"@" +journalId;
			
			//生成provider_id
			provider_id =provider +"@"+rawid;
			
			//生成provider_url
			provider_url = provider +"@" +"https://www.scirp.org/Journal/PaperInformation.aspx?PaperID=" +rawid;
			
			// 处理date
			if(date_created.length() <1) {
				date_created ="1900000";
				date ="1900";
				
			}
			else {
				date = date_created.substring(0,4);
			}
			// 处理作者机构末尾;号
			creator =creator.replaceAll(";$", "");
			creator_institution =creator_institution.replaceAll(";$", "");
			subject =subject.replaceAll(";$", "");
			
			//转义.replace('\0', ' ').replace("'", "''").trim();
			{
			batch = batch .replace('\0', ' ').replace("'", "''").trim();
			rawid =rawid.replace('\0', ' ').replace("'", "''").trim();
			identifier_doi =identifier_doi.replace('\0', ' ').replace("'", "''").trim();
			title =title.replace('\0', ' ').replace("'", "''").trim();
			identifier_pissn =identifier_pissn .replace('\0', ' ').replace("'", "''").trim();
			identifier_eissn =identifier_eissn.replace('\0', ' ').replace("'", "''").trim();
			creator =creator.replace('\0', ' ').replace("'", "''").trim();
			creator_institution =creator_institution.replace('\0', ' ').replace("'", "''").trim();
			source =source.replace('\0', ' ').replace("'", "''").trim();
			publisher =publisher.replace('\0', ' ').replace("'", "''").trim();
			volume =volume.replace('\0', ' ').replace("'", "''").trim();
			issue =issue.replace('\0', ' ').replace("'", "''").trim();
			description =description.replace('\0', ' ').replace("'", "''").trim();
			subject =subject.replace('\0', ' ').replace("'", "''").trim();
			beginpage =beginpage.replace('\0', ' ').replace("'", "''").trim();
			endpage =endpage.replace('\0', ' ').replace("'", "''").trim();
			date_created =date_created.replace('\0', ' ').replace("'", "''").trim();
			down_date =down_date.replace('\0', ' ').replace("'", "''").trim();
			journalId =journalId.replace('\0', ' ').replace("'", "''").trim();
			ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
			provider_subject =provider_subject.replace('\0', ' ').replace("'", "''").trim();
			if_html_fulltex =if_html_fulltex.replace('\0', ' ').replace("'", "''").trim();
			if_pdf_fulltext =if_pdf_fulltext.replace('\0', ' ').replace("'", "''").trim();
			cited_cnt =cited_cnt.replace('\0', ' ').replace("'", "''").trim();
			down_cnt =down_cnt.replace('\0', ' ').replace("'", "''").trim();
			is_oa =is_oa.replace('\0', ' ').replace("'", "''").trim();
			page =page.replace('\0', ' ').replace("'", "''").trim();
			date =date.replace('\0', ' ').replace("'", "''").trim();
			lngid =lngid.replace('\0', ' ').replace("'", "''").trim();
			gch=gch.replace('\0', ' ').replace("'", "''").trim();
			provider_url =provider_url.replace('\0', ' ').replace("'", "''").trim();
			provider_id =provider_id.replace('\0', ' ').replace("'", "''").trim();
				
			}
			

			XXXXObject xObj = new XXXXObject();
			xObj.data.put("batch", batch);
			xObj.data.put("rawid",rawid);
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
			xObj.data.put("beginpage", beginpage);
			xObj.data.put("endpage",endpage);
			xObj.data.put("date_created", date_created);
			xObj.data.put("down_date", down_date);
			xObj.data.put("journalId", journalId);
			xObj.data.put("ref_cnt",ref_cnt);
			xObj.data.put("provider_subject",provider_subject);
			xObj.data.put("if_html_fulltex", if_html_fulltex);
			xObj.data.put("if_pdf_fulltex", if_html_fulltex);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("down_cnt",down_cnt);
			xObj.data.put("is_oa",is_oa);
			xObj.data.put("page",page);
			xObj.data.put("date",date);
			xObj.data.put("lngid",lngid);
			xObj.data.put("type1",type1);
			xObj.data.put("medium",medium);
			xObj.data.put("language",language);
			xObj.data.put("country",country);
			xObj.data.put("gch",gch);
			xObj.data.put("provider",provider);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("provider_id",provider_id);
			xObj.data.put("sud_db_id",sud_db_id);


			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
			context.getCounter("map", "count").increment(1);

		}
	}
}
