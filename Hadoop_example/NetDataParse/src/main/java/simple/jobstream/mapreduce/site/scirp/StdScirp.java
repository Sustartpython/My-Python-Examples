package simple.jobstream.mapreduce.site.scirp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.site.sagejournal.StdXXXXObject;

//输入应该为去重后的html
public class StdScirp extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdScirp.class);
	private static String logHDFSFile = "/user/qianjun/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "scirp." + this.getClass().getSimpleName();

		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public void post(Job job) {

	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		job.setOutputValueClass(BytesWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);

	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String batch = "";
			String rawid = "";
			String identifier_doi = "";
			String title = "";
			String identifier_pissn = "";
			String identifier_eissn = "";
			String creator = "";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String volume = "";
			String issue = "";
			String description = "";
			String subject = "";
			String beginpage = "";
			String endpage = "";
			String date_created = "";
			String down_date = "";
			String journalId = "";
			String ref_cnt = "";
			String provider_subject = "";
			String if_html_fulltext = "0";
			String if_pdf_fulltext = "0";
			String cited_cnt = "";
			String down_cnt = "";
			String is_oa = "";
			String page = "";
			String date = "";
			String lngid = "";
			String type1 = "3";
			String medium = "2";
			String language = "EN";
			String country = "US";
			String gch = "";
			String provider = "scirpjournal";
			String provider_url = "";
			String provider_id = "";
			String fulltext_type="";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract_")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					endpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_cnt")) {
					down_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					provider_subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_raw_id")) {
					journalId = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("fulltext_type")) {
					 fulltext_type = updateItem.getValue().trim();
					}

			}

			// 生成gch
			gch = provider + "@" + journalId;
			provider_id = provider + "@" + rawid;
			provider_url = provider + "@https://www.scirp.org/Journal/PaperInformation.aspx?PaperID=" + rawid;
			
			// 处理是否有html全文，pdf全文
			if(fulltext_type.contains("pdf")){
				if_pdf_fulltext="1";	
			}
			if(fulltext_type.contains("html")){
				if_html_fulltext="1";
			}
			//智图batch
			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			// 转义
			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				identifier_eissn = identifier_eissn.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				gch = gch.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				page = page.replace('\0', ' ').replace("'", "''").trim();
				beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
				endpage = endpage.replace('\0', ' ').replace("'", "''").trim();
				date = date.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				provider_id = provider_id.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				type1 = type1.replace('\0', ' ').replace("'", "''").trim();
				medium = medium.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();

			}

			String sql = "INSERT INTO modify_title_info_zt([batch],[rawid],[identifier_doi],[title],[identifier_pissn],[identifier_eissn],[creator],[creator_institution],[source],[publisher],[volume],[issue],[description],[subject],[beginpage],[endpage],[date_created],[provider],[ref_cnt],[provider_subject],[if_html_fulltext],[if_pdf_fulltext],[cited_cnt],[down_cnt],[is_oa],[page],[date],[lngid],[type],[medium],[language],[country],[gch],[provider_url],[provider_id]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql,batch, rawid, identifier_doi, title, identifier_pissn, identifier_eissn, creator,
					creator_institution, source, publisher, volume, issue, description, subject, beginpage, endpage,
					date_created,provider, ref_cnt, provider_subject, if_html_fulltext, if_pdf_fulltext,
					cited_cnt, down_cnt, is_oa, page, date, lngid, type1, medium, language, country, gch, provider_url,
					provider_id);
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "sql:" + sql);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}

}