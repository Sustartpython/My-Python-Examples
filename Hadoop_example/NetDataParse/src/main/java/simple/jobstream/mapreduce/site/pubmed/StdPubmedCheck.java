package simple.jobstream.mapreduce.site.pubmed;

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

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdPubmedCheck extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdPubmedCheck.class);

	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/CQU/springer/ref_file/coverid.txt";

	public void pre(Job job) {
		String jobName = "pubmed." + this.getClass().getSimpleName();

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
		private static String abstract_ = "";
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
		private static String orc_id = "";
		private static String researcher_id = "";
		private static String is_topcited = "";
		private static String is_hotpaper = "";

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public String[] getCoveridArray() throws IOException {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			// check if the file exists
			Path path = new Path(ref_file_path);
			if (fs.exists(path)) {
				FSDataInputStream is = fs.open(path);
				// get the file info to create the buffer
				FileStatus stat = fs.getFileStatus(path);

				// create the buffer
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
				is.readFully(0, buffer);

				String coveridString = new String(buffer);

				return coveridString.split("\\*");
			} else {
				return null;
			}

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
		
			{
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
				country = "";
				language = "";
				ref_cnt = "";
				ref_id = "";
				cited_id = "";
				cited_cnt = "";
				down_cnt = "";
				orc_id = "";
				researcher_id = "";
				is_topcited = "";
				is_hotpaper = "";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_type")) {
					source_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_alt")) {
					title_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_sub")) {
					title_sub = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_series")) {
					title_series = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_alt")) {
					keyword_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_machine")) {
					keyword_machine = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("clc_no_1st")) {
					clc_no_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("clc_no")) {
					clc_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("clc_machine")) {
					clc_machine = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_word")) {
					subject_word = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_edu")) {
					subject_edu = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					abstract_ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract_alt")) {
					abstract_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract_type")) {
					abstract_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract_alt_type")) {
					abstract_alt_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					begin_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					end_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("jump_page")) {
					jump_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doc_code")) {
					doc_code = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doc_no")) {
					doc_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("recv_date")) {
					recv_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("accept_date")) {
					accept_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("revision_date")) {
					revision_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date_alt")) {
					pub_date_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_place")) {
					pub_place = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_cnt")) {
					page_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pdf_size")) {
					pdf_size = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fulltext_txt")) {
					fulltext_txt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fulltext_addr")) {
					fulltext_addr = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fulltext_type")) {
					fulltext_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("column_info")) {
					column_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fund")) {
					fund = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fund_id")) {
					fund_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fund_alt")) {
					fund_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_id")) {
					author_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_raw")) {
					author_raw = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_alt")) {
					author_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("corr_author")) {
					corr_author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("corr_author_id")) {
					corr_author_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("email")) {
					email = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_dsa")) {
					subject_dsa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("research_field")) {
					research_field = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("contributor_id")) {
					contributor_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("contributor_alt")) {
					contributor_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_intro")) {
					author_intro = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_id")) {
					organ_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_1st")) {
					organ_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_alt")) {
					organ_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("preferred_organ")) {
					preferred_organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("host_organ_id")) {
					host_organ_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_area")) {
					organ_area = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_raw_id")) {
					journal_raw_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					journal_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name_alt")) {
					journal_name_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_suppl")) {
					is_suppl = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eissn")) {
					eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cnno")) {
					cnno = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cover_path")) {
					cover_path = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_id")) {
					ref_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_id")) {
					cited_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_cnt")) {
					down_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("orc_id")) {
					orc_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("researcher_id")) {
					researcher_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_topcited")) {
					is_topcited = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_hotpaper")) {
					is_hotpaper = updateItem.getValue().trim();
				}
			}

			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				sub_db_id = sub_db_id.replace('\0', ' ').replace("'", "''").trim();
				product = product.replace('\0', ' ').replace("'", "''").trim();
				sub_db = sub_db.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				doi = doi.replace('\0', ' ').replace("'", "''").trim();
				source_type = source_type.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				title_alt = title_alt.replace('\0', ' ').replace("'", "''").trim();
				title_sub = title_sub.replace('\0', ' ').replace("'", "''").trim();
				title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
				keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
				keyword_alt = keyword_alt.replace('\0', ' ').replace("'", "''").trim();
				keyword_machine = keyword_machine.replace('\0', ' ').replace("'", "''").trim();
				clc_no_1st = clc_no_1st.replace('\0', ' ').replace("'", "''").trim();
				clc_no = clc_no.replace('\0', ' ').replace("'", "''").trim();
				clc_machine = clc_machine.replace('\0', ' ').replace("'", "''").trim();
				subject_word = subject_word.replace('\0', ' ').replace("'", "''").trim();
				subject_edu = subject_edu.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				abstract_ = abstract_.replace('\0', ' ').replace("'", "''").trim();
				abstract_alt = abstract_alt.replace('\0', ' ').replace("'", "''").trim();
				abstract_type = abstract_type.replace('\0', ' ').replace("'", "''").trim();
				abstract_alt_type = abstract_alt_type.replace('\0', ' ').replace("'", "''").trim();
				page_info = page_info.replace('\0', ' ').replace("'", "''").trim();
				begin_page = begin_page.replace('\0', ' ').replace("'", "''").trim();
				end_page = end_page.replace('\0', ' ').replace("'", "''").trim();
				jump_page = jump_page.replace('\0', ' ').replace("'", "''").trim();
				doc_code = doc_code.replace('\0', ' ').replace("'", "''").trim();
				doc_no = doc_no.replace('\0', ' ').replace("'", "''").trim();
				raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
				recv_date = recv_date.replace('\0', ' ').replace("'", "''").trim();
				accept_date = accept_date.replace('\0', ' ').replace("'", "''").trim();
				revision_date = revision_date.replace('\0', ' ').replace("'", "''").trim();
				pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();
				pub_date_alt = pub_date_alt.replace('\0', ' ').replace("'", "''").trim();
				pub_place = pub_place.replace('\0', ' ').replace("'", "''").trim();
				page_cnt = page_cnt.replace('\0', ' ').replace("'", "''").trim();
				pdf_size = pdf_size.replace('\0', ' ').replace("'", "''").trim();
				fulltext_txt = fulltext_txt.replace('\0', ' ').replace("'", "''").trim();
				fulltext_addr = fulltext_addr.replace('\0', ' ').replace("'", "''").trim();
				fulltext_type = fulltext_type.replace('\0', ' ').replace("'", "''").trim();
				column_info = column_info.replace('\0', ' ').replace("'", "''").trim();
				fund = fund.replace('\0', ' ').replace("'", "''").trim();
				fund_id = fund_id.replace('\0', ' ').replace("'", "''").trim();
				fund_alt = fund_alt.replace('\0', ' ').replace("'", "''").trim();
				author_id = author_id.replace('\0', ' ').replace("'", "''").trim();
				author_1st = author_1st.replace('\0', ' ').replace("'", "''").trim();
				author = author.replace('\0', ' ').replace("'", "''").trim();
				author_raw = author_raw.replace('\0', ' ').replace("'", "''").trim();
				author_alt = author_alt.replace('\0', ' ').replace("'", "''").trim();
				corr_author = corr_author.replace('\0', ' ').replace("'", "''").trim();
				corr_author_id = corr_author_id.replace('\0', ' ').replace("'", "''").trim();
				email = email.replace('\0', ' ').replace("'", "''").trim();
				subject_dsa = subject_dsa.replace('\0', ' ').replace("'", "''").trim();
				research_field = research_field.replace('\0', ' ').replace("'", "''").trim();
				contributor = contributor.replace('\0', ' ').replace("'", "''").trim();
				contributor_id = contributor_id.replace('\0', ' ').replace("'", "''").trim();
				contributor_alt = contributor_alt.replace('\0', ' ').replace("'", "''").trim();
				author_intro = author_intro.replace('\0', ' ').replace("'", "''").trim();
				organ_id = organ_id.replace('\0', ' ').replace("'", "''").trim();
				organ_1st = organ_1st.replace('\0', ' ').replace("'", "''").trim();
				organ = organ.replace('\0', ' ').replace("'", "''").trim();
				organ_alt = organ_alt.replace('\0', ' ').replace("'", "''").trim();
				preferred_organ = preferred_organ.replace('\0', ' ').replace("'", "''").trim();
				host_organ_id = host_organ_id.replace('\0', ' ').replace("'", "''").trim();
				organ_area = organ_area.replace('\0', ' ').replace("'", "''").trim();
				journal_raw_id = journal_raw_id.replace('\0', ' ').replace("'", "''").trim();
				journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim();
				journal_name_alt = journal_name_alt.replace('\0', ' ').replace("'", "''").trim();
				pub_year = pub_year.replace('\0', ' ').replace("'", "''").trim();
				vol = vol.replace('\0', ' ').replace("'", "''").trim();
				num = num.replace('\0', ' ').replace("'", "''").trim();
				is_suppl = is_suppl.replace('\0', ' ').replace("'", "''").trim();
				issn = issn.replace('\0', ' ').replace("'", "''").trim();
				eissn = eissn.replace('\0', ' ').replace("'", "''").trim();
				cnno = cnno.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				cover_path = cover_path.replace('\0', ' ').replace("'", "''").trim();
				is_oa = is_oa.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				ref_id = ref_id.replace('\0', ' ').replace("'", "''").trim();
				cited_id = cited_id.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				down_cnt = down_cnt.replace('\0', ' ').replace("'", "''").trim();
				orc_id = orc_id.replace('\0', ' ').replace("'", "''").trim();
				researcher_id = researcher_id.replace('\0', ' ').replace("'", "''").trim();
				is_topcited = is_topcited.replace('\0', ' ').replace("'", "''").trim();
				is_hotpaper = is_hotpaper.replace('\0', ' ').replace("'", "''").trim();
			}
			if(rawid.equals("8547524")) {
				
				String sql = "INSERT INTO base_obj_meta_a([lngid],[rawid],[sub_db_id],[product],[sub_db],[provider],[down_date],[batch],[doi],[source_type],[provider_url],[title],[title_alt],[title_sub],[title_series],[keyword],[keyword_alt],[keyword_machine],[clc_no_1st],[clc_no],[clc_machine],[subject_word],[subject_edu],[subject],[abstract],[abstract_alt],[abstract_type],[abstract_alt_type],[page_info],[begin_page],[end_page],[jump_page],[doc_code],[doc_no],[raw_type],[recv_date],[accept_date],[revision_date],[pub_date],[pub_date_alt],[pub_place],[page_cnt],[pdf_size],[fulltext_txt],[fulltext_addr],[fulltext_type],[column_info],[fund],[fund_id],[fund_alt],[author_id],[author_1st],[author],[author_raw],[author_alt],[corr_author],[corr_author_id],[email],[subject_dsa],[research_field],[contributor],[contributor_id],[contributor_alt],[author_intro],[organ_id],[organ_1st],[organ],[organ_alt],[preferred_organ],[host_organ_id],[organ_area],[journal_raw_id],[journal_name],[journal_name_alt],[pub_year],[vol],[num],[is_suppl],[issn],[eissn],[cnno],[publisher],[cover_path],[is_oa],[country],[language],[ref_cnt],[ref_id],[cited_id],[cited_cnt],[down_cnt],[orc_id],[researcher_id],[is_topcited],[is_hotpaper]) ";
				sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
				sql = String.format(sql, lngid, rawid, sub_db_id, product, sub_db, provider, down_date, batch, doi,
						source_type, provider_url, title, title_alt, title_sub, title_series, keyword, keyword_alt,
						keyword_machine, clc_no_1st, clc_no, clc_machine, subject_word, subject_edu, subject, abstract_,
						abstract_alt, abstract_type, abstract_alt_type, page_info, begin_page, end_page, jump_page,
						doc_code, doc_no, raw_type, recv_date, accept_date, revision_date, pub_date, pub_date_alt,
						pub_place, page_cnt, pdf_size, fulltext_txt, fulltext_addr, fulltext_type, column_info, fund,
						fund_id, fund_alt, author_id, author_1st, author, author_raw, author_alt, corr_author,
						corr_author_id, email, subject_dsa, research_field, contributor, contributor_id, contributor_alt,
						author_intro, organ_id, organ_1st, organ, organ_alt, preferred_organ, host_organ_id, organ_area,
						journal_raw_id, journal_name, journal_name_alt, pub_year, vol, num, is_suppl, issn, eissn, cnno,
						publisher, cover_path, is_oa, country, language, ref_cnt, ref_id, cited_id, cited_cnt, down_cnt,
						orc_id, researcher_id, is_topcited, is_hotpaper);

				context.getCounter("map", "count").increment(1);

				context.write(new Text(sql), NullWritable.get());
				
			}
//			String sql = "INSERT INTO base_obj_meta_a([lngid],[rawid],[sub_db_id],[product],[sub_db],[provider],[down_date],[batch],[doi],[source_type],[provider_url],[title],[title_alt],[title_sub],[title_series],[keyword],[keyword_alt],[keyword_machine],[clc_no_1st],[clc_no],[clc_machine],[subject_word],[subject_edu],[subject],[abstract],[abstract_alt],[abstract_type],[abstract_alt_type],[page_info],[begin_page],[end_page],[jump_page],[doc_code],[doc_no],[raw_type],[recv_date],[accept_date],[revision_date],[pub_date],[pub_date_alt],[pub_place],[page_cnt],[pdf_size],[fulltext_txt],[fulltext_addr],[fulltext_type],[column_info],[fund],[fund_id],[fund_alt],[author_id],[author_1st],[author],[author_raw],[author_alt],[corr_author],[corr_author_id],[email],[subject_dsa],[research_field],[contributor],[contributor_id],[contributor_alt],[author_intro],[organ_id],[organ_1st],[organ],[organ_alt],[preferred_organ],[host_organ_id],[organ_area],[journal_raw_id],[journal_name],[journal_name_alt],[pub_year],[vol],[num],[is_suppl],[issn],[eissn],[cnno],[publisher],[cover_path],[is_oa],[country],[language],[ref_cnt],[ref_id],[cited_id],[cited_cnt],[down_cnt],[orc_id],[researcher_id],[is_topcited],[is_hotpaper]) ";
//			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
//			sql = String.format(sql, lngid, rawid, sub_db_id, product, sub_db, provider, down_date, batch, doi,
//					source_type, provider_url, title, title_alt, title_sub, title_series, keyword, keyword_alt,
//					keyword_machine, clc_no_1st, clc_no, clc_machine, subject_word, subject_edu, subject, abstract_,
//					abstract_alt, abstract_type, abstract_alt_type, page_info, begin_page, end_page, jump_page,
//					doc_code, doc_no, raw_type, recv_date, accept_date, revision_date, pub_date, pub_date_alt,
//					pub_place, page_cnt, pdf_size, fulltext_txt, fulltext_addr, fulltext_type, column_info, fund,
//					fund_id, fund_alt, author_id, author_1st, author, author_raw, author_alt, corr_author,
//					corr_author_id, email, subject_dsa, research_field, contributor, contributor_id, contributor_alt,
//					author_intro, organ_id, organ_1st, organ, organ_alt, preferred_organ, host_organ_id, organ_area,
//					journal_raw_id, journal_name, journal_name_alt, pub_year, vol, num, is_suppl, issn, eissn, cnno,
//					publisher, cover_path, is_oa, country, language, ref_cnt, ref_id, cited_id, cited_cnt, down_cnt,
//					orc_id, researcher_id, is_topcited, is_hotpaper);
//
//			context.getCounter("map", "count").increment(1);
//
//			context.write(new Text(sql), NullWritable.get());
		

		}
	}

}