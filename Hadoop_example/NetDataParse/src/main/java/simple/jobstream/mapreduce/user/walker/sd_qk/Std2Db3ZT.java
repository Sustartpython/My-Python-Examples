package simple.jobstream.mapreduce.user.walker.sd_qk;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;


// 将A层格式数据转化为智图格式db3导出
public class Std2Db3ZT extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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

		String logHDFSFile = "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt";

		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);
		
		// A层字段
		String lngid = ""; 
		String rawid = ""; 
		String sub_db_id = ""; 
		String product = ""; 
		String sub_db = ""; 
		String provider = ""; 
		String down_date = ""; 
		String batch = ""; 
		String translator = ""; 
		String translator_intro = ""; 
		String ori_src = ""; 
		String doi = ""; 
		String source_type = ""; 
		String provider_url = ""; 
		String title = ""; 
		String title_alt = ""; 
		String title_sub = ""; 
		String title_series = ""; 
		String keyword = ""; 
		String keyword_alt = ""; 
		String keyword_machine = ""; 
		String clc_no_1st = ""; 
		String clc_no = ""; 
		String clc_machine = ""; 
		String subject_word = ""; 
		String subject_edu = ""; 
		String subject = ""; 
		String abstract_ = ""; 
		String abstract_alt = ""; 
		String abstract_type = ""; 
		String abstract_alt_type = ""; 
		String page_info = ""; 
		String begin_page = ""; 
		String end_page = ""; 
		String jump_page = ""; 
		String doc_code = ""; 
		String doc_no = ""; 
		String raw_type = ""; 
		String recv_date = ""; 
		String accept_date = ""; 
		String revision_date = ""; 
		String pub_date = ""; 
		String pub_date_alt = ""; 
		String pub_place = ""; 
		String coden = ""; 
		String page_cnt = ""; 
		String pdf_size = ""; 
		String fulltext_txt = ""; 
		String fulltext_addr = ""; 
		String fulltext_type = ""; 
		String column_info = ""; 
		String fund = ""; 
		String fund_id = ""; 
		String fund_alt = ""; 
		String author_id = ""; 
		String author_1st = ""; 
		String author = ""; 
		String author_raw = ""; 
		String author_alt = ""; 
		String corr_author = ""; 
		String corr_author_id = ""; 
		String email = ""; 
		String subject_major = ""; 
		String research_field = ""; 
		String contributor = ""; 
		String contributor_id = ""; 
		String contributor_alt = ""; 
		String author_intro = ""; 
		String organ_id = ""; 
		String organ_1st = ""; 
		String organ = ""; 
		String organ_alt = ""; 
		String preferred_organ = ""; 
		String host_organ_id = ""; 
		String organ_area = ""; 
		String journal_raw_id = ""; 
		String journal_name = ""; 
		String journal_name_alt = ""; 
		String pub_year = ""; 
		String vol = ""; 
		String num = ""; 
		String is_suppl = ""; 
		String issn = ""; 
		String eissn = ""; 
		String cnno = ""; 
		String isbn = ""; 
		String publisher = ""; 
		String cover_path = ""; 
		String is_oa = ""; 
		String country = ""; 
		String language = ""; 
		String ref_cnt = ""; 
		String ref_id = ""; 
		String cited_id = ""; 
		String cited_cnt = ""; 
		String down_cnt = ""; 
		String orc_id = ""; 
		String researcher_id = ""; 
		String is_topcited = ""; 
		String is_hotpaper = ""; 
		String pubmed_id = ""; 
		String pmc_id = ""; 

		public void setup(Context context) throws IOException, InterruptedException {

		}

	

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			// A层字段
	    	{
	    		lngid = ""; 
	    		rawid = ""; 
	    		sub_db_id = ""; 
	    		product = ""; 
	    		sub_db = ""; 
	    		provider = ""; 
	    		down_date = ""; 
	    		batch = ""; 
	    		translator = ""; 
	    		translator_intro = ""; 
	    		ori_src = ""; 
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
	    		coden = ""; 
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
	    		subject_major = ""; 
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
	    		isbn = ""; 
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
	    		pubmed_id = ""; 
	    		pmc_id = ""; 

	    	}

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
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
				} else if (updateItem.getKey().equals("translator")) {
					translator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("translator_intro")) {
					translator_intro = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ori_src")) {
					ori_src = updateItem.getValue().trim();
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
				} else if (updateItem.getKey().equals("coden")) {
					coden = updateItem.getValue().trim();
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
				} else if (updateItem.getKey().equals("subject_major")) {
					subject_major = updateItem.getValue().trim();
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
				} else if (updateItem.getKey().equals("isbn")) {
					isbn = updateItem.getValue().trim();
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
				} else if (updateItem.getKey().equals("pubmed_id")) {
					pubmed_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pmc_id")) {
					pmc_id = updateItem.getValue().trim();
				}

			}

			
			try {
				Integer.parseInt(pub_year);
			} catch (Exception e) {
				context.getCounter("map", "eror pub_year").increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "eror pub_year:" + rawid + "," + pub_year);
				return;
			}
			
			if ((Integer.parseInt(pub_year) < 1000) || (Integer.parseInt(pub_year) > curYear + 2)) {
				context.getCounter("map", "std_err_date: " + pub_year).increment(1);
				LogMR.log2HDFS4Mapper(context, logHDFSFile, "std_err_date:" + rawid + "," + pub_year);
				return;
			}

			String sql = "INSERT INTO modify_title_info_zt"
					+ "([lngid], [rawid], [title], [title_alternative], [creator], [creator_institution], "
					+ "[subject], [provider_subject],[description], [description_en], [identifier_doi], "
					+ "[identifier_pissn], [volume], [issue], [source], [date], [date_created], "
					+ "[page], [beginpage], [endpage], [jumppage],"
					+ "[type], [medium], [batch], [publisher], [language], [country], "
					+ "[provider], [provider_url], [provider_id], [gch]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s');";
			{
//				String lngid = "SCIENCEDIRECT_WK_" + rawid;
				String type = "3";
				String medium = "2";
//				batch = batch.substring(0, 8) + "00";	// A 层 batch 转智图 batch
				batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
				if (Integer.parseInt(pub_year) < 2000) {
					// 2天前
					batch = (new SimpleDateFormat("yyyyMMdd")).format((new Date()).getTime() - 2 * 24 * 60 * 60 * 1000)
							+ "00";
				}
				context.getCounter("map", "batch " + batch).increment(1);

				publisher = "Elsevier Science";

				String provider = "sciencedirectjournal";
				provider_url = provider + "@" + provider_url;
				String provider_id = provider + "@" + rawid;
				String gch = "";
				// String cover = "";
				if (journal_raw_id.length() > 0) {
					gch = provider + "@" + journal_raw_id;
					gch = gch.toLowerCase(); // 转为小写
				} else {
					context.getCounter("map", "error no issn").increment(1);
					System.err.println("**********error no issn:" + rawid);
				}
				
				title = title.replace('\0', ' ').replace("'", "''").trim();
				String title_alternative = title_alt.replace('\0', ' ').replace("'", "''").trim();
				String creator = author.replace('\0', ' ').replace("'", "''").trim();
				String creator_institution = organ.replace('\0', ' ').replace("'", "''").trim();
				String provider_subject = subject.replace('\0', ' ').replace("'", "''").trim();
				subject = keyword.replace('\0', ' ').replace("'", "''").trim();
				String description = abstract_.replace('\0', ' ').replace("'", "''").trim();
				String description_en = abstract_alt.replace('\0', ' ').replace("'", "''").trim();
				String identifier_doi = doi.replace('\0', ' ').replace("'", "''").trim();
				String identifier_pissn = issn.replace('\0', ' ').replace("'", "''").trim();
				String volume = vol.replace('\0', ' ').replace("'", "''").trim();
				String issue = num.replace('\0', ' ').replace("'", "''").trim();
				String source = journal_name.replace('\0', ' ').replace("'", "''").trim();
				String date = pub_year.replace('\0', ' ').replace("'", "''").trim();
				String date_created = pub_date.replace('\0', ' ').replace("'", "''").trim();
				String page = page_info.replace('\0', ' ').replace("'", "''").trim();
				String beginpage = begin_page.replace('\0', ' ').replace("'", "''").trim();
				String endpage = end_page.replace('\0', ' ').replace("'", "''").trim();
				String jumppage = jump_page.replace('\0', ' ').replace("'", "''").trim();
				
				sql = String.format(sql, lngid, rawid, title, title_alternative, creator, creator_institution, subject,
						provider_subject, description, description_en, identifier_doi, identifier_pissn, volume, issue,
						source, date, date_created, page, beginpage, endpage, jumppage, type, medium, batch, publisher, language, country, provider,
						provider_url, provider_id, gch);
			}

			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());

		}
	}
}