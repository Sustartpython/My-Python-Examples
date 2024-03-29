package simple.jobstream.mapreduce.user.walker.cnki_qk;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

// 将A层数据导出成智立方db3格式
public class Std2Db3ZLFFilter extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2Db3ZLFFilter.class);
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
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
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
		// A层字段
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

		public void setup(Context context) throws IOException, InterruptedException {
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
		}

		private static String getLngIDByCnkiID(String cnkiID) {
			cnkiID = cnkiID.toUpperCase();
			String lngID = "";
			for (int i = 0; i < cnkiID.length(); i++) {
				lngID += String.format("%d", cnkiID.charAt(i) + 0);
			}
			
			return lngID;
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{ // A层字段
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
				} else if (updateItem.getKey().equals("is_topcited")) {
					is_topcited = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_hotpaper")) {
					is_hotpaper = updateItem.getValue().trim();
				}
			}
			

			if ((down_cnt.indexOf(';') < 0) && (cited_cnt.indexOf(';') < 0)) {
				context.getCounter("map", "no ;").increment(1);
				return;
			}
			context.getCounter("map", "semicolon").increment(1);
			

			// 转义及生成SQL语句
			String sql = "INSERT INTO main([lngid], [bookid], [issn], [cnno], [rawid], [cnkiid], [pub1st], "
					+ "[muinfo], [pageline], [pagecount], [ref_cnt], [cited_cnt], "
					+ "[beginpage], [endpage], [jumppage], [bid], [title_c], [title_e], "
					+ "[firstwriter], [showwriter], [cbmwriter], [writer], [author_e], "
					+ "[firstorgan], [organ], [name_c], [name_e], [showorgan], "
					+ "[remark_c], [remark_e], [keyword_c], [keyword_e], [imburse], "
					+ "[doi], [class], [firstclass], [years], [vol], [num], "
					+ "[fromtype], [down_date], [down_cnt], [fulltext_type], [if_html_fulltext], [publishdate]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', " 
					+ "'%s', '%s', '%s', '%s', '%s', '%s');";
			{
				issn = issn.replace('\0', ' ').replace("'", "''").trim();
				cnno = cnno.replace('\0', ' ').replace("'", "''").trim();
				String title_c = title.replace('\0', ' ').replace("'", "''").trim();
				String title_e = title_alt.replace('\0', ' ').replace("'", "''").trim();
				String author_c = author.replace('\0', ' ').replace("'", "''").trim();
				String author_e = author_raw.replace('\0', ' ').replace("'", "''").trim();
				String firstwriter = author_1st.replace('\0', ' ').replace("'", "''").trim();
				String showwriter = author.replace('\0', ' ').replace("'", "''").trim();
				String cbmwriter = author.replace('\0', ' ').replace("'", "''").trim();
				String writer = author.replaceAll("(\\[.*?\\])", "").replace('\0', ' ').replace("'", "''").trim();
				String firstorgan = organ_1st.replace('\0', ' ').replace("'", "''").trim();
				String showorgan = organ.replace('\0', ' ').replace("'", "''").trim();
				organ = organ.replace('\0', ' ').replace("'", "''").trim();
				String name_c = journal_name.replace('\0', ' ').replace("'", "''").trim();
				String name_e = journal_name_alt.replace('\0', ' ').replace("'", "''").trim();
				String remark_c = abstract_.replace('\0', ' ').replace("'", "''").trim();
				String remark_e = abstract_alt.replace('\0', ' ').replace("'", "''").trim();
				String keyword_c = keyword.replace('\0', ' ').replace("'", "''").trim();
				String keyword_e = keyword_alt.replace('\0', ' ').replace("'", "''").trim();
				String imburse = fund.replace('\0', ' ').replace("'", "''").trim();
				doi = doi.replace('\0', ' ').replace("'", "''").trim();
				String sClass = clc_no.replace('\0', ' ').replace("'", "''").trim();
				String firstclass = clc_no_1st.replace('\0', ' ').replace("'", "''").trim();
				String years = pub_year.replace('\0', ' ').replace("'", "''").trim();
				vol = vol.replace('\0', ' ').replace("'", "''").trim();
				num = num.replace('\0', ' ').replace("'", "''").trim();
				String pageline = page_info.replace('\0', ' ').replace("'", "''").trim();
				String pagecount = page_cnt.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				String beginpage = begin_page.replace('\0', ' ').replace("'", "''").trim();
				String endpage = end_page.replace('\0', ' ').replace("'", "''").trim();
				String jumppage = jump_page.replace('\0', ' ').replace("'", "''").trim();
				String muinfo = column_info.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
				down_cnt = down_cnt.replace('\0', ' ').replace("'", "''").trim();
				fulltext_type = fulltext_type.replace('\0', ' ').replace("'", "''").trim();
				String publishdate = pub_date.replace('\0', ' ').replace("'", "''").trim();

				lngid = getLngIDByCnkiID(rawid);
				String pykm = journal_raw_id;
				String bookid = lngid.substring(0, 20);
				if (lngid.length() < 26) {	//九几年的老规则，有点，比较短（WAVE803.010）
					bookid = lngid.substring(0, lngid.length()-6);
				}
				String if_html_fulltext = "0";
				if (fulltext_type.indexOf("html") > -1) {
					if_html_fulltext = "1";
				}
				String pub1st = "0";
				String fromtype = "CNKI";
				sql = String.format(sql, lngid, bookid, issn, cnno, rawid, rawid, pub1st, muinfo, pageline, pagecount, ref_cnt,
						cited_cnt, beginpage, endpage, jumppage, pykm, title_c, title_e, firstwriter, showwriter,
						cbmwriter, writer, author_e, firstorgan, organ, name_c, name_e, showorgan, remark_c, remark_e,
						keyword_c, keyword_e, imburse, doi, sClass, firstclass, years, vol, num, fromtype,
						down_date, down_cnt, fulltext_type, if_html_fulltext, publishdate);
			}

			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
		}
	}
}