package simple.jobstream.mapreduce.user.walker.wf_qk_med;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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

public class Std2Db3Med extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2Db3Med.class);
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

		// A层字段
		private String lngid = "";
		private String rawid = "";
		private String sub_db_id = "";
		private String product = "";
		private String sub_db = "";
		private String provider = "";
		private String down_date = "";
		private String batch = "";
		private String doi = "";
		private String source_type = "";
		private String provider_url = "";
		private String title = "";
		private String title_alt = "";
		private String title_sub = "";
		private String title_series = "";
		private String keyword = "";
		private String keyword_alt = "";
		private String keyword_machine = "";
		private String clc_no_1st = "";
		private String clc_no = "";
		private String clc_machine = "";
		private String subject_word = "";
		private String subject_edu = "";
		private String subject = "";
		private String abstract_ = ""; // 摘要，因关键字冲突，后面加下划线
		private String abstract_alt = "";
		private String abstract_type = "";
		private String abstract_alt_type = "";
		private String page_info = "";
		private String begin_page = "";
		private String end_page = "";
		private String jump_page = "";
		private String doc_code = "";
		private String doc_no = "";
		private String raw_type = "";
		private String recv_date = "";
		private String accept_date = "";
		private String revision_date = "";
		private String pub_date = "";
		private String pub_date_alt = "";
		private String pub_place = "";
		private String page_cnt = "";
		private String pdf_size = "";
		private String fulltext_txt = "";
		private String fulltext_addr = "";
		private String fulltext_type = "";
		private String column_info = "";
		private String fund = "";
		private String fund_alt = "";
		private String author_id = "";
		private String author_1st = "";
		private String author = "";
		private String author_raw = "";
		private String author_alt = "";
		private String corr_author = "";
		private String corr_author_id = "";
		private String email = "";
		private String subject_dsa = "";
		private String research_field = "";
		private String contributor = "";
		private String contributor_id = "";
		private String contributor_alt = "";
		private String author_intro = "";
		private String organ_id = "";
		private String organ_1st = "";
		private String organ = "";
		private String organ_alt = "";
		private String preferred_organ = "";
		private String host_organ_id = "";
		private String organ_area = "";
		private String journal_raw_id = "";
		private String journal_name = "";
		private String journal_name_alt = "";
		private String pub_year = "";
		private String vol = "";
		private String num = "";
		private String is_suppl = "";
		private String issn = "";
		private String eissn = "";
		private String cnno = "";
		private String publisher = "";
		private String cover_path = "";
		private String is_oa = "";
		private String country = "";
		private String language = "";
		private String ref_cnt = "";
		private String ref_id = "";
		private String cited_id = "";
		private String cited_cnt = "";
		private String down_cnt = "";
		private String is_topcited = "";
		private String is_hotpaper = "";

		private Set<String> pykmSet = new HashSet<String>();
		private HashMap<String, String> wf2gchMap = new HashMap<String, String>();

		// 初始化万方医学的拼音刊名列表(小写化)
		private void initPykmSet(FileSystem fs) throws IllegalArgumentException, IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/detail/med/pykm.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					pykmSet.add(line);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("pykmSet size:" + pykmSet.size());
		}

		// 初始化万方期刊拼音刊名与维普gch映射表（拼音刊名小写化、gch大写化）
		private void initWF2gchMap(FileSystem fs) throws IllegalArgumentException, IOException {
			FSDataInputStream fin = fs.open(new Path("/user/ganruoxun/InfoList/qidgch.txt"));
			BufferedReader in = null;
			String line;
			String[] vec = null;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					vec = line.trim().split("\t");
					if (vec.length != 2) {
						continue;
					}
					if (vec[0].length() != 6) {
						continue;
					}
					String pykm = vec[1].trim().toLowerCase();
					String gch = vec[0].trim().toUpperCase();
					if (wf2gchMap.containsKey(pykm)) {
						gch = wf2gchMap.get(pykm) + ";" + gch;						
					}
					wf2gchMap.put(pykm, gch);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("wf2gchMap size:" + wf2gchMap.size());
		}

		public void setup(Context context) throws IOException, InterruptedException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			initPykmSet(fs);
			initWF2gchMap(fs);

//			fs.close();		// 全局文件系统，不能关闭
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "inCount").increment(1);
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

			// 过滤掉非医学刊
			if (!pykmSet.contains(journal_raw_id.toLowerCase())) {
				context.getCounter("map", "not med").increment(1);
				return;
			}

			// rawid 为 doi，非万方自有数据
			if (rawid.startsWith("10.")) {
				context.getCounter("map", "rawid_10.").increment(1);
				return;
			}


			String sql = "";
			// 转义
			{
				title = title.replace('\0', ' ').replace("'", "''").trim();
				String title_alternative = title_alt.replace('\0', ' ').replace("'", "''").trim();
				String creator = author.replace('\0', ' ').replace("'", "''").trim();
				String creator_en = author.replace('\0', ' ').replace("'", "''").trim();
				String creator_institution = organ.replace('\0', ' ').replace("'", "''").trim();
				String subject = keyword.replace('\0', ' ').replace("'", "''").trim();
				String subject_en = keyword_alt.replace('\0', ' ').replace("'", "''").trim();
				String description = abstract_.replace('\0', ' ').replace("'", "''").trim();
				String description_en = abstract_alt.replace('\0', ' ').replace("'", "''").trim();
				String identifier_doi = doi.replace('\0', ' ').replace("'", "''").trim();
				String identifier_pissn = issn.replace('\0', ' ').replace("'", "''").trim();
				String identifier_cnno = cnno.replace('\0', ' ').replace("'", "''").trim();
				String volume = vol.replace('\0', ' ').replace("'", "''").trim();
				String issue = num.replace('\0', ' ').replace("'", "''").trim();
				title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
				String source = journal_name.replace('\0', ' ').replace("'", "''").trim();
				String source_en = journal_name_alt.replace('\0', ' ').replace("'", "''").trim();
				String description_fund = fund.replace('\0', ' ').replace("'", "''").trim();
				String date = pub_year.replace('\0', ' ').replace("'", "''").trim();
				String page = page_info.replace('\0', ' ').replace("'", "''").trim();
				String beginpage = begin_page.replace('\0', ' ').replace("'", "''").trim();
				String endpage = end_page.replace('\0', ' ').replace("'", "''").trim();
				String jumppage = jump_page.replace('\0', ' ').replace("'", "''").trim();
				String pagecount = page_cnt.replace('\0', ' ').replace("'", "''").trim();
				String subject_clc = clc_no.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				String date_created = pub_date.replace('\0', ' ').replace("'", "''").trim();

				String lngid = "WANFANG_MED_QK_" + rawid;
				String type = "3";
				String medium = "2";
//				String language = "ZH";
//				String country = "CN";
				String provider = "wanfangmedjournal";
				String provider_url = provider + "@http://med.wanfangdata.com.cn/Paper/Detail/PeriodicalPaper_" + rawid;
				String provider_id = provider + "@" + rawid;
				String gch = "";
				// 转换万方拼音刊名为维普gch
				if (wf2gchMap.containsKey(journal_raw_id.toLowerCase())) {
					gch = wf2gchMap.get(journal_raw_id.toLowerCase());
				}
				
				String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
				if (Integer.parseInt(date) < 2000) {
					// 2天前
					batch = (new SimpleDateFormat("yyyyMMdd")).format((new Date()).getTime() - 2 * 24 * 60 * 60 * 1000)
							+ "00";
				}
				sql = "INSERT INTO modify_title_info_zt([lngid], " + "[rawid], " + "[title], " + "[title_alternative], "
						+ "[creator], " + "[creator_en], " + "[creator_institution], " + "[subject], "
						+ "[subject_en], " + "[description], " + "[description_en], " + "[identifier_doi], "
						+ "[identifier_pissn], " + "[identifier_cnno], " + "[volume], " + "[issue], "
						+ "[title_series], " + "[source], " + "[source_en], " + "[description_fund], " + "[date], "
						+ "[page], " + "[beginpage], " + "[endpage], " + "[jumppage], " + "[pagecount], "
						+ "[subject_clc], " + "[ref_cnt], " + "[cited_cnt], " + "[date_created], " + "[type], "
						+ "[medium], " + "[batch], " + "[language], " + "[country], " + "[provider], "
						+ "[provider_url], " + "[provider_id], " + "[gch]) ";
				sql += " VALUES ('%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s');";
				sql = String.format(sql, lngid, rawid, title, title_alternative, creator, creator_en,
						creator_institution, subject, subject_en, description, description_en, identifier_doi,
						identifier_pissn, identifier_cnno, volume, issue, title_series, source, source_en,
						description_fund, date, page, beginpage, endpage, jumppage, pagecount, subject_clc, ref_cnt,
						cited_cnt, date_created, type, medium, batch, language, country, provider, provider_url,
						provider_id, gch);
			}
			context.getCounter("map", "outCount").increment(1);

			context.write(new Text(sql), NullWritable.get());
		}
	}
}