package simple.jobstream.mapreduce.user.walker.wf_qk;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.healthmarketscience.jackcess.Database.FileFormat;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

import net.ucanaccess.jdbc.UcanaccessDriver;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.LogMR;

public class CountArticleGroupByBookid2Access extends InHdfsOutHdfsJobInfo {
	public static String inputTablePath = "";
	public static String outputTablePath = "";
	public static int reduceNum = 0;	// 外部传入

	public void pre(Job job) {
		String jobName = "wf_qk." + this.getClass().getSimpleName();
		job.setJobName(jobName);

		inputTablePath = job.getConfiguration().get("export2access.inputTablePath");
		outputTablePath = job.getConfiguration().get("export2access.outputTablePath");
	}

	public void SetMRInfo(Job job) {
		// JobConfUtil.setTaskPerMapMemory(job, 5000);
//		JobConfUtil.setTaskPerReduceMemory(job, 10000);
		// JobConfUtil.setTaskShareMapJVM(job, 200);

//		JobConfUtil.setTaskPerMapMemory(job, 3072);
		JobConfUtil.setTaskPerReduceMemory(job, 5120);

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(CountArticleGroupByBookid2Access.ProcessMapper.class);
		job.setReducerClass(CountArticleGroupByBookid2Access.ProcessReducer.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);
		reduceNum = Integer.valueOf(job.getConfiguration().get("export2access.rednum"));
		job.setNumReduceTasks(reduceNum);
	}

	public void post(Job job) {
	}

	@Override
	public String getHdfsInput() {
		return inputTablePath;
	}

	@Override
	public String getHdfsOutput() {
		return outputTablePath;
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, Text> {
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

		protected void setup(Context context) throws IOException, InterruptedException {

		}

		private static String getLngIDByWanID(String wanID) {
			wanID = wanID.toUpperCase();
			String lngID = "Wd";
			for (int i = 0; i < wanID.length(); i++) {
				lngID += String.format("%d", wanID.charAt(i) + 0);
			}

			return lngID;
		}

		private static String getBookId(String pykm, String years, String num) {
			String bookid = "";
			String line = pykm + years;
			if (0 == num.length()) {
				line += "00";
			} else if (1 == num.length()) {
				line += "0";
			}
			line += num;
			bookid = getLngIDByWanID(line);

			return bookid;
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

			lngid = getLngIDByWanID(rawid);
			String bookid = getBookId(journal_raw_id, pub_year, num);

			String outKey = bookid;
			String outValue = journal_raw_id + "\t" + pub_year + "\t" + num + "\t" + lngid;

			if (num.length() < 1) {
				context.getCounter("map", "num 0 count").increment(1);
				String line = outValue + "\t" + rawid;
				LogMR.log2HDFS4Mapper(context, "/user/qhy/log/log_map/" + DateTimeHelper.getNowDate() + ".txt", line);
				return;
			}

			context.getCounter("map", "count").increment(1);
			context.write(new Text(outKey), new Text(outValue));
		}
	}

	public static class ProcessReducer extends Reducer<Text, Text, Text, NullWritable> {
		int nCount = 0;
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		String tempDir = "";
		String dbName = "WFQKArticleCount.accdb";
		String taskDbName = "";
		private List<String> sqls = new ArrayList<String>();
		private Counter totalCount = null;
		private Counter successCount = null;

		protected void setup(Context context) throws IOException, InterruptedException {

			String sSqlCreate = context.getConfiguration().get("export2access.sSqlCreate");

			String JobDir = context.getConfiguration().get("job.local.dir");
			String taskId = context.getConfiguration().get("mapred.task.id");

			taskDbName = taskId + "_" + dbName;
			tempDir = JobDir + File.separator + taskId;
			File baseDir = new File(tempDir);
			if (!baseDir.exists()) {
				baseDir.mkdirs();
			}

			/*
			 * String sSqlCreate = "CREATE TABLE modify_title_info ("; for (int i = 0; i <
			 * fields.size(); i++) { String field = fields.get(i); String datatype =
			 * dtypes.get(i);
			 * 
			 * sSqlCreate += String.format("%s %s,", field, datatype); } sSqlCreate =
			 * sSqlCreate.substring(0, sSqlCreate.length() - 1); sSqlCreate += ")";
			 */

			// System.out.println(sSqlCreate);

			try {
				Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
//				String url = UcanaccessDriver.URL_PREFIX + tempDir + File.separator + taskDbName
//						+ ";newdatabaseversion=" + FileFormat.V2003.name();
				String url = UcanaccessDriver.URL_PREFIX + tempDir + File.separator + taskDbName
						+ ";newdatabaseversion=" + FileFormat.V2010.name();
				conn = DriverManager.getConnection(url, "", "");
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				stmt.execute(sSqlCreate);

			} catch (Exception ex) {
				ex.printStackTrace();
			}

			totalCount = context.getCounter("ReducerCount", "TOTAL_COUNT");
			successCount = context.getCounter("ReducerCount", "SUCCESS_COUNT");
		}

		public void reduce(Text key, Iterable<Text> values, Context context) throws IOException, InterruptedException {
			String bookid = key.toString();
			String pykm = "";
			String years = "";
			String num = "";
			String lngid = "";

			String outLine = key.toString();
			int cnt = 0;
			for (Text val : values) {
				cnt += 1;
				if (cnt == 1) {
					String line = val.toString();
					String[] vec = line.split("\t"); // split 会忽略掉最后的分隔符

					outLine += "\t" + line;
					pykm = vec[0].trim();
					years = vec[1].trim();
					num = vec[2].trim();
					lngid = vec[3].trim();
				}
			}
			outLine += "\t" + cnt;

			String sql = "insert into ArticleCount(pykm, years, num, bookid, cnt)";
			sql += "values('%s', '%s', '%s', '%s', %d)";
			sql = String.format(sql, pykm, years, num, bookid, cnt);
			totalCount.increment(1);
			sqls.add(sql);
			if (sqls.size() > 5000) {
				insertSql();
			}

			context.getCounter("reduce", "count").increment(1);
			context.write(new Text(outLine), NullWritable.get());
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			insertSql();
			FileSystem hdfs;
			hdfs = FileSystem.get(context.getConfiguration());
			Path srcPath = new Path(tempDir + File.separator + taskDbName);
			String rootDir = context.getConfiguration().get("vipcloud.hdfs.proc.root.dir");
			Path dbPath;
			outputTablePath = context.getConfiguration().get("export2access.outputTablePath");
			if (rootDir == null) {
				dbPath = new Path(outputTablePath + File.separator + taskDbName);
			} else {
				dbPath = new Path(rootDir + outputTablePath + File.separator + taskDbName);
			}
			if (stmt != null) {
				try {
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) {
				try {
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			System.out.println("srcPath:" + srcPath);
			System.out.println("dbPath:" + dbPath);
			hdfs.copyFromLocalFile(srcPath, dbPath);
		}

		/*
		 * public void insertSql() { try { if (stmt != null) { for (int i = 0; i <
		 * sqls.size(); i++) { stmt.execute(sqls.get(i)); successCount.increment(1); }
		 * conn.commit(); sqls.clear(); } } catch (Exception e) { e.printStackTrace();
		 * //System.out.println(_keyid); } }
		 */
		public void insertSql() {
			if (stmt != null) {
				for (int i = 0; i < sqls.size(); i++) {
					String sqlinsert = sqls.get(i);
					try {
						stmt.execute(sqlinsert);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(sqlinsert);
					}
					successCount.increment(1);
				}

				try {
					conn.commit();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				sqls.clear();
			}
		}
	}

}
