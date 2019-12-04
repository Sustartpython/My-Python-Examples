package simple.jobstream.mapreduce.site.plosjournal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.LogMR;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdPlosjournal4New extends InHdfsOutHdfsJobInfo {
//	public static Logger logger = Logger.getLogger(StdMspjournal4New.class);
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "plosjournal." + this.getClass().getSimpleName();
//		String jobName = job.getConfiguration().get("jobName");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(jobName);
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

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			text = nowTime + "\n" + text + "\n\n";

			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统
				FileSystem fs = FileSystem.get(context.getConfiguration());

				FSDataOutputStream fout = null;
				String pathfile = "/qinym/log/log_map/" + nowDate + ".txt";
				if (fs.exists(new Path(pathfile))) {
					fout = fs.append(new Path(pathfile));
				} else {
					fout = fs.create(new Path(pathfile));
				}

				out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
				out.write(text);
				out.close();

			} catch (Exception ex) {
				bException = true;
			}

			if (bException) {
				return false;
			} else {
				return true;
			}
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String lngid = "";
			String rawid = "";
			String down_date = "";
			String batch = "";
			String doi = "";
			String provider_url = "";
			String title = "";
			String abstract_ = "";
			String raw_type = "";
			String keyword = "";
			String recv_date = "";
			String accept_date = "";
			String fulltext_type = "";
			String pub_date = "";
			String column_info = "";
			String fund = "";
			String author_1st = "";
			String author = "";
			String author_intro = "";
			String organ_1st = "";
			String organ = "";
			String journal_raw_id = "";
			String journal_name = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String publisher = "";
			String issn = "";
			String is_oa = "";
			String ref_cnt = "";
//			String cited_cnt = "";
//			String down_cnt = "";
			String orc_id = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					abstract_ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("recv_date")) {
					recv_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("accept_date")) {
					accept_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fulltext_type")) {
					fulltext_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("column_info")) {
					column_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fund")) {
					fund = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_intro")) {
					author_intro = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_1st")) {
					organ_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_raw_id")) {
					journal_raw_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					journal_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("orc_id")) {
					orc_id = updateItem.getValue().trim();
				} 
			}

			if (title.length() < 2) {
				return;
			}  

			String sub_db_id = "00044";
			String sub_db = "QK";
			String product = "PLOS";
			String provider = "PLOS";
			String source_type = "3";
			String country = "US";
			String language = "EN";

			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
 
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
//			down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
//			batch = batch.replace('\0', ' ').replace("'", "''").trim();
			doi = doi.replace('\0', ' ').replace("'", "''").trim();
//			provider_url =provider_url.replace('\0', ' ').replace("'", "''").trim(); 
			title = title.replace('\0', ' ').replace("'", "''").trim();
			keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
//			recv_date = recv_date.replace('\0', ' ').replace("'", "''").trim();
 			abstract_ = abstract_.replace('\0', ' ').replace("'", "''").trim();
 			column_info = column_info.replace('\0', ' ').replace("'", "''").trim();
			fund = fund.replace('\0', ' ').replace("'", "''").trim();
			author_1st = author_1st.replace('\0', ' ').replace("'", "''").trim();
			author = author.replace('\0', ' ').replace("'", "''").trim();
			author_intro = author_intro.replace('\0', ' ').replace("'", "''").trim();
			organ_1st = organ_1st.replace('\0', ' ').replace("'", "''").trim();
			organ = organ.replace('\0', ' ').replace("'", "''").trim();
			journal_raw_id = journal_raw_id.replace('\0', ' ').replace("'", "''").trim();
			journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim(); 
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim(); 
			orc_id = orc_id.replace('\0', ' ').replace("'", "''").trim(); 

			String sql = "insert into base_obj_meta_a([lngid],[rawid],[down_date],[batch],[doi],"
					+ "[provider_url],[title],[abstract],[raw_type],[keyword],[recv_date],[accept_date],[fulltext_type],[pub_date],[column_info],[fund],"
					+ "[author_1st],[author],[author_intro],[organ_1st],[organ],[journal_raw_id],[journal_name],"
					+ "[pub_year],[vol],[num],[publisher],[issn],[is_oa],[ref_cnt],[orc_id],"
					+ "[sub_db_id],[sub_db],[product],[provider],[source_type],[country],[language])";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s','%s','%s','%s','%s'," 
					+ "'%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngid, rawid, down_date, batch, doi,
					provider_url, title,abstract_,raw_type, keyword, recv_date,accept_date, fulltext_type,pub_date, column_info,fund, 
					author_1st, author, author_intro, organ_1st, organ, journal_raw_id,journal_name,
					pub_year, vol, num, publisher, issn, is_oa, ref_cnt,orc_id,  
					sub_db_id,sub_db, product, provider, source_type, country, language);

//			String logHDFSFile = "/user/qinym/log/" + DateTimeHelper.getNowDate() + "sql.txt";
//			LogMR.log2HDFS4Mapper(context, logHDFSFile, "sql:" + sql);

			// logger.error("*****=====" + sql);
			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;

			context.write(new Text(sql), NullWritable.get());
		}
	}

}