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
public class StdPubmed extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdPubmed.class);

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
			String lngid = "";
			String rawid = "";
			String sub_db_id = "00029";
			String product = "pubmed";
			String provider = "pubmed";
			String down_date = "";
			String batch = "";
			String doi = "";
			String source_type = "3";
			String provider_url = "";
			String title = "";
			String keyword = "";
			String abstracts = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String recv_date = "";
			String accept_date = "";
			String revision_date = "";
			String language = "EN";
			String country = "US";
			String author = "";
			String organ = "";
			String journal_name = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String issn = "";
			String pub_date = "";
			String pmcid = "";
			String midId = "";
			String journal_raw_id = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngID")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstracts")) {
					abstracts = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("recv_date")) {
					recv_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("accept_date")) {
					accept_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("revision_date")) {
					revision_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					journal_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("journal_raw_id")) {
					journal_raw_id = updateItem.getValue().trim();

				}

			}
			if (page_info != null && page_info.indexOf("-") > 0) {
				if (page_info.split("-").length == 2) {

					begin_page = page_info.split("-")[0];
					end_page = page_info.split("-")[1];
				}

			}
			if (pub_date.length() > 4) {
				pub_year = pub_date.substring(0, 4);

			}
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim();
			author = author.replace('\0', ' ').replace("'", "''").trim();
			organ = organ.replace('\0', ' ').replace("'", "''").trim();
			keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
			abstracts = abstracts.replace('\0', ' ').replace("'", "''").trim();
			doi = doi.replace('\0', ' ').replace("'", "''").trim();
			journal_raw_id = journal_raw_id.replace('\0', ' ').replace("'", "''").trim();
			page_info = page_info.replace('\0', ' ').replace("'", "''").trim();
			begin_page = begin_page.replace('\0', ' ').replace("'", "''").trim();
			end_page =end_page.replace('\0', ' ').replace("'", "''").trim();
			
		
		
			
			provider_url = "https://www.ncbi.nlm.nih.gov/pubmed/" + rawid;
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();

			String sql = "INSERT INTO base_obj_meta_a([lngid], [rawid], [sub_db_id], [product], [provider], [down_date], [batch], [doi], [source_type], [provider_url], [title], [keyword], [abstract], [page_info], [begin_page], [end_page], [recv_date], [accept_date], [revision_date], [language], [country], [author], [organ], [journal_name], [pub_year], [vol], [num], [issn], [pub_date],[journal_raw_id]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, sub_db_id, product, provider, down_date, batch, doi, source_type,
					provider_url, title, keyword, abstracts, page_info, begin_page, end_page, recv_date, accept_date,
					revision_date, language, country, author, organ, journal_name, pub_year, vol, num, issn, pub_date,
					journal_raw_id);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}

}