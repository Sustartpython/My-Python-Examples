package simple.jobstream.mapreduce.site.sagejournal;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
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
public class StdXXXXObject extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdXXXXObject.class);

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "sagejournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";

	public void pre(Job job) {
		job.setJobName(job.getConfiguration().get("jobName"));
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

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		JobConfUtil.setTaskPerMapMemory(job, 1024 * 10);
		JobConfUtil.setTaskPerReduceMemory(job, 1024 * 8);
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
				String pathfile = "/user/qianjun/log/log_map/" + nowDate + ".txt";
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
			String identifier_doi = "";
			String title = "";
			String identifier_eissn = "";
			String identifier_pissn = "";
			String creator_institution = "";
			String creator = "";
			String gch = "";
			String source = "";
			String subject = "";
			String cited_cnt = "";
			String volume = "";
			String issue = "";
			String description = "";
			String page = "";
			String beginpage = "";
			String endpage = "";
			String date = "";
			String date_created = "";
			String journalId = "";
			String language = "EN";
			String country = "US";
			String provider = "sagejournal";
			String provider_url = "";
			String provider_id = "";
			String type = "3";
			String medium = "2";
			String batch = "";
			String publisher = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim().replace("/doi/full/", "").replace("/doi/abs/", "");
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim().replace(" [", "[");
				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
					if (page != null && page.indexOf("–") > 0) {
						beginpage = page.split("–")[0];
						endpage = page.split("–")[1];

					}

				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();

				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("journalId")) {
					gch = provider + "@" + updateItem.getValue().trim();
				}

			}
			lngid = "SAGE_WK_" + rawid;
			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			provider_url = provider + "@https://journals.sagepub.com/doi/full/" + rawid;
			provider_id = provider + "@" + rawid;

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
				type = type.replace('\0', ' ').replace("'", "''").trim();
				medium = medium.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();

			}

			String sql = "INSERT INTO modify_title_info_zt([lngid],[rawid],[title],[identifier_doi],[identifier_eissn],[date_created],[creator],[gch],[source],[volume],[issue],[description],[page],[beginpage],[endpage],[date],[country],[language],[provider_url],[provider_id],[provider],[type],[medium],[batch],[subject],[cited_cnt],[publisher],[identifier_pissn],[creator_institution]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngid, rawid, title, identifier_doi, identifier_eissn, date_created, creator, gch,
					source, volume, issue, description, page, beginpage, endpage, date, country, language, provider_url,
					provider_id, provider, type, medium, batch, subject, cited_cnt, publisher, identifier_pissn,
					creator_institution);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}


}