package simple.jobstream.mapreduce.site.sinomed_kp;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.tools.SimpleCopyListing;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.site.cssci.StdCsscimeta.ProcessMapper;

//输入应该为去重后的html
public class Stdkp extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = job.getConfiguration().get("jobName");
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

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String journal_id = ""; // 期刊id
			String source = ""; // 期刊名
			String publisher = "";
			String identifier_pissn = ""; // issn
			String identifier_cnno = ""; // cn
			String provider_url = "";
			String title = ""; // 标题
			String creator = ""; // 作者
			String creator_institution = ""; // 作者单位
			String description = ""; // 摘要
			String pub_place = ""; // 出版地
			String date = ""; // 年
			String date_created = ""; // 出版时间
			String volume = "";
			String issue = ""; // 期
			String page = ""; // 页码信息
			String beginpage = ""; // 起始页
			String endpage = ""; // 结束页
			String subject_clc = ""; // 中图分类号
			String subject = ""; // 关键词
			String keyword = "";
			String rawid = "";
			String lngid = "";
			String provider = "sinomedkpjournal";
			String country = "CN";
			String language = "ZH";
			String provider_id = ""; // 来源id
			String medium = "2";
			String batch = "";
			String type = "3";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("journal_raw_id")) {
					journal_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cnno")) {
					identifier_cnno = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_place")) {
					pub_place = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					endpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_word")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				}

			}
			if (subject.equals("")) {
				subject = keyword;
			} else if (keyword.equals("")) {
				subject = subject;
			} else {
				subject = subject + ";" + keyword;
			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;
			String gch = provider + "@" + journal_id;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			} else {
				date = "1900";
				date_created = "19000000";
			}

			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace("  ", "");

			String sql = "insert into modify_title_info_zt (source,publisher,identifier_pissn,identifier_cnno,provider_url,title,creator,creator_institution,description,pub_place,date,date_created,issue,page,beginpage,endpage,subject_clc,subject,rawid,lngid,country,language,provider_id,medium,batch,gch,volume,type,provider)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, source, publisher, identifier_pissn, identifier_cnno, provider_url, title, creator,
					creator_institution, description, pub_place, date, date_created, issue, page, beginpage, endpage,
					subject_clc, subject, rawid, lngid, country, language, provider_id, medium, batch, gch, volume,
					type, provider);
			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());

		}
	}
}