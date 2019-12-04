package simple.jobstream.mapreduce.site.apajournal;

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
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.site.cssci.StdCsscimeta.ProcessMapper;

//输入应该为去重后的html
public class Stdapa extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 20;

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

			String url = "";

			String title = "";// 标题
			// String title_alternative="";
			String creator = "";// 作者
			String publisher = "";// 发行商
			String date = "";// 出版日期
			String creator_institution = "";// 机构
			String description = "";// 内容提要
			String provider_subject = "";
			String rawid = "";//
			String subject = "";
			String page = "";

			String source = "";
			String identifier_pissn = "";
			String identifier_eissn = "";

			String lngID = "";
			String batch = "";
			String gch = "";
			String provider = "apajournal";
			String provider_url = "";
			String provider_id = "";
			String country = "US";

			String date_created = "";
			String language = "EN";
			String type = "3";
			String medium = "2";

			String beginpage = "";
			String endpage = "";
			String volume = "";
			String issue = "";
			String identifier_doi = "";
			String rawtype = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					endpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngID = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_raw_id")) {
					gch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					rawtype = updateItem.getValue().trim();
				}
			}
//			if (title.length() >= 2) {
//				return;
//			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;
			gch = provider + "@" + rawid;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			} else {
				date = "1900";
				date_created = "19000000";
			}

			// date_created = date + "0000";

			// 转义sql字符
			// title =
			// StringEscapeUtils.escapeSql(title.replaceAll("《","").replaceAll("》","")).trim();//标题
			// title_alternative =
			// StringEscapeUtils.escapeSql(title_alternative.replaceAll("《","").replaceAll("》","")).trim();//标题
			// creator =
			// StringEscapeUtils.escapeSql(creator.replaceAll("；",";").replaceAll("，",";").replaceAll(";
			// ", ";").replaceAll(", ",";").replaceAll(",","")).trim();//作者
			// identifier_pisbn =
			// StringEscapeUtils.escapeSql(identifier_pisbn).trim();//isbn号

			// subject=StringEscapeUtils.escapeSql(subject);//关键字
			// description =
			// StringEscapeUtils.escapeSql(description).trim();//内容提要
			// provider_subject =
			// StringEscapeUtils.escapeSql(provider_subject).trim();
			// publishers = StringEscapeUtils.escapeSql(publishers).trim();

			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();

			String sql = "insert into modify_title_info_zt(lngid,rawid,identifier_doi,gch,title,creator,source,identifier_pissn,identifier_eissn,provider_subject,beginpage,endpage,description,date,date_created,publisher,page,creator_institution,subject,volume,issue,language,country,provider,provider_url,provider_id,rawtype,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID, rawid, identifier_doi, gch, title, creator, source, identifier_pissn,
					identifier_eissn, provider_subject, beginpage, endpage, description, date, date_created, publisher,
					page, creator_institution, subject, volume, issue, language, country, provider, provider_url,
					provider_id, rawtype, type, medium, batch);

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());

		}
	}
}