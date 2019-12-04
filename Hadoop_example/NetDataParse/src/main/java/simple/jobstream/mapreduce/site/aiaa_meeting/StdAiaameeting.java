package simple.jobstream.mapreduce.site.aiaa_meeting;

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
public class StdAiaameeting extends InHdfsOutHdfsJobInfo {
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

			String lngid = "";
			String rawid = "";
			String title = ""; //标题
			String identifier_eisbn = ""; //eisbn
			String identifier_doi = ""; //doi
			String creator = ""; //作者
			String source = ""; //会议名称
			String source_institution = ""; //会议地点
			String publisher = ""; //出版单位
			String subject = "";  //关键词
			String date_created =""; //会议日期
			String title_series = "";//记录名
			String language = "EN";
			String country = "US";
			String type = "6";
			String provider = "aiaaconference";
			String provider_url = "";
			String provider_id = ""; //来源id
			String batch = "";
			String date = "";
			String medium = "2";
			

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eisbn")) {
					identifier_eisbn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_name")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_place")) {
					source_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_word")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_type")) {
					type = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("meeting_record_name")) {
					title_series = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}
			}


			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;

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
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			title_series = title_series.replace('\0', ' ').replace("'", "''").trim();

			String sql = "insert into modify_title_info_zt (lngid,rawid,title,identifier_eisbn,identifier_doi,creator,source,source_institution,publisher,subject,date_created ,language,country,type,provider,provider_url,provider_id,batch,date,medium,title_series)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql,lngid,rawid,title,identifier_eisbn,identifier_doi,creator,source,source_institution,publisher,subject,date_created ,language,country,type,provider,provider_url,provider_id,batch,date,medium,title_series);

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());

		}
	}
}