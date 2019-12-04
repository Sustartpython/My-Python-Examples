package simple.jobstream.mapreduce.site.jamajournal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdJama extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "jamajournal." + this.getClass().getSimpleName();
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
				String pathfile = "/lqx/log/log_map/" + nowDate + ".txt";
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
			
			String sub_db_id = "";
			
			String down_date = "";
			String identifier_doi = "";
			String title = "";
			String title_series = "";
			String identifier_pissn = "";
			String creator = "";
			String creator_institution = "";
			String publisher = "";
			String date = "";
			String description = "";
			String subject = "";
			String volume = "";
			String date_created = "";
			String page = "";
			String beginpage = "";
			String endpage = "";
			String pagecount = "";
			String issue = "";
			String lngID = "";
			String batch = "";
			String provider = "jamajournal";
			String provider_url = "";
			String provider_id = "";
			String country = "US";
			String language = "EN";
			String type = "3";
			String medium = "2";
			String rawid = "";
			String source = "";
			String parse_time = "";
			String gch = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_series")) {
					title_series = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("beginpage")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("endpage")) {
					endpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pagecount")) {
					pagecount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("parse_time")) {
					parse_time = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("gch_id")) {
					gch = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}

			}
			if (title.length() < 2) {
				return;
			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			gch = provider + "@" + gch;
			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;
			
//			lngID = VipIdEncode.getLngid(sub_db_id, rawid ,false);
			lngID = "JAMA_WK_" + rawid;

			down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
			identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			date = date.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			volume = volume.replace('\0', ' ').replace("'", "''").trim();
			issue = issue.replace('\0', ' ').replace("'", "''").trim();
			date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
			page = page.replace('\0', ' ').replace("'", "''").trim();
			beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
			endpage = endpage.replace('\0', ' ').replace("'", "''").trim();
			pagecount = pagecount.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			parse_time = parse_time.replace('\0', ' ').replace("'", "''").trim();
			batch = batch.replace('\0', ' ').replace("'", "''").trim();
			provider_id = provider_id.replace('\0', ' ').replace("'", "''").trim();
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
			lngID = lngID.replace('\0', ' ').replace("'", "''").trim();
			provider = provider.replace('\0', ' ').replace("'", "''").trim();
			country = country.replace('\0', ' ').replace("'", "''").trim();
			language = language.replace('\0', ' ').replace("'", "''").trim();
			type = type.replace('\0', ' ').replace("'", "''").trim();
			medium = medium.replace('\0', ' ').replace("'", "''").trim();
			gch = gch.replace('\0', ' ').replace("'", "''").trim();

//			down_date, identifier_doi, title, title_series, identifier_pissn, creator, creator_institution, 
//			publisher, date, description, subject, volume, issue, date_created, page, beginpage, endpage,pagecount,source,parse_time ,
//			batch, provider_id, provider_url, lngID, provider, country, language, type, medium

			String sql = "insert into modify_title_info_zt(rawid,identifier_doi,title,title_series,identifier_pissn,creator,"
					+ "creator_institution,publisher,date,description,subject,volume,issue,date_created,page,beginpage,"
					+ "endpage,pagecount,source,batch,provider_id,provider_url,lngID,provider,country,language,type,medium,gch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, rawid, identifier_doi, title, title_series, identifier_pissn, creator,
					creator_institution, publisher, date, description, subject, volume, issue, date_created, page,
					beginpage, endpage, pagecount, source, batch, provider_id, provider_url, lngID, provider, country,
					language, type, medium, gch);

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());

		}
	}

}