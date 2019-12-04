package simple.jobstream.mapreduce.site.iopjournal;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
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


/**
 * <p>Description: 导出 iop 期刊到 db3 </p>  
 * @author qiuhongyang 2018年11月29日 上午10:37:16
 */
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
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

		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);

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
				String pathfile = "/vipuser/walker/log/log_map/" + nowDate + ".txt";
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

			String rawid = "";
			String title = "";
			String title_alternative = "";
			String creator = "";
			String creator_institution = "";
			String subject = "";
			String description = "";
			String description_en = "";
			String identifier_doi = "";
			String identifier_pissn = "";
			String volume = "";
			String issue = "";
			String source = "";
			String date = "";
			String page = "";	// iopjournal 站点 无页码信息
			String date_created = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_alternative")) {
					title_alternative = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description_en")) {
					description_en = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
			}

			if ((Integer.parseInt(date) < 1000) || (Integer.parseInt(date) > curYear + 1)) {
				context.getCounter("map", "std_err_date").increment(1);
				log2HDFSForMapper(context, "std_err_date:" + rawid + "," + date);
				return;
			}

			String language = "EN";
			String country = "UK"; // UK（英国）

			{
				title = title.replace('\0', ' ').replace("'", "''").trim();
				title_alternative = title_alternative.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				description_en = description_en.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				date = date.replace('\0', ' ').replace("'", "''").trim();
//				page = page.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
			}

			String lngid = "IOP_WK_" + rawid;
			String type = "3";
			String medium = "2";
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";

			String publisher = "IOP Publishing";

			String provider = "iopjournal";
			String provider_url = provider + "@http://iopscience.iop.org/article/" + rawid;
			String provider_id = provider + "@" + rawid;
			String gch = "";
			// String cover = "";
			if (identifier_pissn.length() > 0) {
				gch = provider + "@" + identifier_pissn;
				gch = gch.toLowerCase(); // 转为小写
			} else {
				context.getCounter("map", "error no issn").increment(1);
				System.err.println("**********error no issn:" + rawid);
			}

			String sql = "";
			{
				sql = "INSERT INTO modify_title_info_zt([lngid], " + "[rawid], " + "[title], " + "[title_alternative], "
						+ "[creator], " + "[creator_institution], " + "[subject], "
						+ "[description], " + "[description_en], " + "[identifier_doi], " + "[identifier_pissn], "
						+ "[volume], " + "[issue], " + "[source], " + "[date], " + "[date_created], " + "[type], "
						+ "[medium], " + "[batch], " + "[publisher], " + "[language], " + "[country], " + "[provider], "
						+ "[provider_url], " + "[provider_id], " + "[gch]) ";
				sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', "
						+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
						+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', " + "'%s', '%s', '%s');";
				sql = String.format(sql, lngid, rawid, title, title_alternative, creator, creator_institution, subject,
						description, description_en, identifier_doi, identifier_pissn, volume, issue,
						source, date, date_created, type, medium, batch, publisher, language, country, provider,
						provider_url, provider_id, gch);
			}

			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}

	
}