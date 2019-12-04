package simple.jobstream.mapreduce.site.tanf_qk;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.print.DocFlavor.STRING;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2Db3.class);

	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String postfixDb3 = "tandfjournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";

	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		jobName = "tanfqk." + jobName;
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
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
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);

	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public static String[] splitpage(String str) {
			String pagestart = "";
			String pageend = "";
			String[] listresult = new String[2];
			if (str.indexOf("-") != -1 && !str.endsWith("-")) {
				String[] listarray = str.split("-");
				pagestart = listarray[0];
				pageend = listarray[1];
			} else {
				pagestart = "";
				pageend = "";
			}
			listresult[0] = pagestart;
			listresult[1] = pageend;
			return listresult;
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String rawid = "";
			String id = "";
			String subject = "";
			String description = "";
			String publisher = "";
			String title = "";
			String identifier_pissn = "";
			String page = "";
			String identifier_doi = "";
			String source = "";
			String issue = "";
			String volume = "";
			String date = "";
			String date_created = "";
			String country = "";
			String language = "";
			String url = "";
			String pageCount = "";
			String creator = "";
			String gch = "";
			String cited_cnt = "";
			String ref_cnt = "";
			String beginpage = "";
			String endpage = "";
			String creator_institution = "";

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("id")) {
					id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pageCount")) {
					pageCount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("gch")) {
					gch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("startpage")) {
					beginpage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("endpage")) {
					endpage = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
			}
			String lngid = "TANDF_WK_" + id;
			String provider = "tandfjournal";
			String type = "3";
			String medium = "2";
			gch = provider + "@" + gch;

			String absurl = url;
			String provider_url = provider + '@' + absurl;
			String provider_id = provider + '@' + id;

			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			String batch = formatter.format(new Date());
			batch += "00";

			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
				page = page.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				date = date.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				pageCount = pageCount.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				gch = gch.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			}

			String sql = "INSERT INTO modify_title_info_zt([lngid], " + "[title]," + "[creator]," + "[source],"
					+ "[publisher]," + "[volume]," + "[issue]," + "[page]," + "[date_created]," + "[date],"
					+ "[language]," + "[identifier_pissn]," + "[identifier_doi]," + "[type]," + "[description],"
					+ "[provider]," + "[batch]," + "[medium]," + "[country]," + "[provider_url]," + "[provider_id],"
					+ "[rawid]," + "[subject]," + "[beginpage]," + "[endpage]," + "[pageCount]," + "[page],"
					+ "[gch],"+ "[cited_cnt],"+ "[ref_cnt],"+"[creator_institution])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s'," 
					+ "'%s','%s','%s','%s','%s'," 
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s'," 
					+ "'%s','%s','%s','%s','%s'," 
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s');";
			sql = String.format(sql, lngid, title, creator, source, publisher, volume, issue, page, date_created, date,
					language, identifier_pissn, identifier_doi, type, description, provider, batch, medium, country,
					provider_url, provider_id, rawid, subject, beginpage, endpage, pageCount, page, gch,cited_cnt,
					ref_cnt,creator_institution);
			
//			String sql = "INSERT INTO modify_title_info_zt([identifier_doi])";
//			sql += " VALUES ('%s');";
//			sql = String.format(sql, identifier_doi);

			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());

		}
	}

	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private FileSystem hdfs = null;
		private String tempDir = null;

		private SQLiteConnection connSqlite = null;
		private List<String> sqlList = new ArrayList<String>();

		private Counter sqlCounter = null;

		protected void setup(Context context) throws IOException, InterruptedException {
			try {
				System.setProperty("sqlite4java.library.path", "/usr/lib64/");

				// 创建存放db3文件的本地临时目录
				String taskId = context.getConfiguration().get("mapred.task.id");
				String JobDir = context.getConfiguration().get("job.local.dir");
				tempDir = JobDir + File.separator + taskId;
				File baseDir = new File(tempDir);
				if (!baseDir.exists()) {
					baseDir.mkdirs();
				}

				//
				hdfs = FileSystem.get(context.getConfiguration());
				sqlCounter = context.getCounter("reduce", "sqlCounter");

				String db3PathFile = baseDir.getAbsolutePath() + File.separator + taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(tempFileDb3); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(
						baseDir.getAbsolutePath() + File.separator + "." + taskId + "_" + postfixDb3 + ".db3.crc");
				if (crcFile.exists()) {
					if (crcFile.delete()) { // 删除crc文件
						logger.info("***** delete success:" + crcFile.toString());
					} else {
						logger.info("***** delete failed:" + crcFile.toString());
					}
				}

				connSqlite = new SQLiteConnection(new File(db3PathFile));
				connSqlite.open();
			} catch (Exception e) {
				logger.error("****************** setup failed. ******************", e);
			}

			logger.info("****************** setup finished  ******************");
		}

		public void insertSql(Context context) {
			String sql = "";
			if (sqlList.size() > 0) {
				try {
					connSqlite.exec("BEGIN TRANSACTION;");
					for (int i = 0; i < sqlList.size(); ++i) {
						sql = sqlList.get(i);
						connSqlite.exec(sql);
						sqlCounter.increment(1);
					}
					connSqlite.exec("COMMIT TRANSACTION;");

					sqlList.clear();

				} catch (Exception e) {
					context.getCounter("reduce", "insert error").increment(1);
					logger.error("***Error: insert failed. sql:" + sql, e);
				}
			}

		}

		public void reduce(Text key, Iterable<NullWritable> values, Context context)
				throws IOException, InterruptedException {

			sqlList.add(key.toString());

			if (sqlList.size() > 1000) {
				insertSql(context);
			}

			context.getCounter("reduce", "count").increment(1);
			context.write(key, NullWritable.get());
		}

		protected void cleanup(Context context) throws IOException, InterruptedException {
			logger.info("****************** Enter cleanup ******************");
			insertSql(context); // 处理余数
			if (connSqlite != null && connSqlite.isOpen()) {
				connSqlite.dispose(); // 关闭sqlite连接
			}

			try {
				File localDir = new File(tempDir);
				if (!localDir.exists()) {
					throw new FileNotFoundException(tempDir + " is not found.");
				}

				// 再次获取，这里并不能感知到pre获取的参数
				outputHdfsPath = context.getConfiguration().get("outputHdfsPath");
				// 最终存放db3的hdf目录。嵌在了MR的输出目录，便于自动清空。
				Path finalHdfsPath = new Path(outputHdfsPath + File.separator + "/db3/");

				/*
				 * if (!hdfs.exists(finalHdfsPath)) { //hdfs.delete(finalHdfsPath, true);
				 * hdfs.mkdirs(finalHdfsPath); //创建输出目录 }
				 */

				File[] files = localDir.listFiles();
				for (File file : files) {
					if (file.getName().endsWith(".db3")) {
						Path srcPath = new Path(file.getAbsolutePath());
						Path dstPash = new Path(finalHdfsPath.toString() + "/" + file.getName());
						hdfs.moveFromLocalFile(srcPath, dstPash); // 移动文件
						// hdfs.copyFromLocalFile(true, true, srcPath, dstPash); //删除local文件，并覆盖hdfs文件
						logger.info("copy " + srcPath.toString() + " to " + dstPash.toString());
					}
				}
			} catch (Exception e) {
				logger.error("****************** upload file failed. ******************", e);
			}
		}

	}
}