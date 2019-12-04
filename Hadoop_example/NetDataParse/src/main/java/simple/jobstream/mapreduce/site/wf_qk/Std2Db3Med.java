package simple.jobstream.mapreduce.site.wf_qk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class Std2Db3Med extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2Db3Med.class);
	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	private static String postfixDb3 = "wanfang_qk_med";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";

	public void pre(Job job) {
		String jobName = "wf_qk_med." + this.getClass().getSimpleName();
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
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);

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
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {

		private static String rawid = "";
		private static String pykm = "";
		private static String issn = "";
		private static String cnno = "";
		private static String title_c = "";
		private static String title_e = "";
		private static String remark_c = "";
		private static String remark_e = "";
		private static String doi = "";
		private static String author_c = "";
		private static String author_e = "";
		private static String organ = "";
		private static String name_c = "";
		private static String name_e = "";
		private static String years = "";
		private static String vol = "";
		private static String num = "";
		private static String sClass = "";
		private static String keyword_c = "";
		private static String keyword_e = "";
		private static String imburse = "";
		private static String pageline = "";
		private static String pagecount = "";
		private static String ref_cnt = "";
		private static String cited_cnt = "";
		private static String beginpage = "";
		private static String endpage = "";
		private static String jumppage = "";
		private static String muinfo = "";
		private static String pub1st = "0"; // 是否优先出版

		private static Set<String> pykmSet = new HashSet<String>();

		public void setup(Context context) throws IOException, InterruptedException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());

			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/detail/med/pykm.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					line = line.trim();
					if (line.length() < 1) {
						continue;
					}
					pykmSet.add(line);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}
			System.out.println("pykmSet size:" + pykmSet.size());
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
				String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
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

		private String[] parsePageInfo(String line) {
			String beginpage = "";
			String endpage = "";
			String jumppage = "";

			int idx = line.indexOf(',');
			if (idx > 0) {
				jumppage = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉加号及以后部分
			}
			idx = line.indexOf('-');
			if (idx > 0) {
				endpage = line.substring(idx + 1).trim();
				line = line.substring(0, idx).trim(); // 去掉减号及以后部分
			}
			beginpage = line.trim();
			if (endpage.length() < 1) {
				endpage = beginpage;
			}

			String[] vec = { beginpage, endpage, jumppage };
			return vec;
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			context.getCounter("map", "inCount").increment(1);
			{
				rawid = "";
				pykm = "";
				issn = "";
				cnno = "";
				title_c = "";
				title_e = "";
				remark_c = "";
				remark_e = "";
				doi = "";
				author_c = "";
				author_e = "";
				organ = "";
				name_c = "";
				name_e = "";
				years = "";
				vol = "";
				num = "";
				sClass = "";
				keyword_c = "";
				keyword_e = "";
				imburse = "";
				pageline = "";
				pagecount = "";
				ref_cnt = "";
				cited_cnt = "";
				beginpage = "";
				endpage = "";
				jumppage = "";
				muinfo = "";
				pub1st = "0"; // 是否优先出版
			}

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cnno")) {
					cnno = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_e")) {
					remark_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_c")) {
					author_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_e")) {
					author_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("name_c")) {
					name_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("name_e")) {
					name_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sClass")) {
					sClass = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_e")) {
					keyword_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("imburse")) {
					imburse = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pageline")) {
					pageline = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pagecount")) {
					pagecount = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("muinfo")) {
					muinfo = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub1st")) {
					pub1st = updateItem.getValue().trim();
				}
			}

			// 过滤掉非医学刊
			if (!pykmSet.contains(pykm)) {
				context.getCounter("map", "not med").increment(1);
				return;
			}

			// rawid 为 doi，非万方自有数据
			if (rawid.startsWith("10.")) {
				context.getCounter("map", "rawid_10.").increment(1);
				return;
			}

			sClass = sClass.replaceAll("\\s+", ";");

			String[] vec = parsePageInfo(pageline);
			beginpage = vec[0];
			endpage = vec[1];
			jumppage = vec[2];

			// 补缺
			{
				if (title_c.length() < 1) {
					title_c = title_e;
				}
			}

			String title = title_c;
			String title_alternative = title_e;
			String creator = author_c;
			String creator_en = author_e;
			String creator_institution = organ;
			String subject = keyword_c;
			String subject_en = keyword_e;
			String description = remark_c;
			String description_en = remark_e;
			String identifier_doi = doi;
			String identifier_pissn = issn;
			String identifier_cnno = cnno;
			String volume = vol;
			String issue = num;
			String title_series = muinfo;
			String source = name_c;
			String source_en = name_e;
			String description_fund = imburse.replace('%', ';');;
			String date = years;
			String page = pageline;
			String subject_clc = sClass;
			String date_created = years + "0000";

			String lngid = "WANFANG_MED_QK_" + rawid;
			String type = "3";
			String medium = "2";
			String language = "ZH";
			String country = "CN";
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			if (Integer.parseInt(date) < 2000) {
				// 2天前
				batch = (new SimpleDateFormat("yyyyMMdd")).format((new Date()).getTime() - 2 * 24 * 60 * 60 * 1000)
						+ "00";
			}
			context.getCounter("map", "batch " + batch).increment(1);

			String provider = "wanfangmedjournal";
			String provider_url = provider + "@http://med.wanfangdata.com.cn/Paper/Detail/PeriodicalPaper_" + rawid;
			String provider_id = provider + "@" + rawid;
			String gch = provider + "@" + pykm;

			// 转义
			{
				title = title.replace('\0', ' ').replace("'", "''").trim();
				title_alternative = title_alternative.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				creator_en = creator_en.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				subject_en = subject_en.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				description_en = description_en.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
				identifier_cnno = identifier_cnno.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				source_en = source_en.replace('\0', ' ').replace("'", "''").trim();
				description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
				date = date.replace('\0', ' ').replace("'", "''").trim();
				page = page.replace('\0', ' ').replace("'", "''").trim();
				beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
				endpage = endpage.replace('\0', ' ').replace("'", "''").trim();
				jumppage = jumppage.replace('\0', ' ').replace("'", "''").trim();
				pagecount = pagecount.replace('\0', ' ').replace("'", "''").trim();
				subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
			}

			String sql = "";
			{
				sql = "INSERT INTO modify_title_info_zt([lngid], " + "[rawid], " + "[title], " + "[title_alternative], "
						+ "[creator], " + "[creator_en], " + "[creator_institution], " + "[subject], "
						+ "[subject_en], " + "[description], " + "[description_en], " + "[identifier_doi], "
						+ "[identifier_pissn], " + "[identifier_cnno], " + "[volume], " + "[issue], "
						+ "[title_series], " + "[source], " + "[source_en], " + "[description_fund], " + "[date], "
						+ "[page], " + "[beginpage], " + "[endpage], " + "[jumppage], " + "[pagecount], " + "[subject_clc], "
						+ "[ref_cnt], " + "[cited_cnt], " + "[date_created], " + "[type], " + "[medium], " + "[batch], "
						+ "[language], " + "[country], " + "[provider], " + "[provider_url], " + "[provider_id], "
						+ "[gch]) ";
				sql += " VALUES ('%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', "
						+ "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s', " + "'%s');";
				sql = String.format(sql, lngid, rawid, title, title_alternative, creator, creator_en,
						creator_institution, subject, subject_en, description, description_en, identifier_doi,
						identifier_pissn, identifier_cnno, volume, issue, title_series, source, source_en,
						description_fund, date, page, beginpage, endpage, jumppage, pagecount, subject_clc, ref_cnt, cited_cnt,
						date_created, type, medium, batch, language, country, provider, provider_url, provider_id, gch);
			}
			context.getCounter("map", "outCount").increment(1);

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