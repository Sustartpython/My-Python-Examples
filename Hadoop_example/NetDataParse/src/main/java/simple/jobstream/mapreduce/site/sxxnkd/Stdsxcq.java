package simple.jobstream.mapreduce.site.sxxnkd;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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



import org.json.JSONArray;
import org.json.JSONObject;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class Stdsxcq extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Stdsxcq.class);

	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "chineseallbook.xxxobj2std";
	

		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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
		System.out.println("******io.compression.codecs*******"
				+ job.getConfiguration().get("io.compression.codecs"));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {

		private static Map<String, String> monthMap = new HashMap<String, String>();
	
		public String getMapValueByKey(String mykey) {
			String value = "00";
			for (Map.Entry entry : monthMap.entrySet()) {

				String key = entry.getKey().toString();
				if (mykey.toLowerCase().startsWith(key)) {
					value = entry.getValue().toString();
					break;

				}

			}
			return value;

		}

		public void setup(Context context) throws IOException,
				InterruptedException {
		}

		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String rawid = "";//id
			String title = "";//主题
			String cover="";
			String date="";
			String publisher = "";
			String description = "";
			String creator = "";
			String date_created = "";
			String provider = "xnkjuchineseallbook";
			String country = "CN";
			String type = "1";
			String medium = "2";
			String language = "ZH";
			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
					title = title.replace("'", "''");
				}
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("cover")) {
					cover = updateItem.getValue().trim();
					cover = cover.replace("'", "''");
					cover = cover.toLowerCase();
				}
				
				if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
					publisher = publisher.replace("'", "''");
				}

				if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
					description = description.replace("'", "''");
				}

				if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
					creator = creator.replace("'", "''").replace("，", ";").replace(",", ";").replace("；", ";");
				}
				if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
					date_created = date_created.replace("'", "''");
					if(date_created.length() < 4){
						date_created = "19000000";
						date = "1900";
					}else {
						date_created = date_created+"0000";
						date_created = date_created.substring(0,8);
						date = date_created.substring(0,4);
					}
				}

			}
			String provider_id = "xnkjuchineseallbook@"+rawid;
			String lngid = "XNKJU_CHINESEALL_TS_"+rawid;
			String provider_url = "xnkjuchineseallbook@http://sxxkd.chineseall.cn/v3/book/detail/"+rawid;
			String batch = (new SimpleDateFormat("yyyyMMdd"))
					.format(new Date()) + "00";
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			String sql = "INSERT INTO modify_title_info_zt([rawid],[title],[cover],[language],[date],[date_created],[creator],[description],[publisher], [language], [medium], "
					+ " [type], [country], [provider], [provider_id], [lngid], [provider_url],[batch]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s', '%s',  '%s','%s','%s',  '%s', '%s', '%s');";
			sql = String.format(sql,rawid,title,cover,language,date,date_created,creator,description,publisher, language, medium, type, country, provider, provider_id, lngid,provider_url,batch);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}
	}

	public static class ProcessReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		private FileSystem hdfs = null;
		private String tempDir = null;

		private SQLiteConnection connSqlite = null;
		private List<String> sqlList = new ArrayList<String>();

		private Counter sqlCounter = null;

		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				System.setProperty("sqlite4java.library.path", "/usr/lib64/");

				// 创建存放db3文件的本地临时目录
				String taskId = context.getConfiguration()
						.get("mapred.task.id");
				String JobDir = context.getConfiguration().get("job.local.dir");
				tempDir = JobDir + File.separator + taskId;
				File baseDir = new File(tempDir);
				if (!baseDir.exists()) {
					baseDir.mkdirs();
				}

				//
				hdfs = FileSystem.get(context.getConfiguration());
				sqlCounter = context.getCounter("reduce", "sqlCounter");

				String tempPathFile = "/RawData/_rel_file/zt_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator
						+ taskId + "_" + "chineseallbook.db3";
				Path src = new Path(tempPathFile); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath()
						+ File.separator + "." + taskId + "_" + "chineseallbook.db3.crc");
				if (crcFile.exists()) {
					if (crcFile.delete()) { // 删除crc文件
						logger.info("***** delete success:"
								+ crcFile.toString());
					} else {
						logger.info("***** delete failed:" + crcFile.toString());
					}
				}

				connSqlite = new SQLiteConnection(new File(db3PathFile));
				connSqlite.open();
			} catch (Exception e) {
				logger.error(
						"****************** setup failed. ******************",
						e);
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

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {

			sqlList.add(key.toString());

			if (sqlList.size() > 1000) {
				insertSql(context);
			}

			context.getCounter("reduce", "count").increment(1);
			context.write(key, NullWritable.get());
		}

		protected void cleanup(Context context) throws IOException,
				InterruptedException {
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
				outputHdfsPath = context.getConfiguration().get(
						"outputHdfsPath");
				// 最终存放db3的hdf目录。嵌在了MR的输出目录，便于自动清空。
				Path finalHdfsPath = new Path(outputHdfsPath + File.separator
						+ "/db3/");

				File[] files = localDir.listFiles();
				for (File file : files) {
					if (file.getName().endsWith(".db3")) {
						Path srcPath = new Path(file.getAbsolutePath());
						Path dstPash = new Path(finalHdfsPath.toString() + "/"
								+ file.getName());
						hdfs.moveFromLocalFile(srcPath, dstPash); // 移动文件
						// hdfs.copyFromLocalFile(true, true, srcPath, dstPash);
						// //删除local文件，并覆盖hdfs文件
						logger.info("copy " + srcPath.toString() + " to "
								+ dstPash.toString());
					}
				}
			} catch (Exception e) {
				logger.error(
						"****************** upload file failed. ******************",
						e);
			}
		}

	}
}