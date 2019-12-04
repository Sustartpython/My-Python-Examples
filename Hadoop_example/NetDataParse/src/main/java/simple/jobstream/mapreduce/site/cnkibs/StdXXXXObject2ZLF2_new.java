package simple.jobstream.mapreduce.site.cnkibs;

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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
//import org.apache.taglibs.standard.tag.el.sql.UpdateTag;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import simple.jobstream.mapreduce.site.cnkibs.ClassLoad;
import simple.jobstream.mapreduce.site.cnkibs.ClassType;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
//A层直接转智立方
//不需修改
public class StdXXXXObject2ZLF2_new extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdXXXXObject2ZLF2_new.class);

	private static boolean testRun = true;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "cnkithesis";

	public void pre(Job job) {
		String jobName = "cnkithesis.StdXXXXObject_zlf";

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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		private static HashMap<String, String> FirstClassMap = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap = new HashMap<>();
		private static ClassType classtype = null;

		public void setup(Context context) throws IOException, InterruptedException {
			String firstclass_info = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);

			FirstClassMap = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap = classload.getsecondclass();

			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);

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
			String title = "";
			String subject_clc = "";
			String subject = "";
			String creator = "";
			String creator_descipline = "";
			String date = "";
			String creator_degree = "";// 学位
			String creator_institution = "";// 学校
			String description = "";
			String contributor = "";
			String language = "1";
			String country = "CN";
			String type = "2";
			String medium = "";
			String batch = "";
			String description_fund = "";
			String provider_url="";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("degree")) {
					creator_degree = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("subject_dsa")) {
					creator_descipline = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("clc_no")) {
					subject_clc = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}

				else if(updateItem.getKey().equals("fund")) {
					description_fund = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("provider_url")) {
					provider_url= updateItem.getValue().trim();
					String check = provider_url.replace("http://kns.cnki.net/kcms/detail/detail.aspx?","");
					// 处理provider_url问题
					if(check.length() >0) {
						
						if(check.split("&")[0].length()<8) {
							
							provider_url = "http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=" + "CMFD" + "&filename=" +rawid;
						}
					}
				}
			}

//			 String num = rawid.replace('\0', ' ').replace("'", "''").replace(".nh", "").trim();
//			 int a = Integer.parseInt(num);
//			 a = a*2+2;
//			 lngid = "C_BS_" + Integer.toString(a);
			title = title.replace('\0', ' ').replace("'", "''").trim();
			if (title.length() < 2) {
				return;
			}
			contributor = contributor.replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			creator_descipline = creator_descipline.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").replace(" ", ";").replace(",", ";").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim().replace(";;", ";");
			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
			if (description_fund.endsWith(";")) {
				description_fund = description_fund.substring(0, description_fund.length() - 1);
			}
//			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			//String NetFullTextAddr = "http://epub.cnki.net/kns/detail/detail.aspx?dbname=CMFD&filename=" + rawid;
			String NetFullTextAddr = provider_url;
			// String owner = "cqu";
			String titletype = "0;1;512;513";
			String srcID = "VIP";
			String srcproducer = "CNKI";
			JSONObject NetFullTextAddr_All = createNetFullTextAddr_All(NetFullTextAddr);
			JSONObject netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawid);
			String classtypes = classtype.GetClassTypes(subject_clc);
			String showclasstypes = classtype.GetShowClassTypes(subject_clc);

			String sql = "INSERT INTO modify_title_info([country],[language],[showclasstypes],[classtypes],[netfulltextaddr_all_std],[NetFullTextAddr_All],[srcID],[titletype],[srcproducer],[lngid], [rawid], [title_c],[Showwriter], [bsdegree], [bsspeciality], [Showorgan], [bstutorsname] ,[remark_c], [class], [keyword_c], [type],  [medium], [years],[Imburse],[batch],[netfulltextaddr]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s', '%s','%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, country, language, showclasstypes, classtypes, netfulltextaddr_all_std,
					NetFullTextAddr_All, srcID, titletype, srcproducer, lngid, rawid, title, creator, creator_degree,
					creator_descipline, creator_institution, contributor, description, subject_clc, subject, type,
					medium, date, description_fund, batch, NetFullTextAddr);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

		}

	}

	public static JSONObject createNetFullTextAddr_All(String addr) {
		String[] attr = { addr };
		String key = "CNKI@CNKIDATA";
		HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
		try {
			JSONArray array = new JSONArray(attr);
			map.put(key, array);
			JSONObject o = new JSONObject(map);

			return o;
		} catch (Exception e) {
			return null;
		}
	}

	public static JSONObject createnetfulltextaddr_all_std(String rawid) {
		String[] attr = { rawid };
		String key = "CNKI@CNKIDATA";
		HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
		try {
			JSONArray array = new JSONArray(attr);
			map.put(key, array);
			JSONObject o = new JSONObject(map);
			return o;
		} catch (Exception e) {
			return null;
		}
	}

	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
		private FileSystem hdfs = null;
		private String tempDir = null;

		private SQLiteConnection connSqlite = null;
		private List<String> sqlList = new ArrayList<String>();

		private Counter sqlCounter = null;

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
				String pathfile = "/vipuser/chenyong/log/log_map/" + nowDate + ".txt";
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

				String tempPathFile = "/RawData/cnki/bs/ref_file/cnkibs_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator + taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(tempPathFile); // 模板文件（HDFS路径）
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
					log2HDFSForMapper(context, sql);
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