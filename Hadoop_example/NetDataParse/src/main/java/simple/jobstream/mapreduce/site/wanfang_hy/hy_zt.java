package simple.jobstream.mapreduce.site.wanfang_hy;

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




import org.json.JSONArray;
import org.json.JSONObject;



import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
//不需修改
public class hy_zt extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(hy_zt.class);

	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "hy_zt";
		if (testRun) {
			jobName = "test_" + jobName;
		}

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

	
		public static String  wanID2vipID(String wanID){
			String vipID = "";
			for(char a : wanID.toCharArray()){
				if('0' <= a && a <= '9'){
					vipID = vipID + a;
				}else{
					vipID = vipID + Integer.toString(0 + a);
				}
				
				
			}
			vipID = "W_HY_"+ Long.toString( Long.parseLong(vipID)*2 + 3);
			return vipID;
		}
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String rawid = "";//id
			String title = "";//主题
			String date="";//会议年份
			String title_series="";//母体文献
			String creator = "";//作者
			String creator_institution = "";//机构
			String source = "";//会议名称
			String source_institution = "";//会议地点
			String subject = "";//关键词
			String description = "";//摘要
			String creator_release = "";//主办单位
			String description_fund = "";//基金
			String language = "ZH";//语言
			String type = "6";
			String country = "CN";
			String lngID = "";
			String subject_clc = ""; //分类号
			String date_created = "";
			String owner = "cqu";
			String year = "";
			String month = "";
			String day = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_c")) {
					title = updateItem.getValue().trim();
					title = title.replace("'", "''");
				}
				if (updateItem.getKey().equals("years")) {
					date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("hymeetingrecordname")) {
					title_series = updateItem.getValue().trim();
					title_series = title_series.replace("'", "''");
				}

				if (updateItem.getKey().equals("Showwriter")) {
					creator = updateItem.getValue().trim();
					creator = creator.replace("'", "''");
				}

				if (updateItem.getKey().equals("media_c")) {
					source = updateItem.getValue().trim();
					source = source.replace("'", "''");
				}
				if (updateItem.getKey().equals("hymeetingplace")) {
					source_institution = updateItem.getValue().trim();
					source_institution = source_institution.replace("'", "''");
				}
				if (updateItem.getKey().equals("keyword_c")) {
					subject = updateItem.getValue().trim();
					subject = subject.replace("；", ";").replaceAll(";$", "");
				}
				if (updateItem.getKey().equals("remark_c")) {
					description = updateItem.getValue().trim();
					description = description.replace("'", "''");
				}

				if (updateItem.getKey().equals("hypressorganization")) {
					creator_release = updateItem.getValue().trim();
					creator_release = creator_release.replace("'", "''");
				}
				if (updateItem.getKey().equals("Showorgan")) {
					creator_institution = updateItem.getValue().trim();
					creator_institution = creator_institution.replace("'", "''");
				}
				if (updateItem.getKey().equals("flh")) {
					subject_clc = updateItem.getValue().trim();
					subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").replace("，", ";").replace(" ", ";").trim();
				}
				if (updateItem.getKey().equals("hymeetingdate")) {
					//date_created = date_created.replace("-", "");
					date_created = updateItem.getValue().trim();
					String[] list = date_created.split("-");
					if (list.length == 3) {
						year = list[0];
						month = list[1];
						day = list[2];
					}
					else if (list.length == 2) {
						year = list[0];
						month = list[1];
						day = "00";
					}
					else if (list.length == 1) {
						year = list[0];
						if (year.equals("")) {
							year = "1900";
						}
						month = "00";
						day = "00";
					}
					else if (list.length == 0) {
						year = "1900";
						month = "00";
						day = "00";
					}
					if(month.length() == 1){
						month = "0" + month;
					}
					if(day.length() == 1){
						day = "0" + day;
					}
					date_created = year + month + day;
					if (date_created.length() > 8) {
						date_created = date_created.substring(0,8);
					}
					if (date_created.length()<4) {
						date_created = "19000000";
					}
					date = date_created.substring(0,4);
					
				}
				if (updateItem.getKey().equals("description_fund")) {
					description_fund = updateItem.getValue().trim();
					description_fund = description_fund.replace("【基金】", "");
				}
			}
			lngID = wanID2vipID(rawid);
			
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			String medium = "2";
			String batch = (new SimpleDateFormat("yyyyMMdd"))
					.format(new Date()) + "00";
			String provider = "wanfangconference";
			String provider_url = provider
					+"@http://d.wanfangdata.com.cn/Conference/" + rawid;
			String provider_id = provider + "@" + rawid;
			String sql = "INSERT INTO modify_title_info_zt([provider_id],[provider],[provider_url],[batch],[medium],[country],[description_fund],[subject_clc],[type],[language],[lngid],[rawid], [title], [date],[title_series], [creator], [creator_institution], [source], [source_institution], [subject], [description], [creator_release],[date_created],[owner]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql,provider_id,provider,provider_url,
					batch,medium,country,description_fund,subject_clc,type,
					language,lngID,rawid, title, date,title_series,creator,
					creator_institution, source,source_institution, subject,
					description,creator_release,date_created,owner);
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

				String tempPathFile = "/RawData/wanfang/hy/ref_file/wanfangconference.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator
						+ taskId + "_" + "wanfangconference.db3";
				Path src = new Path(tempPathFile); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath()
						+ File.separator + "." + taskId + "_" + "wanfangconference.db3.crc");
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
					log2HDFSForMapper(context,sql);
					logger.error("***Error: insert failed. sql:" + sql, e);
				}
			}

		}
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			
			text = text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/user/chenyong/log/log.txt";
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
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
			}
			else {
				return true;
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

				/*
				 * if (!hdfs.exists(finalHdfsPath)) {
				 * //hdfs.delete(finalHdfsPath, true);
				 * hdfs.mkdirs(finalHdfsPath); //创建输出目录 }
				 */

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