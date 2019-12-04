package simple.jobstream.mapreduce.site.wanfang_hy;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;



import org.json.JSONArray;
import org.json.JSONObject;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class stdXXXXobjForZLF extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(stdXXXXobjForZLF.class);

	private static boolean testRun = true;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "Stdhy";
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
		private static HashMap<String, String> FirstClassMap      = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap     = new HashMap<>();		
		private static ClassType classtype = null;
		
		public void setup(Context context) throws IOException,InterruptedException 
		{
			String firstclass_info  = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);
			
			FirstClassMap      = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap     = classload.getsecondclass();
			
			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);
		}
		
		
		
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
		//生成NetFullTextAddr_all
		public static JSONObject TextAddr_all(String addr){
			String [] attr = {addr};
			String key = "WANFANG@WANFANGDATA";
			JSONObject o = null;
			try {
				HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
				JSONArray array = new JSONArray(attr);
				map.put(key, array);
				o = new JSONObject(map);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return o;
		}
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String rawid = "";//id
			String title_c = "";//主题
			String years="";//会议年份
			String hymeetingrecordname="";//母体文献
			String Showwriter = "";//作者
			String Showorgan = "";//机构
			String media_c = "";//会议名称
			String hymeetingplace = "";//会议地点
			String keyword_c = "";//关键词
			String remark_c = "";//摘要
			String hyhostorganization = "";//主办单位
			String language = "1";//语言
			String type = "3";
			String titletype = "0;1;768;769";
			String NetFullTextAddr = "";
			JSONObject NetFullTextAddr_all;
			JSONObject NetFullTextAddr_all_std;
			String srcID = "VIP";
			String lngID = "";
			String hyh = "";
			String hymeetingdate = "";
			String flh = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title_c = updateItem.getValue().trim();
					title_c = title_c.replace("'", "''");
				}
				else if(updateItem.getKey().equals("pub_year")) {
					years = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("meeting_record_name")) {
					hymeetingrecordname = updateItem.getValue().trim();
					hymeetingrecordname = hymeetingrecordname.replace("'", "''");
				}

				else if(updateItem.getKey().equals("author")) {
					Showwriter = updateItem.getValue().trim();
					Showwriter = Showwriter.replace("'", "''");
				}

				else if(updateItem.getKey().equals("meeting_name")) {
					media_c = updateItem.getValue().trim();
					media_c = media_c.replace("'", "''");
				}
				else if(updateItem.getKey().equals("meeting_place")) {
					hymeetingplace = updateItem.getValue().trim();
					hymeetingplace = hymeetingplace.replace("'", "''");
				}
				else if(updateItem.getKey().equals("keyword")) {
					keyword_c = updateItem.getValue().trim();
					keyword_c = keyword_c.replace("'", "''");
				}
				else if(updateItem.getKey().equals("abstract")) {
					remark_c = updateItem.getValue().trim();
					remark_c = remark_c.replace("'", "''");
				}

				else if(updateItem.getKey().equals("host_organ")) {
					hyhostorganization = updateItem.getValue().trim();
					hyhostorganization = hyhostorganization.replace("'", "''");
				}
				else if(updateItem.getKey().equals("organ")) {
					Showorgan = updateItem.getValue().trim();
					Showorgan = Showorgan.replace("'", "''");
				}
				else if(updateItem.getKey().equals("hyh")) {
					hyh = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("pub_date")) {
					hymeetingdate = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("clc_no")) {
					flh = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("lngid")) {
					lngID = updateItem.getValue().trim();
				}
			}
			if (flh.length() <1) {
				context.getCounter("map", "not find class").increment(1);
			}
			else {
				context.getCounter("map", "find class").increment(1);
			}
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			String T_classtypes  = classtype.GetClassTypes(flh);
			String T_showclasstypes = classtype.GetShowClassTypes(flh);
//			lngID = wanID2vipID(rawid);
			NetFullTextAddr = "http://d.wanfangdata.com.cn/Conference/" + rawid;
			NetFullTextAddr_all = TextAddr_all(NetFullTextAddr);
			NetFullTextAddr_all_std = TextAddr_all(rawid);
			if (T_classtypes.length() <1) {
				context.getCounter("map", "not find classtypes").increment(1);
			}
			else {
				context.getCounter("map", "find classtypes").increment(1);
			}
			if (T_showclasstypes.length() <1) {
				context.getCounter("map", "not find showclasstypes").increment(1);
			}
			else {
				context.getCounter("map", "find showclasstypes").increment(1);
			}
			String sql = "INSERT INTO modify_title_info([classtypes],[showclasstypes],[srcID],[titletype],[type],[language],[NetFullTextAddr_all_std],[NetFullTextAddr_all],[NetFullTextAddr],[lngID],[rawid], [title_c], [years],  [hymeetingrecordname], [Showwriter], [Showorgan], [media_c], [hymeetingplace], [keyword_c], [remark_c], [hyhostorganization],[hymeetingdate],[class]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s', '%s',  '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s','%s');";
			sql = String.format(sql,T_classtypes,T_showclasstypes,srcID,titletype,type, language,NetFullTextAddr_all_std,NetFullTextAddr_all,NetFullTextAddr,lngID,rawid, title_c, years,hymeetingrecordname,Showwriter,Showorgan, media_c,hymeetingplace, keyword_c,remark_c,hyhostorganization,hymeetingdate,flh);
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

				String tempPathFile = "/RawData/wanfang/hy/ref_file/wanfang_hy_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator
						+ taskId + "_" + "wanfang_hy.db3";
				Path src = new Path(tempPathFile); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath()
						+ File.separator + "." + taskId + "_" + "wanfang_hy.db3.crc");
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