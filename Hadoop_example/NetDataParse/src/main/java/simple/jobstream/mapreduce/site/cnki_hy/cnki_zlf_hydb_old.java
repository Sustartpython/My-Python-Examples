package simple.jobstream.mapreduce.site.cnki_hy;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
public class cnki_zlf_hydb_old extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(cnki_hydb.class);

	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/cnki/hy/ref_file/lngid_20190614.txt";

	public void pre(Job job) {
		String jobName = "cnki_hy_zlf";
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
			InitBookidSet(context);
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

	
		//生成NetFullTextAddr_all
		public static JSONObject TextAddr_all(String addr){
			String [] attr = {addr};
			String key = "CNKI@CNKIDATA";
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
		
		private static String getLngIDByCnkiID(String cnkiID) {
			   cnkiID = cnkiID.toUpperCase();
			   String lngID = "";
			   for (int i = 0; i < cnkiID.length(); i++) {
			    lngID += String.format("%d", cnkiID.charAt(i) + 0);
			   }
			   
			   return lngID;
			  }
		public static Set<String> isbnSet = new HashSet<String>();
		public static void InitBookidSet(Context context) throws IOException{
	        FileSystem fs = FileSystem.get(context.getConfiguration());

	        Path path = new Path(ref_file_path);
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);
            
            String coveridString = new String(buffer);
            
			for (String co: coveridString.split("@")){
				isbnSet.add(co.trim());
			}
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
			String remark_c_fund = "";//基金
			String language = "1";//语言
			String type = "3";
			String country = "CN";
			String titletype = "0;1;768;769";
			String NetFullTextAddr = "";
			JSONObject NetFullTextAddr_all;
			JSONObject NetFullTextAddr_all_std;
			String srcID = "VIP";
			String lngID = "";
			String keyword_c_clc = ""; //分类号
			String hymeetingdate = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
					title_c = title_c.replace("'", "''");
				}
				if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("hymeetingrecordname")) {
					hymeetingrecordname = updateItem.getValue().trim();
					hymeetingrecordname = hymeetingrecordname.replace("'", "''");
				}

				if (updateItem.getKey().equals("Showwriter")) {
					Showwriter = updateItem.getValue().trim();
					Showwriter = Showwriter.replace("'", "''");
				}

				if (updateItem.getKey().equals("media_c")) {
					media_c = updateItem.getValue().trim();
					media_c = media_c.replace("'", "''");
				}
				if (updateItem.getKey().equals("hymeetingplace")) {
					hymeetingplace = updateItem.getValue().trim();
					hymeetingplace = hymeetingplace.replace("'", "''");
				}
				if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
					keyword_c = keyword_c.replace("；", ";").replaceAll(";$", "");
				}
				if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
					remark_c = remark_c.replace("'", "''");
				}

				if (updateItem.getKey().equals("hypressorganization")) {
					hyhostorganization = updateItem.getValue().trim();
					hyhostorganization = hyhostorganization.replace("'", "''");
				}
				if (updateItem.getKey().equals("Showorgan")) {
					Showorgan = updateItem.getValue().trim();
					Showorgan = Showorgan.replace("'", "''").replace("； ", ";");
				}
				if (updateItem.getKey().equals("flh")) {
					keyword_c_clc = updateItem.getValue().trim();
					keyword_c_clc = keyword_c_clc.replace(";", " ");
				}
				if (updateItem.getKey().equals("hymeetingdate")) {
					hymeetingdate = updateItem.getValue().trim();
				}
				
				if (updateItem.getKey().equals("remark_c_fund")) {
					remark_c_fund = updateItem.getValue().trim();
					remark_c_fund = remark_c_fund.replace("【基金】", "");
				}
			}
			
			
			lngID = "C_HY_"+getLngIDByCnkiID(rawid);
			String T_classtypes  = classtype.GetClassTypes(keyword_c_clc);
			String T_showclasstypes = classtype.GetShowClassTypes(keyword_c_clc);
			
			NetFullTextAddr = "http://www.cnki.net/kcms/detail/detail.aspx?dbcode=CIPD&filename=" + rawid;
			NetFullTextAddr_all = TextAddr_all(NetFullTextAddr);
			NetFullTextAddr_all_std = TextAddr_all(rawid);
			
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			if (title_c.trim().length() < 1) {
				context.getCounter("map", "null title_c").increment(1);
				return;
			}
			if (!isbnSet.contains(lngID)){
				context.getCounter("map", "null lngID").increment(1);
				return;
			}
			String medium = "";
			String batch = (new SimpleDateFormat("yyyyMMdd"))
					.format(new Date()) + "00";
			String provider = "cnkiconference";
			String provider_url = provider
					+"@http://kns.cnki.net/kcms/detail/detail.aspx?dbcode=CIPD&filename=" + rawid;
			String provider_id = provider + "@" + rawid;
			String sql = "INSERT INTO modify_title_info([classtypes],[showclasstypes],[srcID],[titletype],[type],[language],[NetFullTextAddr_all_std],[NetFullTextAddr_all],[NetFullTextAddr],[lngID],[rawid], [title_c], [years],  [hymeetingrecordname], [Showwriter], [Showorgan], [media_c], [hymeetingplace], [keyword_c], [remark_c], [hyhostorganization],[hymeetingdate],[class]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s', '%s', '%s',  '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s','%s');";
			sql = String.format(sql,T_classtypes,T_showclasstypes,srcID,titletype,type, language,NetFullTextAddr_all_std,NetFullTextAddr_all,NetFullTextAddr,lngID,rawid, title_c, years,hymeetingrecordname,Showwriter,Showorgan, media_c,hymeetingplace, keyword_c,remark_c,hyhostorganization,hymeetingdate,keyword_c_clc);
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
						+ taskId + "_" + "cnkiconference_zlf.db3";
				Path src = new Path(tempPathFile); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath()
						+ File.separator + "." + taskId + "_" + "cnkiconference_zlf.db3.crc");
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
		        String pathfile = "/vipuser/chenyong/log/log.txt";
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