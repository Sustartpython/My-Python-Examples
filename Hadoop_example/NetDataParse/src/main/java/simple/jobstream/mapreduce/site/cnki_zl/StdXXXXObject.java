package simple.jobstream.mapreduce.site.cnki_zl;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
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

import simple.jobstream.mapreduce.common.util.StringHelper;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class StdXXXXObject extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdXXXXObject.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 100;
	private static int reduceNum = 100;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "cnkipatent";
	

	
	public void pre(Job job) {
		String jobName = "StdXXXXObject";
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		
	
		public static String encodeID(String strRaw) {
			Base32 base32 = new Base32();
			String strEncode = "";
			try {
				strEncode = base32.encodeAsString(strRaw.getBytes("utf8"));
				if (strEncode.endsWith("======")) {
					strEncode = strEncode.substring(0, strEncode.length() - 6) + "0";
				} else if (strEncode.endsWith("====")) {
					strEncode = strEncode.substring(0, strEncode.length() - 4) + "1";
				} else if (strEncode.endsWith("===")) {
					strEncode = strEncode.substring(0, strEncode.length() - 3) + "8";
				} else if (strEncode.endsWith("=")) {
					strEncode = strEncode.substring(0, strEncode.length() - 1) + "9";
				}
				strEncode = StringHelper.makeTrans("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
						"ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210", strEncode);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return strEncode;
		}
		public static String getLngid(String sub_db_id, String rawid, boolean case_insensitive) {
			String uppercase_rawid = ""; // 大写版 rawid
			if (case_insensitive) { // 源网站的 rawid 区分大小写
				char[] rawlist = rawid.toCharArray();
				for (char ch : rawlist) {
					if (Character.toUpperCase(ch) == ch) {
						uppercase_rawid += ch;
					} else {
						uppercase_rawid += Character.toUpperCase(ch) + "_";
					}
				}
			} else {
				uppercase_rawid = rawid.toUpperCase();
			}
			String limited_id = uppercase_rawid;
			if (limited_id.length() > 20) {
				limited_id = DigestUtils.md5Hex(uppercase_rawid).toUpperCase();
			} else {
				limited_id = encodeID(uppercase_rawid);
			}
			String lngid = sub_db_id + limited_id;
			return lngid;
		}
	
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String rawid = "";
			String lngid = "";
			String title = "";
			String identifier_issn = "";//申请号
			String date_created = ""; //申请日
			String identifier_standard = "";//公开号
			String date_impl = "";//公开日
			String applicant = "";//申请人
			String creator_institution = "";//申请人地址
			String creator = "";//发明人
			String agency = "";//代理机构
			String agents = "";//代理人
			String province_code = "";//国省代码
			String description = "";//摘要
			String subject_csc = "";//主分类号
			String subject_isc = "";//专利分类号
			String language = "ZH";
			String country = "CN";
			String provider = "cnkipatent";
			String provider_url = "";
			String provider_id = "";
			String type = "7";
			String medium = "2";
			String batch = "";
			String date = "";
			String owner = "";
			String page = "";
			String description_core = "";
			String legal_status = "";
			String description_type = "";
			String sub_db_id = "00003";
			

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {title = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("identifier_issn")) {identifier_issn = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("date_created")) {date_impl = updateItem.getValue().trim().replace("-", "");}
				if (updateItem.getKey().equals("identifier_standard")) {identifier_standard = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("date_impl")) {date_created = updateItem.getValue().trim().replace("-", "");}
				if (updateItem.getKey().equals("creator_cluster")) {applicant = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("creator_institution")) {creator_institution = updateItem.getValue().trim().replace(" ","");}
				if (updateItem.getKey().equals("creator")) {creator = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("agency")) {agency = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("agents")) {agents = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("province_code")) {province_code = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("description")) {description = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("subject_csc")) {subject_csc = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("subject_isc")) {subject_isc = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("page")) {page = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("description_core")){description_core = updateItem.getValue().trim();}
				if (updateItem.getKey().equals("db")){description_type = updateItem.getValue().trim();}
				
				if (updateItem.getKey().equals("legalState")){legal_status = updateItem.getValue().trim();}
			
			}
			
			if(legal_status.length() < 1){
				context.getCounter("map", "Not legal_status").increment(1);
				return;
			}
			if(legal_status.equals("not")){
				legal_status = "";
			}
			if(page.contains("'")){
				page = "";
			}
			

			
			
			 rawid = identifier_standard;
			 lngid = getLngid( sub_db_id,  rawid, false);
			 title  = title.replace('\0', ' ').replace("'", "''").trim();
			 creator = creator.replace('\0', ' ').replace("'", "''").trim();
			 creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			 applicant =applicant.replace('\0', ' ').replace("'", "''").trim();
			 description = description.replace('\0', ' ').replace("'", "''").trim();
			 subject_csc = subject_csc.replace('\0', ' ').replace("'", "''").trim();
			 subject_isc = subject_isc.replace('\0', ' ').replace("'", "''").trim();
			 agency = agency.replace('\0', ' ').replace("'", "''").trim();
			 agents = agents.replace('\0', ' ').replace("'", "''").trim();
			 date = date_created.substring(0,4);
			 description_core = description_core.replace('\0', ' ').replace("'", "''").trim();
			 legal_status = legal_status.replace('\0', ' ').replace("'","''").trim();
			 if(description_type.equals("SCPD_WG")){
				 description_type = "外观设计";
			 }else if(description_type.equals("SCPD_XX")){
				 description_type = "实用新型";
			 }else if(description_type.equals("SCPD_FM")){
				 description_type = "发明专利";
			 }
	

			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
		    provider_url = provider + "@http://dbpub.cnki.net/grid2008/dbpub/detail.aspx?dbname=SCPD&filename=" + rawid;
			provider_id = provider + "@" + rawid;

	  
			String sql = "INSERT INTO modify_title_info_zt([rawid],[lngid],[title],[identifier_pissn],[date_created],[identifier_standard],[date_impl],[applicant],[creator_institution],[creator],[agency],[agents],[province_code],[description],[subject_csc],[subject_isc],[language],[country],[provider],[provider_url],[provider_id],[type],[medium],[batch],[date],[page],[description_core],[legal_status],[description_type]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s', '%s','%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, rawid,lngid,title,identifier_issn,date_created,identifier_standard,date_impl,applicant,creator_institution,creator,agency,agents,province_code,description,subject_csc,subject_isc,language,country,provider,provider_url,provider_id,type,medium,batch,date,page,description_core,legal_status,description_type);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(sql), NullWritable.get());
			
		}
	}

	public static class ProcessReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {
		private FileSystem hdfs = null;
		private String tempDir = null;

		private SQLiteConnection connSqlite = null;
		private List<String> sqlList = new ArrayList<String>();
		
		private Counter sqlCounter = null;
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/user/chenyong/log/log_map/" + nowDate + ".txt";
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
		protected void setup(Context context)
				throws IOException, InterruptedException
		{
			try {
				System.setProperty("sqlite4java.library.path", "/usr/lib64/");

				//创建存放db3文件的本地临时目录
				String taskId = context.getConfiguration().get("mapred.task.id");
				String JobDir = context.getConfiguration().get("job.local.dir");
				tempDir = JobDir + File.separator + taskId;
				File baseDir = new File(tempDir);
				if (!baseDir.exists())
				{
					baseDir.mkdirs();
				}
					
				//
				hdfs = FileSystem.get(context.getConfiguration());
				sqlCounter = context.getCounter("reduce", "sqlCounter");
				
				String tempPathFile = "/RawData/cnki/zl/ref_file/zt_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(tempPathFile);	//模板文件（HDFS路径）
				Path dst = new Path(db3PathFile);	//local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath() + File.separator + "." +  taskId + "_" + postfixDb3 + ".db3.crc");
				if (crcFile.exists()) {
					if(crcFile.delete()) {	//删除crc文件
						logger.info("***** delete success:" + crcFile.toString());
					}
					else {
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
		
		public void insertSql(Context context)
		{
			String sql = "";
			if (sqlList.size() > 0) {
				try {						
					connSqlite.exec("BEGIN TRANSACTION;");
					for (int i = 0; i < sqlList.size(); ++i)
					{
						sql = sqlList.get(i);
						connSqlite.exec(sql);
						sqlCounter.increment(1);
					}
					connSqlite.exec("COMMIT TRANSACTION;");
					
					sqlList.clear();
								
				} catch (Exception e) {
					context.getCounter("reduce", "insert error").increment(1);
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
					InterruptedException
		{			
			logger.info("****************** Enter cleanup ******************");
			insertSql(context); 		//处理余数
			if (connSqlite != null && connSqlite.isOpen()) {
				connSqlite.dispose(); 		//关闭sqlite连接
			}
			
			try
			{
				File localDir = new File(tempDir);
				if (!localDir.exists())
				{
					throw new FileNotFoundException(tempDir + " is not found.");
				}

				//再次获取，这里并不能感知到pre获取的参数
				outputHdfsPath = context.getConfiguration().get("outputHdfsPath");
				//最终存放db3的hdf目录。嵌在了MR的输出目录，便于自动清空。
				Path finalHdfsPath = new Path(outputHdfsPath + File.separator + "/db3/");	 	

				/*
				if (!hdfs.exists(finalHdfsPath))
				{
					//hdfs.delete(finalHdfsPath, true);	
					hdfs.mkdirs(finalHdfsPath);		//创建输出目录
				}
				*/
				
				File[] files = localDir.listFiles();
				for (File file : files)
				{
					if (file.getName().endsWith(".db3"))
					{
						Path srcPath = new Path(file.getAbsolutePath());
						Path dstPash = new Path(finalHdfsPath.toString() + "/" + file.getName());						
						hdfs.moveFromLocalFile(srcPath, dstPash); 	//移动文件
						//hdfs.copyFromLocalFile(true, true, srcPath, dstPash);	//删除local文件，并覆盖hdfs文件
						logger.info("copy " + srcPath.toString() + " to "
								+ dstPash.toString());
					}
				}
			}
			catch (Exception e)
			{
				logger.error("****************** upload file failed. ******************", e);
			}
		}
		
	}
}