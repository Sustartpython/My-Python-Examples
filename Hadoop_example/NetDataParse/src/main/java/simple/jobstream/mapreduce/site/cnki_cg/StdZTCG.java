package simple.jobstream.mapreduce.site.cnki_cg;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.almworks.sqlite4java.SQLiteConnection;
import com.google.gson.Gson;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class StdZTCG extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZTCG.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "cnkicstad";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
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
		private static Set<String> setFilename = new HashSet<String>();		//获取-9状态的filename
		public void setup(Context context) throws IOException,
				InterruptedException {
			// 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        initSetFilename(fs);		
		}
		
		private static void initSetFilename(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/bz/ref_file/filename.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 2) {
						continue;
					}
		        	setFilename.add(line);        	
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }	        
	        System.out.println("setFilename size:" + setFilename.size());
	
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
	
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
		        String pathfile = "/tangmao/log/log_map/" + nowDate + ".txt";
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
	
		public static String getJsonStrByKV(String Key, List<String> Value) {
			Gson gson = new Gson();
			HashMap<String, List<String>> tmpMap = new HashMap<String, List<String>>();  
			tmpMap.put(Key,Value);
			return gson.toJson(tmpMap);
		}
		
		public static String GetLngId(String Id) {
			String lngId = "";
			for (int i = 0; i < Id.length(); i++) {
				char ch = Id.charAt(i);
				int ascCh = ch;
				if (ascCh > 47 && ascCh < 58) {
					lngId += ch;
				} else {
					lngId += ascCh;
				}
				
			}
			BigInteger bigLngId = new BigInteger(lngId);
			bigLngId = bigLngId.multiply(new BigInteger("2"));
			bigLngId = bigLngId.add(new BigInteger("5"));
			lngId = "C_BZ_" + bigLngId.toString();
		
			return lngId;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
			String rawid = "";
			String title = "";
			String description = "";
			String subject = "";
			//------------------------------cg----------------------------------------------
			String creator = "";
			String creator_institution = "";
			String subject_clc = "";
			String subject_esc = "";
			String description_type = "";
			String cglevel = "";
			String date = "";
			String cgevaluate = "";
			String cgdate = "";
			
			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_clc")) {
					subject_clc = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_esc")) {
					subject_esc = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description_type")) {
					description_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cglevel")) {
					cglevel = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cgevaluate")) {
					cgevaluate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cgdate")) {
					cgdate = updateItem.getValue().trim();
				}
			}
			
			
			//****************************处理特殊字段*************************
			if (date.isEmpty()) {
				date = "1900";
			}
			
			String date_created = date + "0000";
			
			
			String provider = "cnkicstad";
			String provider_url = provider + "@" + "http://dbpub.cnki.net/grid2008/dbpub/detail.aspx?dbname="
					+ rawid.toUpperCase().substring(0, 4) + "&filename=" + rawid.toUpperCase();
			String provider_id = provider + "@"  + rawid;
			String lngid = "CNKI_SNAD_CG_" + rawid;
			String country = "CN";
			String language = "ZH";
			
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyyMMdd");
			String batch = formatter.format(new Date());
			batch += "00";
			String type = "9";
			String medium = "2";
			
			//转义
			{	
				title = title.replace('\0', ' ').replace("'", "''").trim();	
				description = description.replace('\0', ' ').replace("'", "''").trim();	
				subject = subject.replace('\0', ' ').replace("'", "''").trim();	
				creator = creator.replace('\0', ' ').replace("'", "''").trim();	
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();	
				subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();	
				subject_esc = subject_esc.replace('\0', ' ').replace("'", "''").trim();	
				description_type = description_type.replace('\0', ' ').replace("'", "''").trim();
				cglevel = cglevel.replace('\0', ' ').replace("'", "''").trim();
				cgevaluate = cgevaluate.replace('\0', ' ').replace("'", "''").trim();
			}
			
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid], [title], [creator], [date], [subject], [creator_institution], [subject_clc], [subject_esc], [description_type],  "
					+ "[language], [country], [type], [provider], [provider_url], [provider_id], [batch], [medium], [description], [date_created])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, title, creator, date, subject, creator_institution, subject_clc, subject_esc, description_type, language, country, type, provider, provider_url, 
					provider_id, batch, medium, description, date_created);
			
//			String sql = "INSERT INTO modify_title_info_zt([rawid])";
//			sql += " VALUES ('%s');";
//			sql = String.format(sql, rawid);
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
				
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(tempFileDb3);	//模板文件（HDFS路径）
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