package simple.jobstream.mapreduce.site.rscbook;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

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
	
	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/CQU/rscbook/ref_file/20181218.txt";
	private static String postfixDb3 = "rscbook";
	

	
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
		
		public static Set<String> isbnSet = new HashSet<String>();
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			InitBookidSet(context);
			
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
		        String pathfile = "/vipuser/zengfanrong/log/log_map/" + nowDate + ".txt";
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
            
			for (String co: coveridString.split("★")){
				isbnSet.add(co.trim());
			}
		}
		
		

		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String title = "";
			String title_sub = "";
			String title_series = "";
			String identifier_eisbn = "";
			String identifier_pisbn = "";
			String identifier_pissn = "";
			String identifier_doi = "";
			String creator = "";
			String creator_institution = "";
			String source_fl = "";
			String publisher = "";
			String date = "";
			String date_created = "";
			String volume = "";
			String description = "";
			String subject = "";
			String subject_dsa = "";
			String page = "";
			String creator_bio = "";
			String lngid = "";
			String rawid = "";
			String cover = "";
			String language = "EN";
			String country = "US";
			String provider = "rscbook";
			String provider_url = "";
			String provider_id = "";
			String type = "1";
			String medium = "2";
			String batch = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_sub")) {
					title_sub = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("title_series")){
					title_series = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pisbn")) {
					identifier_pisbn = updateItem.getValue().trim().replace("-", "");
				}
				
				else if (updateItem.getKey().equals("identifier_eisbn")) {
					identifier_eisbn = updateItem.getValue().trim().replace("-", "");
				}

				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim().replace(" [", "[");
				}

				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				
				else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
				
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("creator_bio")) {
					creator_bio = updateItem.getValue().trim();
					
				}else if(updateItem.getKey().equals("date_created")){
					date_created = updateItem.getValue().trim();
				}
				
			}
			if(rawid.length() < 1){
				return;
			}
			
			 lngid = "RSC_TS_" + rawid;
			 title  = title.replace('\0', ' ').replace("'", "''").trim();
			 title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			 creator_bio = creator_bio.replace('\0', ' ').replace("'", "''").trim();
			 
			 creator = creator.replace('\0', ' ').replace("'", "''").trim();
			 publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			 description = description.replace('\0', ' ').replace("'", "''").trim();
			 subject = subject.replace('\0', ' ').replace("'", "''").trim();
			 subject = subject.replace('\0', ' ').replace("'", "''").trim();
			

			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
		    provider_url = provider + "@http://pubs.rsc.org/en/content/ebook/" +rawid;
			provider_id = provider + "@" + rawid;
			
			


			if (isbnSet.contains(rawid)){
				cover = "/smartlib/rscbook/" + rawid + ".jpg";
			}

	  
			String sql = "INSERT INTO modify_title_info_zt([lngid],[identifier_doi],[rawid],[title_sub],[title],[title_series],[identifier_eisbn],[identifier_pisbn],[creator],[creator_institution],[publisher],[date],[description],[subject],[type],[language],[country],[provider],[batch],[provider_url],[medium],[date],[provider_id],[cover], [volume],[date_created], [page], [creator_bio]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');";
			sql = String.format(sql, lngid,identifier_doi, rawid, title_sub, title, title_series,identifier_eisbn, identifier_pisbn, creator, creator_institution,publisher, date,  description, subject, type, language, country, provider, batch, provider_url, medium,date,provider_id,cover,volume,date_created,page,creator_bio);
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
				
				String tempPathFile = "/RawData/_rel_file/zt_template.db3";
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