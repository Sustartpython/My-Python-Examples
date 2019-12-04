package simple.jobstream.mapreduce.site.emeraldjournal;

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

import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdEmerald extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdEmerald.class);
	
	private static String postfixDb3 = "emeraldjournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "emeraldjournal." + this.getClass().getSimpleName();
		if (testRun) {
			jobName = "test_" + jobName;
		}
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		job.setJobName(jobName);
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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {

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
		        String pathfile = "/lqx/log/log_map/" + nowDate + ".txt";
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
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String url = "";
				
			String title = "";//标题
//			String title_alternative="";
			String creator = "";//作者
			String publisher = "";//发行商
			String identifier_pisbn = "";//isbn号
			String date="";//出版日期
			String creator_institution="";//机构
			String description = "";//内容提要
			String provider_subject = "";
			String rawid = "";//
			String subject = "";
			String title_series = "";
			String subject_clc = "";
			String page = "";
			
			
            String source = "";
            String identifier_pissn = "";
            String identifier_eissn = "";

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider="emeraldjournal";
		    String provider_url="";
		    String provider_id="";
		    String country="UK";
		    String cover="";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "EN";
			String type = "3";
			String medium = "2";
			
			String beginpage = "";
			String endpage = "";
			String volume = "";
			String issue = "";
			String identifier_doi = "";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("beginpage")) {
					beginpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("endpage")) {
					endpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("insitution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("gch")) {
					gch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_subject")) {
					provider_subject = updateItem.getValue().trim();
				}
			}
			rawid = key.toString();


			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
			
			url = "https://www.emeraldinsight.com" + url;
			provider_url = provider + "@" + url;
			provider_id = provider + "@" + rawid;
			gch = provider + "@" + gch;
//			lngID = "EMERALD_WK_" + rawid;
			lngID = VipIdEncode.getLngid("00094",rawid,false);
			if (date.equals("") && !date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			if (date.equals("") && date_created.equals("")) {
				date = "1900";
				date_created = "19000000";
			}
			
			if (!beginpage.equals("")) {
				if (endpage.equals("")) {
					endpage = beginpage;
				}
				page = beginpage + "-" + endpage;
			}
			
			

			//date_created = date + "0000";
			
			//转义sql字符
//			title = StringEscapeUtils.escapeSql(title.replaceAll("《","").replaceAll("》","")).trim();//标题
//			title_alternative = StringEscapeUtils.escapeSql(title_alternative.replaceAll("《","").replaceAll("》","")).trim();//标题
//			creator = StringEscapeUtils.escapeSql(creator.replaceAll("；",";").replaceAll("，",";").replaceAll("; ", ";").replaceAll(", ",";").replaceAll(",","")).trim();//作者
			//identifier_pisbn = StringEscapeUtils.escapeSql(identifier_pisbn).trim();//isbn号

//			subject=StringEscapeUtils.escapeSql(subject);//关键字
//			description = StringEscapeUtils.escapeSql(description).trim();//内容提要
//			provider_subject = StringEscapeUtils.escapeSql(provider_subject).trim();
			//publishers = StringEscapeUtils.escapeSql(publishers).trim();
			
			

			
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			
			
					   
			String sql = "insert into modify_title_info_zt(lngid,rawid,identifier_doi,gch,title,creator,source,identifier_pissn,provider_subject,beginpage,endpage,description,date,date_created,publisher,page,creator_institution,subject,volume,issue,language,country,provider,provider_url,provider_id,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,identifier_doi,gch,title,creator,source,identifier_pissn,provider_subject,beginpage,endpage,description,date,date_created,publisher,page,creator_institution,subject,volume,issue,language,country,provider,provider_url,provider_id,type,medium,batch);					
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

	public static class ProcessReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
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
				logger.info("***** baseDir:" + baseDir);	
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
				logger.info("***** finalHdfsPath:" + finalHdfsPath);	
				
				
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