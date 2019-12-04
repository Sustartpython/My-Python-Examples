package simple.jobstream.mapreduce.site.ciirSslibrary;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.Pattern;

import com.almworks.sqlite4java.SQLiteException;
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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class StdSslibrary extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdSslibrary.class);
	
	private static String postfixDb3 = "ciirsslibrarybook";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "ciirsslibrary." + this.getClass().getSimpleName();
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
		
		public void setup(Context context) throws IOException, InterruptedException {
			
		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			
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
		        String pathfile = "/venter/sslibrary/log/log_map/" + nowDate + ".txt";
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
		
		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
				
			String title = "";//标题
			String creator = "";//作者
			String publishers = "";//发行商
			//String identifier_pisbn = "";//isbn号
			String date="";//出版年份
			String subject="";//关键字
			String description = "";//内容提要
			String rawid = "";//
			String page="";
			String subject_clc="";

		    String lngID = "";
		    String batch="";
		    String provider="ciirsslibrarybook";
		    String provider_url="";
		    String provider_id="";
		    String country="CN";
		    
		    String date_created=""; //出版日期
			String language = "ZH";
			String type = "1";
			String medium = "2";
			
			//xObject 里面的数据
			String dxid = "";
			String ssid = "";
			String isFromBW = "";
			String isjgptjs="";
			String cnFenlei="";
			String bookCardD="";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("bookName")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("introduce")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publishers = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date_created = updateItem.getValue().trim().replaceAll("\\.", "");
				}
				else if(updateItem.getKey().equals("page")){
					page = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("bookCardD")){
					bookCardD = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("ssid")){
					ssid = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("isFromBW")) {
					isFromBW = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("isjgptjs")){
					isjgptjs = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("cnFenlei")) {
					cnFenlei = updateItem.getValue().trim();
				}
				try {
					if (updateItem.getKey().equals("author")){
						creator = updateItem.getValue().trim();
					}
				} catch (Exception e) {
					// TODO: handle exception
				}

			}
			subject_clc = cnFenlei.replaceAll(";", " ");
			subject_clc = StringEscapeUtils.escapeSql(subject_clc).trim();
			
			rawid = key.toString();
			dxid = rawid;
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
			provider_url = provider + "@http://www.sslibrary.com/book/card?cnFenlei="+cnFenlei+"&ssid="+ssid+"&d="+bookCardD+"&dxid="+dxid+"&isFromBW="+isFromBW+"&isjgptjs="+isjgptjs;
			provider_id = provider + "@" + rawid;
			lngID = "CIIR_SSLIBRARY_TS_" + rawid;
			if (date_created.length() >=4) {
				date = date_created.substring(0, 4);
			}else {
				date = date_created;
			}
			if (date_created.length() < 8 && date_created.length() >4){
				date_created = date_created + "00";
			}else{
				date_created = date_created + "0000";
			}
			if (Pattern.compile("\\((?:[^()]+\\s[^()]+)+\\)").matcher(subject).find()){
				subject = subject.replaceAll("\\)\\s+", ");");
			}else {
				//subject = Pattern.compile(":\\s+").matcher(subject).replaceAll(":").replaceAll(" ",";").trim();
				subject = subject.replaceAll(":\\s+", ":").replaceAll(" ",";").trim();
			}
			
			
			//转义sql字符
			title = StringEscapeUtils.escapeSql(title).trim();//标题
			creator = StringEscapeUtils.escapeSql(creator.replaceAll("；",";").replaceAll("，",";").replaceAll("; ", ";").replaceAll(", ",";").replaceAll(",","")).trim();//作者
//			identifier_pisbn = StringEscapeUtils.escapeSql(identifier_pisbn).trim();//isbn号
			description = StringEscapeUtils.escapeSql(description).trim();//内容提要
			publishers = StringEscapeUtils.escapeSql(publishers).trim();
			subject=StringEscapeUtils.escapeSql(subject);//关键字
            provider_url =StringEscapeUtils.escapeSql(provider_url);
            provider_url = provider_url.replaceAll(";", "%3B");
            
			String sql = "insert into modify_title_info_zt(lngid,rawid,title,creator,description,subject,date,date_created,language,country,provider,provider_url,provider_id,type,medium,batch,publisher,page,subject_clc)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,title,creator,description,subject,date,date_created,language,country,provider,provider_url,provider_id,type,medium,batch,publishers,page,subject_clc);					
			
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

		protected void setup(Context context) throws IOException, InterruptedException
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
                        try {
                            connSqlite.exec(sql);
                            sqlCounter.increment(1);
                        }catch (SQLiteException e){
                            context.getCounter("reduce", "insert error").increment(1);
                            logger.error("***Error: insert failed. sql:" + sql, e);
                        }
                    }
                    connSqlite.exec("COMMIT TRANSACTION;");
                    sqlList.clear();
                } catch (Exception e) {
                    logger.error("****Transaction Error****",e);
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