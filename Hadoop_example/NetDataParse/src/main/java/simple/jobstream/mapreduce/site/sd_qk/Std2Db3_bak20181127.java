package simple.jobstream.mapreduce.site.sd_qk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
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
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class Std2Db3_bak20181127 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2Db3_bak20181127.class);
	
	private static int reduceNum = 0;
	private static String postfixDb3 = "sciencedirectjournal";
	private static String db3Template = "/RawData/_rel_file/zt_template.db3";
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "sd_qk." + this.getClass().getSimpleName();
		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
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
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		private static Map<String, String> mapCountry = new HashMap<String, String>();		//国家代码表
		private static Map<String, String> mapLanguage = new HashMap<String, String>();		//语言代码表
		private static Map<String, String> mapSubject = new HashMap<String, String>();		//issn-类别代码表
		private static Map<String, String> mapSource = new HashMap<String, String>();		//issn-刊名表
		
		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);
		
		private static void initMapCountryLanguage(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/国家语言对照表.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 2) {
						continue;
					}
		        	
		        	String[] vec = line.split("★");
		        	if (vec.length != 3) {
						continue;
					}
		        	
		        	String issn = vec[0].trim();
		        	String country = vec[1].trim();
		        	String language = vec[2].trim();
		        	
		        	if (country.length() > 0) {
						mapCountry.put(issn, country);
					}
		        	
		        	if (language.length() > 0) {
						mapLanguage.put(issn, language);
					}
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }
	        
	        System.out.println("mapCountry size:" + mapCountry.size());
	        System.out.println("mapLanguage size:" + mapLanguage.size());
		}
		
		
		private static void initMapSubject(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/issn-provider_subject.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 9) {
						continue;
					}
		        	
		        	String[] vec = line.split("★");
		        	if (vec.length != 2) {
						continue;
					}
		        	
		        	String issn = vec[0].trim();
		        	String subject = vec[1].trim();
		        	
		        	if (subject.length() > 0) {
						mapSubject.put(issn, subject);
					}
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }
	        
	        System.out.println("mapSubject size:" + mapSubject.size());
		}
		
		private static void initMapSource(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/elsevier/sd_qk/ref_file/issn-source.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 9) {
						continue;
					}
		        	
		        	String[] vec = line.split("★");
		        	if (vec.length != 2) {
						continue;
					}
		        	
		        	String issn = vec[0].trim();
		        	String source = vec[1].trim();
		        	
		        	if (source.length() > 0) {
						mapSource.put(issn, source);
					}
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }
	        
	        System.out.println("mapSource size:" + mapSource.size());
		}
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			// 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        initMapCountryLanguage(fs);		//初始化issn、国家、语言字典
	        initMapSubject(fs);
	        initMapSource(fs);
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
		        String pathfile = "/vipuser/walker/log/log_map/" + nowDate + ".txt";
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
				
			String rawid = "";
			String title = "";
			String title_alternative = "";
			String creator = "";
			String creator_institution = "";
			String subject = "";
			String description = "";
			String description_en = "";
			String identifier_doi = "";
			String identifier_pissn = "";
			String volume = "";
			String issue = "";
			String source = "";			
			String date = "";
			String page = "";
			String date_created = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_alternative")) {
					title_alternative = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description_en")) {
					description_en = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
			}
			
			if ((Integer.parseInt(date) < 1000) || (Integer.parseInt(date) > curYear+1)) {
	    		context.getCounter("map", "std_err_date").increment(1);
	    		log2HDFSForMapper(context, "std_err_date:" + rawid + "," + date);
	    		return;
			}			
			
			String language = "EN";
			if (mapLanguage.containsKey(identifier_pissn)) {
				language = mapLanguage.get(identifier_pissn);
			}			
			String country = "NL";		//NL（荷兰）
			if (mapCountry.containsKey(identifier_pissn)) {
				country = mapCountry.get(identifier_pissn);
			}
			String provider_subject = "";		//原始专辑、类别
			if (mapSubject.containsKey(identifier_pissn)) {
				provider_subject = mapSubject.get(identifier_pissn);
			}
			
			if (mapSource.containsKey(identifier_pissn)) {
				source = mapSource.get(identifier_pissn);
			}
			else {
				context.getCounter("map", "no issn source").increment(1);
			}
			
			{
				title = title.replace('\0', ' ').replace("'", "''").trim();
				title_alternative = title_alternative.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				description_en = description_en.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();		
				date = date.replace('\0', ' ').replace("'", "''").trim();
				page = page.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
			}
			
			
			String lngid = "SCIENCEDIRECT_WK_" + rawid;
			String type = "3";
			String medium = "2";
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";		
			if (Integer.parseInt(date) < 2000) {
				//2天前
				batch = (new SimpleDateFormat("yyyyMMdd")).format((new Date()).getTime()-2 * 24 * 60 * 60 * 1000) + "00";
			}
			context.getCounter("map", "batch " + batch).increment(1);
			
			String publisher = "Elsevier Science";
			
			
			String provider = "sciencedirectjournal";
			String provider_url = provider + "@http://www.sciencedirect.com/science/article/pii/" + rawid;
			String provider_id = provider + "@" + rawid;
			String gch = "";
			//String cover = "";
			if (identifier_pissn.length() > 0) {
				gch = provider + "@" + identifier_pissn;
				gch = gch.toLowerCase();	 	//转为小写
			}
			else {
				context.getCounter("map", "error no issn").increment(1);
				System.err.println("**********error no issn:" + rawid);
			}
			
					   
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid], [title], [title_alternative], [creator], [creator_institution], [subject], [provider_subject],[description], [description_en], [identifier_doi], [identifier_pissn], [volume], [issue], [source], [date], [page], [date_created], [type], [medium], [batch], [publisher], [language], [country], [provider], [provider_url], [provider_id], [gch]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, title, title_alternative, creator, creator_institution, subject, provider_subject, description, description_en, identifier_doi, identifier_pissn, volume, issue, source, date, page, date_created, type, medium, batch, publisher, language, country, provider, provider_url, provider_id, gch);								
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
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
				
				//String tempPathFile = "/RawData/CQU/template/zt_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(db3Template);	//模板文件（HDFS路径）
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