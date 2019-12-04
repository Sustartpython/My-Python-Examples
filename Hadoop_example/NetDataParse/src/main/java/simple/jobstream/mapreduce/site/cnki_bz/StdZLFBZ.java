package simple.jobstream.mapreduce.site.cnki_bz;

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
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.DocFlavor.STRING;

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
import org.apache.hadoop.mapreduce.Mapper.Context;
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

import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdZLFBZ extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZLFBZ.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "cnki_bz";
	private static String tempFileDb3 = "/RawData/wanfang/bz/template/wanfang_bz_template.db3";
	
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
		
		private static HashSet<String> pubNoSet =  new HashSet<String>();
		
		private static void initPubNoSet(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/bz/txt/20181118/delate.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					String pub_no = line.trim();
					pubNoSet.add(pub_no);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("pubNoSet size:" + pubNoSet.size());
		}
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			// 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        initSetFilename(fs);	
			initPubNoSet(context);
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
			String title_c = "";
			String title_e = "";
			String remark_c = "";
			String remark_e = "";
			String years = "";
			String sClass = "";
			String keyword_c = "";
			String keyword_e = "";
			String bzmaintype = "";
			String media_c = "";
			String bznum2 = "";
			String bzpubdate = "";
			String bzimpdate = "";
			String bzstatus = "";
			String bzcountry = "";
			String bzissued = "";
			String showorgan = "";
			String bzintclassnum = "";
			String bzpagenum = "";
			String bzsubsbz = "";
			String bzreplacedbz = "";
			String netfulltextaddr = "";
			String bzcnclassnum = "";
			String bzrefbz = "";
			String showwriter = "";
			String bzcommittee = "";
			String language = "";
			String titletype = "";
			String filename = "";
			
			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("drafter")) {
					showwriter = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("committee")) {
					bzcommittee = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzrefbz")) {
					bzrefbz = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzsubsbz")) {
					bzsubsbz = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzreplacedbz")) {
					bzreplacedbz = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzcnclassnum")) {
					bzcnclassnum = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("netfulltextaddr")) {
					netfulltextaddr = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzmaintype")) {
					bzmaintype = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzpagenum")) {
					bzpagenum = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzintclassnum")) {
					bzintclassnum = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("media_c")) {
					media_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzissued")) {
					bzissued = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzimpdate")) {
					bzimpdate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzstatus")) {
					bzstatus = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzcountry")) {
					bzcountry = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bzpubdate")) {
					bzpubdate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bznum2")) {
					bznum2 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_e")) {
					remark_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("sClass")) {
					sClass = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_e")) {
					keyword_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				
			}
			
//			if (setFilename.contains(rawid)) {
//				context.getCounter("map", "-9 filename").increment(1);
//				return;
//			}
			
			//转义
			{	
				bzcommittee = bzcommittee.replace('\0', ' ').replace("'", "''").trim();
				showwriter = showwriter.replace('\0', ' ').replace("'", "''").trim();
				bzrefbz = bzrefbz.replace('\0', ' ').replace("'", "''").trim();
				bzreplacedbz = bzreplacedbz.replace('\0', ' ').replace("'", "''").trim();
				netfulltextaddr = netfulltextaddr.replace('\0', ' ').replace("'", "''").trim();
				bzsubsbz = bzsubsbz.replace('\0', ' ').replace("'", "''").trim();
				bzpagenum = bzpagenum.replace('\0', ' ').replace("'", "''").trim();	
				bzissued = bzissued.replace('\0', ' ').replace("'", "''").trim();	
				bzcountry = bzcountry.replace('\0', ' ').replace("'", "''").trim();	
				bzstatus  = bzstatus.replace('\0', ' ').replace("'", "''").trim();	
				bzimpdate = bzimpdate.replace('\0', ' ').replace("'", "''").trim();	
				bzpubdate = bzpubdate.replace('\0', ' ').replace("'", "''").trim();	
				bznum2 = bznum2.replace('\0', ' ').replace("'", "''").trim();	
				media_c = media_c.replace('\0', ' ').replace("'", "''").trim();	
				bzmaintype = bzmaintype.replace('\0', ' ').replace("'", "''").trim();	
				title_c = title_c.replace('\0', ' ').replace("'", "''").trim();	
				title_e = title_e.replace('\0', ' ').replace("'", "''").trim();				
				showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();			
				remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();	
				remark_e = remark_e.replace('\0', ' ').replace("'", "''").trim();	
				keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();	
				keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").trim();	
				years = years.replace('\0', ' ').replace("'", "''").trim();
				sClass = sClass.replace('\0', ' ').replace("'", "''").trim().replace(";", " ");						
				bzcnclassnum = bzcnclassnum.replace('\0', ' ').replace("'", "''").trim().replace(";", " ");
				bzintclassnum = bzintclassnum.replace('\0', ' ').replace("'", "''").trim().replace(";", " ");	
				
			}
			
			
			//****************************处理特殊字段*************************
			
			
			if (rawid.toUpperCase().contains("SOSD")) {
				language = "2";
			} else {
				language = "1";
			}
			
			if (language.equals("1")) {
				titletype = "0;1;1280;1281";
			} else {
				titletype = "0;2;1280;1282";
			}
			
			Pattern p1 = Pattern.compile("-(?:18|19|20)\\d{2}$");
			//media_c = media_c.replaceAll("-((?:18|19|20)\\d{2})$", "\\1");
			Pattern p2 = Pattern.compile("^(?:18|19|20)\\d{2}");
			Pattern p3 = Pattern.compile("(?:18|19|20)\\d{2}$");
			if (p1.matcher(media_c).find()) {
				years = media_c.substring(media_c.length()-4, media_c.length());
			} else if (p2.matcher(bzpubdate).find()) {
				years = bzpubdate.substring(0, 4);
			} else if (p3.matcher(bznum2).find()) {
				years = bznum2.substring(bznum2.length()-4, bznum2.length());
			} 
			
			if (rawid.trim().length() < 4) {
				context.getCounter("map", "rawid null").increment(1);
				return;
			}
			netfulltextaddr = "http://dbpub.cnki.net/grid2008/dbpub/detail.aspx?dbname="
					+ rawid.toUpperCase().substring(0, 4) + "&filename=" + rawid.toUpperCase();
			
			
			
//			String lngid = GetLngId(rawid.toUpperCase());
			String lngid = "";
			String dbname = rawid.toUpperCase().substring(0, 4);
			if (dbname.equals("SCSD")) {
				String sub_db_id = "00083";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else if (dbname.equals("SOSD")) {
				String sub_db_id = "00084";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else if (dbname.equals("SCSF")) {
				String sub_db_id = "00085";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else if (dbname.equals("SCHF")) {
				String sub_db_id = "00086";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else if (dbname.equals("SMSD")) {
				String sub_db_id = "00087";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else if (dbname.equals("CISD")) {
				String sub_db_id = "00088";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			}else {
				context.getCounter("map", "err dbname").increment(1);
				return ;
			}
			String netfulltextaddr_all = "";
			String netfulltextaddr_all_std = "";
			
			String Key = "TONGFANG@CNKI";
			List<String> Value = new ArrayList<String>();	
		
			Value.add(netfulltextaddr);
			netfulltextaddr_all = getJsonStrByKV(Key, Value);
			Value.clear();
				
			Value.add(rawid);
			netfulltextaddr_all_std = getJsonStrByKV(Key, Value); 
			Value.clear();

			//{"TONGFANG@CNKI":["http://www.bzfxw.com/soft/softdown.asp?softid=149584","http://dbpub.cnki.net/grid2008/dbpub/Detail.aspx?DBName=SCSD&FileName=SCSD000006005816"]}
			//String netfulltextaddr_all = "{\"WANFANG@WANFANGDATA\":[\"" + netfulltextaddr + "\"]}";
			//{"TONGFANG@CNKI":["SCSD000006005816"]}
			
			String sql = "INSERT INTO modify_title_info([lngid], [title_c], [title_e], [bzmaintype], [years], [media_c], [bznum2], [bzpubdate], "
					+ "[bzimpdate], [bzstatus], [bzcountry], [bzissued], [showorgan], [class], [keyword_c], [keyword_e], [bzintclassnum], "
					+ "[remark_c], [bzpagenum], [bzsubsbz], [netfulltextaddr], [rawid], [type], [language], [titletype], [srcid], [netfulltextaddr_all], [netfulltextaddr_all_std], [bzcnclassnum], [bzreplacedbz], [bzrefbz], [showwriter], [bzcommittee]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, title_c, title_e, bzmaintype, years, media_c, bznum2, bzpubdate, bzimpdate, bzstatus, bzcountry, bzissued, showorgan, 
					sClass, keyword_c, keyword_e, bzintclassnum, remark_c, bzpagenum, bzsubsbz, netfulltextaddr, rawid, "5", language, titletype, "VIP", netfulltextaddr_all, netfulltextaddr_all_std, bzcnclassnum, bzreplacedbz, bzrefbz, showwriter, bzcommittee);								
			
			
//			String sql = "INSERT INTO modify_title_info([lngid]) ";
//			sql += " VALUES ('%s');";
//			sql = String.format(sql, lngid);								
			context.getCounter("map", "4pre_" + rawid.toUpperCase().substring(0, 4)).increment(1);
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