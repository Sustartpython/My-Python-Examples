package simple.jobstream.mapreduce.site.wanfang_cg.std;

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
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.avro.Schema.Field.Order;
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

import simple.jobstream.mapreduce.site.cnkibs.ClassType;
import simple.jobstream.mapreduce.site.wanfang_cg.ClassLoad;

//输入应该为去重后的html
public class StdZLFCG extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZLFCG.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 50;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "W_CG_";
	private static String tempFileDb3 = "/RawData/wanfang/cg/template/wanfang_cg_template.db3";
	
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
		
		private static HashMap<String, String> FirstClassMap      = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap     = new HashMap<>();		
		private static ClassType classtype = null;
		
		private static HashSet<String> pubNoSet =  new HashSet<String>();
		
		private static void initPubNoSet(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/cg/txt/20181118/delate.txt"));
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
			String firstclass_info  = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);
			
			FirstClassMap      = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap     = classload.getsecondclass();
			
			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);
			
			initPubNoSet(context);
			
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
		
		private static String getLngIDByWanID(String wanID) {
			wanID = wanID.toUpperCase();
			String lngID = "W_";
			for (int i = 0; i < wanID.length(); i++) {
				lngID += String.format("%d", wanID.charAt(i) + 0);
			}
			
			return lngID;
		}
		
		public static String getJsonStrByKV(String Key, List<String> Value) {
			Gson gson = new Gson();
			HashMap<String, List<String>> tmpMap = new HashMap<String, List<String>>();  
			tmpMap.put(Key,Value);
			return gson.toJson(tmpMap);
		}
		
		public String CovertID(String rawid) {
			rawid = rawid.toUpperCase().trim();
			String numStr = "";
			for (int i = 0; i < rawid.length(); i++) {
				char ch = rawid.charAt(i);
				if (ch >= '0' && ch <= '9') {
					numStr += ch;
				} else {
					numStr += String.valueOf(((int)ch));
				}
			}
			
			String lngid = "W_CG_" + String.valueOf(Long.parseLong(numStr)*2+6);
			if (lngid.length() > 5) {
				return lngid;
			} else {
				return "";
			}	
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String rawid = "";
			String title_c = "";
	    	String title_e = "";
	    	//项目编号：
	    	String cgitemnumber = "";
	    	String years = "";
	    	String cglimituse = "";
	    	String cgprovinces = "";
	    	String _class = "";
	    	String cgcgtypes = "";
	    	String keyword_c = "";
			String remark_c = "";
			String cgappunit = "";
			String cgappdate = "";
			String cgregdate = "";
			String Cgtransrange = "";
			String cgrecomunit = "";
			String media_c = "";
			String cgprocesstimes = "";
			String Showorgan = "";
			String Showwriter = "";
			String cgindustry = "";
			String cginduscode = "";
			String cgcontactunit = "";
			String cgcontactaddr = "";
			String cgcontactfax = "";
			String cgzipcode = "";
			String cgcontactemail = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgitemnumber")) {
					cgitemnumber = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cglimituse")) {
					cglimituse = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgprovinces")) {
					cgprovinces = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("_class")) {
					_class = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgappunit")) {
					cgappunit = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgappdate")) {
					cgappdate = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgregdate")) {
					cgregdate = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("Cgtransrange")) {
					Cgtransrange = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgrecomunit")) {
					cgrecomunit = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("media_c")) {
					media_c = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgprocesstimes")) {
					cgprocesstimes = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("Showorgan")) {
					Showorgan = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("Showwriter")) {
					Showwriter = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgindustry")) {
					cgindustry = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cginduscode")) {
					cginduscode = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgcontactunit")) {
					cgcontactunit = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgcontactaddr")) {
					cgcontactaddr = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgcontactfax")) {
					cgcontactfax = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgzipcode")) {
					cgzipcode = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cgcontactemail")) {
					cgcontactemail = updateItem.getValue().trim();
				}	
			}
			
			//特殊字段处理
			String classtypeString = classtype.GetClassTypes(_class);
			String showclasstypesString = classtype.GetShowClassTypes(_class);
			String NetFullTextAddr = "";
			String lngid = "";
			String FullTextAddress = "NoPDF";
			String language = "1";
			String type_ = "6";
			String titletype = "0;1;1536;1537";
			NetFullTextAddr = "http://d.wanfangdata.com.cn/Cstad/" + cgitemnumber;
			lngid = CovertID(rawid);
			

			
			//转义
			{	
				title_c = title_c.replace('\0', ' ').replace("'", "''").trim();
				title_e = title_e.replace('\0', ' ').replace("'", "''").trim();
				cgitemnumber  = cgitemnumber .replace('\0', ' ').replace("'", "''").trim();
				years = years.replace('\0', ' ').replace("'", "''").trim();
				cglimituse = cglimituse.replace('\0', ' ').replace("'", "''").trim();
				cgprovinces = cgprovinces.replace('\0', ' ').replace("'", "''").trim();
				_class = _class.replace('\0', ' ').replace("'", "''").trim();
				cgcgtypes = cgcgtypes.replace('\0', ' ').replace("'", "''").trim();
				keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();
				remark_c  = remark_c.replace('\0', ' ').replace("'", "''").trim();
				cgappunit = cgappunit.replace('\0', ' ').replace("'", "''").trim();
				cgappdate = cgappdate.replace('\0', ' ').replace("'", "''").trim();
				cgregdate = cgregdate.replace('\0', ' ').replace("'", "''").trim();
				Cgtransrange = Cgtransrange.replace('\0', ' ').replace("'", "''").trim();
				cgrecomunit = cgrecomunit.replace('\0', ' ').replace("'", "''").trim();
				media_c = media_c.replace('\0', ' ').replace("'", "''").trim();
				cgprocesstimes = cgprocesstimes.replace('\0', ' ').replace("'", "''").trim();
				Showorgan = Showorgan.replace('\0', ' ').replace("'", "''").trim();
				Showwriter = Showwriter.replace('\0', ' ').replace("'", "''").trim();
				cgindustry = cgindustry.replace('\0', ' ').replace("'", "''").trim();
				cginduscode = cginduscode.replace('\0', ' ').replace("'", "''").trim();
				cgcontactunit = cgcontactunit.replace('\0', ' ').replace("'", "''").trim();
				cgcontactaddr  = cgcontactaddr.replace('\0', ' ').replace("'", "''").trim();
				cgcontactfax  = cgcontactfax.replace('\0', ' ').replace("'", "''").trim();
				cgzipcode  = cgzipcode.replace('\0', ' ').replace("'", "''").trim();
				cgcontactemail   = cgcontactemail.replace('\0', ' ').replace("'", "''").trim();
				
			}
			
			String sql = "INSERT INTO modify_title_info([lngid],[rawid],[title_c],[cgitemnumber],[years],[cglimituse],[cgprovinces],[class], " +
					"[cgcgtypes],[keyword_c],[remark_c],[cgappunit],[cgappdate],[cgrecomunit],[media_c],[cgprocesstimes],[Showorgan],[Showwriter],[cgindustry],[cginduscode], " +
					"[cgcontactunit],[cgcontactaddr],[cgcontactfax],[cgzipcode],[cgcontactemail],[NetFullTextAddr],[FullTextAddress], " +
					"[language],[type],[titletype],[cgregdate],[Cgtransrange],[classtypes],[showclasstypes]) ";
			sql += "VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, title_c, cgitemnumber, years, cglimituse, cgprovinces, _class, 
					cgcgtypes, keyword_c, remark_c, cgappunit, cgappdate, cgrecomunit, media_c, cgprocesstimes, Showorgan, Showwriter, cgindustry, cginduscode, 
					cgcontactunit, cgcontactaddr, cgcontactfax, cgzipcode, cgcontactemail, NetFullTextAddr, FullTextAddress, 
					language, type_, titletype, cgappdate, Cgtransrange, classtypeString, showclasstypesString);
			
//			String sql = "INSERT INTO modify_title_info([lngid],[rawid]) ";
//			sql += "VALUES ('%s','%s');";
//			sql = String.format(sql, lngid,rawid);
			
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