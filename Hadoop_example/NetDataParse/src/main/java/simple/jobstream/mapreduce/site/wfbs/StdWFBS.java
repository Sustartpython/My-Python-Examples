package simple.jobstream.mapreduce.site.wfbs;

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
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

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
import org.json.JSONArray;
import org.json.JSONObject;
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

import simple.jobstream.mapreduce.site.wanfang_hy.ClassLoad;
import simple.jobstream.mapreduce.site.wanfang_hy.ClassType;

//输入应该为去重后的html
public class StdWFBS extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdWFBS.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 10;
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "wfbs.Std2zlf";
		
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
		
		private static Map<String, String> monthMap = new HashMap<String, String>();
		private static HashMap<String, String> FirstClassMap      = new HashMap<>();
		private static HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		private static HashMap<String, String> SecondClassMap     = new HashMap<>();		
		private static ClassType classtype = null;
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			String firstclass_info  = "/RawData/_rel_file/FirstClass.txt";
			String secondclass_info = "/RawData/_rel_file/SecondClass.txt";
			ClassLoad classload = new ClassLoad(context, firstclass_info, secondclass_info);
			
			FirstClassMap      = classload.getfirstclass();
			SecondOnlyClassMap = classload.getsecondonlyclass();
			SecondClassMap     = classload.getsecondclass();
			
			classtype = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap);
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
		        String pathfile = "/zengfanrong/log/log_map/" + nowDate + ".txt";
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
			String title_c = "";
			String Showwriter = "";
			String bsspeciality = "";
			String bsdegree = "";
			String Showorgan = "";
			String bstutorsname = "";
			String years = "";
			String classFid = "";
			String keyword_c = "";
			String remark_c = "";
			String marksNumber = "";
			String doi = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Showwriter")) {
					Showwriter = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("bsspeciality")) {
					bsspeciality = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("bsdegree")) {
					bsdegree = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Showorgan")) {
					Showorgan = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("bstutorsname")) {
					bstutorsname = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("class")) {
					classFid = updateItem.getValue().trim();
					classFid = classFid.replace("，", ";").replace(" ", ";").trim();
					if(classFid.endsWith(";")) {
						classFid = classFid.replaceAll(";$", "");
						context.getCounter("map", "end with ;号").increment(1);
					}
				}

				if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("marksNumber")) {
					marksNumber = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}

			}
			
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			doi = doi.replace('\0', ' ').replace("'", "''").trim();
			title_c = title_c.replace('\0', ' ').replace("'", "''").trim();
			Showwriter = Showwriter.replace('\0', ' ').replace("'", "''").trim();
			bsspeciality = bsspeciality.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").trim();
			classFid = classFid.replace('\0', ' ').replace("'", "''").trim();
			keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();
			remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();
			marksNumber = marksNumber.replace('\0', ' ').replace("'", "''").trim();
			bsdegree = bsdegree.replace('\0', ' ').replace("'", "''").trim();
		    Showorgan = Showorgan.replace('\0', ' ').replace("'", "''").trim();
			bstutorsname = bstutorsname.replace('\0', ' ').replace("'", "''").trim();
			String lngid = wanID2vipID(rawid);
		
			String NetFullTextAddr = "http://www.wanfangdata.com.cn/details/detail.do?_type=degree&id=" + rawid;
			String language = "1";
			String type = "2";
			String titletype = "0;1;512;513";
			String srcID = "VIP";
			String srcproducer = "WANFANG";
			JSONObject NetFullTextAddr_All = createNetFullTextAddr_All(NetFullTextAddr);
			JSONObject netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawid);
			if (classFid.length() <1) {
				context.getCounter("map", "not find class").increment(1);
			}
			else {
				context.getCounter("map", "find class").increment(1);
			}
			String T_classtypes  = classtype.GetClassTypes(classFid);
			String T_showclasstypes = classtype.GetShowClassTypes(classFid);
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
			String sql = "INSERT INTO modify_title_info([lngid], [rawid], [title_c],  [Showwriter], [bsspeciality], [years], [class], [keyword_c], [remark_c], [bsdegree], [Showorgan], [bstutorsname], [NetFullTextAddr], [language], [type], [titletype], [srcID], [srcproducer], [NetFullTextAddr_All], [netfulltextaddr_all_std], [Doi],[classtypes],[showclasstypes]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, title_c, Showwriter, bsspeciality, years, classFid, keyword_c, remark_c, bsdegree, Showorgan, bstutorsname, NetFullTextAddr, language, type, titletype, srcID, srcproducer, NetFullTextAddr_All, netfulltextaddr_all_std, doi,T_classtypes,T_showclasstypes);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
			
		}
	}
	public static String  wanID2vipID(String wanID){
		String vipID = "";
		for(char a : wanID.toCharArray()){
			if('0' <= a && a <= '9'){
				vipID = vipID + a;
			}else{
				vipID = vipID + Integer.toString(0 + a);
			}
			System.out.println(vipID);
			
		}
		vipID = "W_BS_"+ Long.toString( Long.parseLong(vipID)*2 + 2);
		return vipID;
	}
	
	public static JSONObject createNetFullTextAddr_All(String addr){
		String [] attr = {addr};
		String key = "WANFANG@VIP";
		HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
		try{
		JSONArray array = new JSONArray(attr);
		map.put(key, array);
		JSONObject o = new JSONObject(map);
		
		return o;}
		catch(Exception e){
			return null;
		}
	}
	
	public static JSONObject createnetfulltextaddr_all_std(String rawid){
		String [] attr = {rawid};
		String key = "WANFANG@VIP";
		HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
		try{
		JSONArray array = new JSONArray(attr);
		map.put(key, array);
		JSONObject o = new JSONObject(map);
		return o;}catch(Exception e){
			return null;
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
				
				String tempPathFile = "/RawData/cnki/bs/ref_file/cnkibs_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + "wfbs.db3";
				Path src = new Path(tempPathFile);	//模板文件（HDFS路径）
				Path dst = new Path(db3PathFile);	//local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath() + File.separator + "." +  taskId + "_" + "wfbs.db3.crc");
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