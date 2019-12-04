package simple.jobstream.mapreduce.site.rucjournal;

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
import java.util.HashMap;

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
import org.json.JSONArray;
import org.json.JSONObject;
import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class StdXXXXObject extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdXXXXObject.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 2;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "ezuezhe";

	
	public void pre(Job job) {
		String jobName = "step3_db3";
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
		
		Pattern patYearNum = Pattern.compile("(\\d{4})年([a-zA-Z0-9]+)期");
		
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
		
		
		public static JSONObject createnetfulltextaddr_all_std(String rawid){
			String [] attr = {rawid};
			String key = "RDFYBKZL@RUC";
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
		
		public static JSONObject createnetfulltextaddr_all(String rawid){
			String [] attr = {"http://ipub.exuezhe.com/paper.html?id=" + rawid};
			String key = "RDFYBKZL@RUC";
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
		
		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String lngID = "";
			String rawID = "";
			String titletype = "0;1;256;257";
			String media_c = "";
			String years = "";
			String title_c = "";
			String showwriter = "";
			String showorgan = "";
			String title_e = "";
			String keyword_c = "";
			String keyword_e = "";
			String remark_c = "";
			String remark_e = "";
			String medias_qk = "";
			String language = "";
			String type = "";
			String FirstWriter = "";
			String Introduce = "";
			String srcID = "";
			String range = "";
			String srcproducer = "";
			String includeid = "";
			String netfulltextaddr_all = "" ;
			String netfulltextaddr_all_std = "" ;
			String netfulltextaddr = "";
			String 作者 = "";
			String 作者简介 = "";
			String 译者 = "";
			String still = "";
			String opc = "";
			String oad = "";
			String opy = "";
			String opn = "";
			String opg = "";
			String source = "";
			String num = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawID")) {
					rawID = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("lngID")) {
					lngID = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("showwriter")) {
					showwriter = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				}
				
				if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_e")) {
					keyword_e = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("media_qk")) {
					medias_qk = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("media_c")) {
					media_c = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("FirstWriter")) {
					FirstWriter = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Introduce")) {
					Introduce = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("netfulltextaddr_all")) {
					netfulltextaddr_all = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("netfulltextaddr")) {
					netfulltextaddr = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("作者")) {
					作者 = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("作者简介")) {
					作者简介 = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("译者")) {
					译者 = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("still")) {
					still = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opc")) {
					opc = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("oad")) {
					oad = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opy")) {
					opy = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opn")) {
					opn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opg")) {
					opg = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
				if(updateItem.getKey().equals("netfulltextaddr_all_std")){
					netfulltextaddr_all_std = updateItem.getValue().trim();
				}
			}
			
			
			netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawID).toString();
			netfulltextaddr_all = createnetfulltextaddr_all(rawID).toString();
			rawID = rawID.replace('\0', ' ').replace("'", "''").trim();
			
			srcID = "RUC";
			range = "RDFYBKZL";
			srcproducer = "RDFYBKZL";
			includeid = "[RDFYBKZL]" + rawID;
			titletype = titletype.replace('\0', ' ').replace("'", "''").trim();
			media_c = media_c.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title_c = title_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			showwriter = showwriter.replace('\0', ' ').replace("'", "''").trim().replace("#", ";");
			showorgan = showorgan.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title_e = title_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			remark_c = remark_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			remark_e = remark_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			medias_qk = medias_qk.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			language = language.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			type = type.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			FirstWriter = FirstWriter.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Introduce = Introduce.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcID = srcID.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			range = range.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcproducer = srcproducer.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			includeid = includeid.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr_all = netfulltextaddr_all.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr = netfulltextaddr.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr_all_std = netfulltextaddr_all_std.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			作者 = 作者.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			作者简介 = 作者简介.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			译者 = 译者.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			still = still.replace('\0', ' ').replace("'", "''").trim().replace("——", "").replace("\n", "");
			opc = opc.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			oad = oad.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opy = opy.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opn = opn.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opg = opg.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			source = source.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Matcher matYearNum = patYearNum.matcher(medias_qk);
			if (matYearNum.find()) {
				years = matYearNum.group(1);
				num = matYearNum.group(2);
			}else{
				context.getCounter("map", "years null").increment(1);
			}
			
			String sql = "INSERT INTO modify_title_info([lngID], [rawID] , [titletype] , [media_c] , [years] , [title_c] , [title_e] , [keyword_c] , [keyword_e] , [remark_c] , [remark_e] , [showwriter] , [showorgan] , [medias_qk] , [language] , [type] , [FirstWriter] , [Introduce] , [srcID] , [range] , [srcproducer] , [includeid] , [netfulltextaddr_all], [netfulltextaddr], [auto],[autoinfo],[tauto],[still],[opc],[oad],[opy],[opn],[opg],[source],[netfulltextaddr_all_std],[num])";
			sql += " VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s','%s');";
			sql = String.format(sql, lngID, rawID , titletype , media_c , years , title_c , title_e , keyword_c , keyword_e , remark_c , remark_e , showwriter , showorgan , medias_qk , language , type , FirstWriter , Introduce , srcID , range , srcproducer , includeid , netfulltextaddr_all , netfulltextaddr, 作者,作者简介, 译者 ,still,opc,oad,opy,opn,opg,source,netfulltextaddr_all_std,num);
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
				
				String tempPathFile = "/RawData/exuezhe/template/exuezhe_zlf_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + postfixDb3 +".db3";
				Path src = new Path(tempPathFile);	//模板文件（HDFS路径）
				Path dst = new Path(db3PathFile);	//local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath() + File.separator + "." +  taskId + "_" + postfixDb3 +".db3.crc");
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
			if (sqlList.size() > 1000) {
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
			
			if (sqlList.size() > 1) {
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