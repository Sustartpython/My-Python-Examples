package simple.jobstream.mapreduce.site.aip;

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

//输入应该为去重后的html
public class CountHY extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(CountHY.class);
	
	private static String postfixDb3 = "aipjournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "StdAIP";
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
		
		private  static Map<String, String> mapSource =new HashMap<String, String>();
		private  static Map<String, String> mapPissn =new HashMap<String, String>();
		private  static Map<String, String> mapEissn =new HashMap<String, String>();
		
		
		private static void initMapSource() {
			mapSource.put("adv", "AIP Advances");
			mapSource.put("apc", "AIP Conference Proceedings");
			mapSource.put("apb", "APL Bioengineering");
			mapSource.put("apm", "APL Materials");
			mapSource.put("app", "APL Photonics");
			mapSource.put("apl", "Applied Physics Letters");
			mapSource.put("are", "Applied Physics Reviews");
			mapSource.put("bmf", "Biomicrofluidics");
			mapSource.put("cha", "Chaos: An Interdisciplinary Journal of Nonlinear Science");
			mapSource.put("cip", "Computers in Physics");
			mapSource.put("csx", "Computing in Science & Engineering");
			mapSource.put("jap", "Journal of Applied Physics");
			mapSource.put("phy", "Journal of Applied Physics");
			mapSource.put("jcp", "The Journal of Chemical Physics");
			mapSource.put("jmp", "Journal of Mathematical Physics");
			mapSource.put("jpr", "Journal of Physical and Chemical Reference Data");
			mapSource.put("rse", "Journal of Renewable and Sustainable Energy");
			mapSource.put("ltp", "Low Temperature Physics");
			mapSource.put("phf", "Physics of Fluids");
			mapSource.put("pfl", "Physics of Fluids");
			mapSource.put("pfa", "Physics of Fluids");
			mapSource.put("php", "Physics of Plasmas");
			mapSource.put("pfb", "Physics of Plasmas");
			mapSource.put("pto", "Physics Today");
			mapSource.put("rsi", "Review of Scientific Instruments");
			mapSource.put("sdy", "Structural Dynamics");

		}
		
		private static void initMapPissn() {
			mapPissn.put("adv", "");
			mapPissn.put("apc", "");
			mapPissn.put("apb", "2473-2877");
			mapPissn.put("apm", "2166-532X");
			mapPissn.put("app", "2378-0967");
			mapPissn.put("apl", "0003-6951");
			mapPissn.put("are", "");
			mapPissn.put("bmf", "1932-1058");
			mapPissn.put("cha", "1054-1500");
			mapPissn.put("cip", "");
			mapPissn.put("csx", "");
			mapPissn.put("jap", "0021-8979");
			mapPissn.put("phy", "0021-8979");
			mapPissn.put("jcp", "0021-9606");
			mapPissn.put("jmp", "0022-2488");
			mapPissn.put("jpr", "0047-2689");
			mapPissn.put("rse", "");
			mapPissn.put("ltp", "1063-777X");
			mapPissn.put("phf", "1070-6631");
			mapPissn.put("pfl", "1070-6631");
			mapPissn.put("pfa", "1070-6631");
			mapPissn.put("php", "1070-664X");
			mapPissn.put("pfb", "1070-664X");
			mapPissn.put("pto", "0031-9228");
			mapPissn.put("rsi", "0034-6748");
			mapPissn.put("sdy", "");			
		}
		
		private static void initMapEissn() {
			mapEissn.put("adv", "2158-3226");
			mapEissn.put("apc", "");
			mapEissn.put("apb", "");
			mapEissn.put("apm", "");
			mapEissn.put("app", "");
			mapEissn.put("apl", "1077-3118");
			mapEissn.put("are", "1931-9401");
			mapEissn.put("bmf", "");
			mapEissn.put("cha", "1089-7682");
			mapEissn.put("cip", "");
			mapEissn.put("csx", "");
			mapEissn.put("jap", "1089-7550");
			mapEissn.put("phy", "1089-7550");
			mapEissn.put("jcp", "1089-7690");
			mapEissn.put("jmp", "1089-7658");
			mapEissn.put("jpr", "1529-7845");
			mapEissn.put("rse", "1941-7012");
			mapEissn.put("ltp", "");
			mapEissn.put("phf", "1089-7666");
			mapEissn.put("pfl", "1089-7666");
			mapEissn.put("pfa", "1089-7666");
			mapEissn.put("php", "1089-7674");
			mapEissn.put("pfb", "1089-7674");
			mapEissn.put("pto", "1945-0699");
			mapEissn.put("rsi", "1089-7623");
			mapEissn.put("sdy", "");
		}
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMapSource();
			initMapPissn();
			initMapEissn();
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
		        String pathfile = "/venter/mirror_chaoxing/log/log_map/" + nowDate + ".txt";
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
	    	String volume = "";
	    	String issue = "";
	    	String catalog = "";
	    	String identifier_doi = "";
				
			String title = "";//标题
//			String title_alternative="";
			String creator = "";//作者
			//String publishers = "";//发行商
			//String identifier_pisbn = "";//isbn号
			String date="";//出版日期
			String creator_institution="";//机构
			String description = "";//内容提要
			String provider_subject = "";
			String rawid = "";//
			
            String source = "";
            String identifier_pissn = "";
            String identifier_eissn = "";
            String press_year="";

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider="aipjournal";
		    String provider_url="";
		    String provider_id="";
		    String country="US";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "EN";
			String type = "3";
			String medium = "2";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("catalog")) {
					catalog = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("insitution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("press_year")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date_created = updateItem.getValue().trim();
				}
			}
			if (catalog.equals("apc")) {
				context.getCounter("map", "count apc").increment(1);
			}
			return;
			
			
			
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