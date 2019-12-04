package simple.jobstream.mapreduce.user.walker.WOS;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.InetAddress;
import java.net.InetSocketAddress;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import javax.sound.sampled.Line;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.HAUtil;
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
import com.process.frame.JobStreamRun;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;


public class Update2Db3Test extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Update2Db3Test.class);
	
	private static String postfixDb3 = "wos_update";
	private static String tempFileDb3 = "/RawData/WOS/rel_file/wos_template_update.db3/update-r-00000";

	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "/RawData/WOS/update_data";
	public static String outputHdfsPath = "/vipuser/walker/output/Update2Db3Test";
	

	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		
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

	public int getReduceNum() {
		long total = 0;
		
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
			{
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.default.name"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);

			InetSocketAddress active = HAUtil.getAddressOfActive(hdfs);
			System.out.println("hdfs host:"+active.getHostName());  //hadoop001
			System.out.println("hdfs port:"+active.getPort());      // 9000
			InetAddress address = active.getAddress();
			System.out.println("hdfs://"+address.getHostAddress()+":"+active.getPort());  //   hdfs://192.168.8.21:9000
			
			
			
			Path path = new Path("/vipuser/walker/output/Update2Db3TestLog");
			FileStatus[] files = hdfs.listStatus(path);

			for (FileStatus file : files) {
				if (file.getPath().getName().endsWith(".txt")) {
					System.out.println(file.getPath().getName());
					
				
					BufferedReader bufRead = null;

			        FSDataInputStream fin = hdfs.open(file.getPath());
			        
			        bufRead = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
			        String line = null;
			        while ((line = bufRead.readLine()) != null) {
						line = line.trim();
						if (line.startsWith("map_out:")) {
							String item = line.substring("map_out:".length()).trim();
							total += Long.parseLong(item);
						}
					}
			        
			        fin.close();
			        bufRead.close();
				}
			}
			hdfs.close();
			
			System.out.println("****** total:" + total);
			
		} catch (Exception ex) {
			System.out.println("*****************exit****************:");
			ex.printStackTrace();
			System.exit(-1);
		}
		
		return (int) (total / 10000);
	}
	
	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);
		
		reduceNum = getReduceNum();
		System.out.println("******reduceNum:" + reduceNum);
		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		private  static Map<String, String> mapLib =new HashMap<String, String>();		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMapLib();
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyyMMdd_HH_mm_ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			
			String text = "jobName:" + context.getJobName() + "\n";
			text += "status:" + context.getStatus() + "\n";
			text += "progress:" + context.getProgress() + "\n";
			text += "map_out:" + context.getCounter("map", "out").getValue() + "\n";
			text += "reduce_out:" + context.getCounter("Map-Reduce Framework", "Reduce output records").getValue() + "\n";

			BufferedWriter out = null;
			// 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        FSDataOutputStream fout = null;
	        String pathfile = "/vipuser/walker/output/Update2Db3TestLog/" + nowTime + ".txt";
	        if (fs.exists(new Path(pathfile))) {
	        	fout = fs.append(new Path(pathfile));
			}
	        else {
	        	fout = fs.create(new Path(pathfile));
	        }
	        
	        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
		    out.write(text);
		    out.close();
		    fs.close();
		}

		private static void initMapLib() {
			mapLib.put("SCI", "SCI-EXPANDED");
			mapLib.put("SSCI", "SSCI");
			mapLib.put("AHCI", "A&HCI");
			mapLib.put("ISTP", "CPCI-S");		//会议
			mapLib.put("ISSHP", "CPCI-SSH");	//会议
			mapLib.put("ESCI", "ESCI");
			mapLib.put("CCR", "CCR-EXPANDED");
			mapLib.put("IC", "IC");
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
		        String pathfile = "/walker/log/log_map/" + nowDate + ".txt";
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
		
			
		
		private static String getRangeByLibName(String libName) {
			String range = "";
			
			for (String lib : libName.split(";")) {
				lib = lib.trim();
				if (lib.length() < 1) {
					continue;
				}
				String showLib = lib;
				if (mapLib.containsKey(lib)) {
					showLib = mapLib.get(lib);
				}
				range += showLib + ";";
			}
			range = range.replaceAll(";+$", "");	//去掉末尾多余的分号
			
			return range;
		}
		
		private static String getIncludeID(String range, String UT) {
			String includeid = "";
			for (String item : range.split(";")) {
				item = item.trim();
				if (item.length() > 0) {
					includeid += "[" + item + "]" + UT + ";";
				}
			}
			includeid = includeid.replaceAll(";+$", "");	//去掉末尾多余的分号
			
			return includeid;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);

			String UT = "";
			String TC = "";
			String Z9 = "";
			String LIBName = "";

			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("UT")) {
					UT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("TC")) {
					TC = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Z9")) {
					Z9 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("LIBName")) {
					LIBName = updateItem.getValue().trim();
				}
			}
			
			
			if (!UT.startsWith("WOS:")) {
				context.getCounter("map", "error wos").increment(1);
				return;
			}
			for (String lib : LIBName.split(";")) {
				context.getCounter("map", lib).increment(1);
			}
			
			String lngid = "WKL_" + UT.substring(4);
			String range = getRangeByLibName(LIBName);
			String includeid = getIncludeID(range, UT);
		    String wos_tc = TC.replace('\0', ' ').replace("'", "''").trim();
		    String  wos_z9 = Z9.replace('\0', ' ').replace("'", "''").trim();
					   
			String sql = "INSERT INTO modify_title_info([lngid], [wos_tc], [wos_z9], [range], [includeid]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, wos_tc, wos_z9, range, includeid);
							
			context.getCounter("map", "out").increment(1);
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