package simple.jobstream.mapreduce.user.walker.QK;

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
public class WFQK2DB3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(WFQK2DB3.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "/RawData/wanfang/qk/detail/XXXXObject";
	public static String outputHdfsPath = "/vipuser/walker/output/QK/WFQK2DB3";
	
	private static String postfixDb3 = "wanfang_qk";
	private static String tempFileDb3 = "/RawData/wanfang/qk/template/wanfang_qk_template.db3";
	
	public void pre(Job job) {
		String jobName = "WFQK2DB3";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
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
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		ArrayList<String> orgArrayList = new ArrayList<String>();
		
		private void initOrgList() {
			orgArrayList.add("Sichuan University ");
			orgArrayList.add("川大");
			orgArrayList.add("四川中西学堂");
			orgArrayList.add("锦江书院");
			orgArrayList.add("尊经书院");
			orgArrayList.add("四川通省大学堂");
			orgArrayList.add("四川省城高等学堂");
			orgArrayList.add("四川官立高等学校");
			orgArrayList.add("国立成都高等师范学校");
			orgArrayList.add("国立成都大学");
			orgArrayList.add("国立成都师范大学");
			orgArrayList.add("华西大学");
			orgArrayList.add("四川医学院");
			orgArrayList.add("成都科学技术大学");
			orgArrayList.add("成都工学院");
			orgArrayList.add("四川化工学院");
			orgArrayList.add("华西医科大学");
			orgArrayList.add("四川联合大学");

		}
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initOrgList();
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
		
		private static String getLngIDByWanID(String wanID) {
			wanID = wanID.toUpperCase();
			String lngID = "W_";
			for (int i = 0; i < wanID.length(); i++) {
				lngID += String.format("%d", wanID.charAt(i) + 0);
			}
			
			return lngID;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
			String rawid = "";
			String pykm = "";
			String title_c = "";
			String title_e = "";
			String remark_c = "";
			String remark_e = "";
			String doi = "";
			String author_c = "";
			String author_e = "";
			String organ = "";
			String name_c = "";
			String name_e = "";
			String years = "";
			String vol = "";
			String num = "";
			String sClass = "";
			String keyword_c = "";
			String keyword_e = "";
			String imburse = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_c")) {
					author_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_e")) {
					author_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_c")) {
					name_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_e")) {
					name_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("imburse")) {
					imburse = updateItem.getValue().trim();
				}

			}
			
			boolean bFind = false;
			for (String org : orgArrayList) {
				if (organ.indexOf(org) > -1) {
					bFind = true;
					break;
				}
			}
			if (!bFind) {
				return;
			}
			
			
			String showwriter = author_c.length() > 0 ? author_c : author_e;
			String showorgan = organ;
			String lngid = getLngIDByWanID(rawid);		
			String bookid = lngid.substring(0, lngid.length()-6);
			String firstclass = "";
			String firstwriter = "";
			String firstorgan = "";
			String[] vec = sClass.split(" ");
			if (vec.length > 0) {
				firstclass = vec[0].trim();
			}
			vec = showwriter.split(";");
			if (vec.length > 0) {
				firstwriter = vec[0].trim();
				firstwriter = firstwriter.replaceAll("\\[.*?\\]$", "");		//去掉后面的标号
			}
			String cbmwriter = showwriter;
			String writer = showwriter.replace(';', ' ');
			
			vec = showorgan.split(";");
			if (vec.length > 0) {
				firstorgan = vec[0].trim();
				firstorgan = firstorgan.replaceAll("^\\[.*?\\]", "");		//去掉前面的标号
			}
			
			//转义
			{
				title_c = title_c.replace('\0', ' ').replace("'", "''").trim();	
				title_e = title_e.replace('\0', ' ').replace("'", "''").trim();	
				author_c = author_c.replace('\0', ' ').replace("'", "''").trim();	
				author_e = author_e.replace('\0', ' ').replace("'", "''").trim();	
				firstwriter = firstwriter.replace('\0', ' ').replace("'", "''").trim();	
				showwriter  = showwriter.replace('\0', ' ').replace("'", "''").trim();	
				cbmwriter = cbmwriter.replace('\0', ' ').replace("'", "''").trim();
				writer = writer.replace('\0', ' ').replace("'", "''").trim();
				firstorgan = firstorgan.replace('\0', ' ').replace("'", "''").trim();	
				showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();	
				organ = organ.replace('\0', ' ').replace("'", "''").trim();	
				name_c = name_c.replace('\0', ' ').replace("'", "''").trim();	
				name_e = name_e.replace('\0', ' ').replace("'", "''").trim();	
				remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();	
				remark_e = remark_e.replace('\0', ' ').replace("'", "''").trim();	
				keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();	
				keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").trim();	
				imburse = imburse.replace('\0', ' ').replace("'", "''").trim();	
				doi = doi.replace('\0', ' ').replace("'", "''").trim();	
				sClass = sClass.replace('\0', ' ').replace("'", "''").trim();	
				firstclass = firstclass.replace('\0', ' ').replace("'", "''").trim();	
				years = years.replace('\0', ' ').replace("'", "''").trim();	
				vol = vol.replace('\0', ' ').replace("'", "''").trim();	
				num = num.replace('\0', ' ').replace("'", "''").trim();	
			}
			String fromtype = "WANFANG";
					   
			String sql = "INSERT INTO main([lngid], [bookid], [rawid], [qid], [title_c], [title_e], [firstwriter], [showwriter], [cbmwriter], [writer], [author_e], [firstorgan], [organ], [name_c], [name_e], [showorgan], [remark_c], [remark_e], [keyword_c], [keyword_e], [imburse], [doi], [class], [firstclass], [years], [vol], [num], [fromtype]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, bookid, rawid, pykm, title_c, title_e, firstwriter, showwriter, cbmwriter, writer, author_e, firstorgan, organ, name_c, name_e, showorgan, remark_c, remark_e, keyword_c, keyword_e, imburse, doi, sClass, firstclass, years, vol, num, fromtype);								
			
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
				//outputHdfsPath = context.getConfiguration().get("outputHdfsPath");
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