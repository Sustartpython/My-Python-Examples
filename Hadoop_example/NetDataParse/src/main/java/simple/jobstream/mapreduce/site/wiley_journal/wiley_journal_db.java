package simple.jobstream.mapreduce.site.wiley_journal;

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

//输入应该为去重后的JSON
public class wiley_journal_db extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(wiley_journal_db.class);
	
	private static String postfixDb3 = "wileyjournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "wileyjournal.xxxobj2db";
		
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
		System.out.println("******io.compression.codecs*******"
				+ job.getConfiguration().get("io.compression.codecs"));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);
		
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {

		private static Map<String, String> monthMap = new HashMap<String, String>();

		private static void initMonthmonthMap() {
			monthMap.put("january", "01");
			monthMap.put("jan", "01");
			monthMap.put("jan.", "01");
			monthMap.put("february", "02");
			monthMap.put("feb", "02");
			monthMap.put("feb.", "02");
			monthMap.put("march", "03");
			monthMap.put("mar", "03");
			monthMap.put("mar.", "03");
			monthMap.put("april", "04");
			monthMap.put("apr", "04");
			monthMap.put("apr.", "04");
			monthMap.put("may", "05");
			monthMap.put("may.", "05");
			monthMap.put("june", "06");
			monthMap.put("jun", "06");
			monthMap.put("jun.", "06");
			monthMap.put("july", "07");
			monthMap.put("jul", "07");
			monthMap.put("jul.", "07");
			monthMap.put("august", "08");
			monthMap.put("aug", "08");
			monthMap.put("aug.", "08");
			monthMap.put("september", "09");
			monthMap.put("sept", "09");
			monthMap.put("sept.", "09");
			monthMap.put("sep", "09");
			monthMap.put("sep.", "09");
			monthMap.put("october", "10");
			monthMap.put("oct", "10");
			monthMap.put("oct.", "10");
			monthMap.put("november", "11");
			monthMap.put("nov", "11");
			monthMap.put("nov.", "11");
			monthMap.put("december", "12");
			monthMap.put("dezember", "12");
			monthMap.put("dec", "12");
			monthMap.put("dec.", "12");
			monthMap.put("oktober", "10");
			monthMap.put("juni", "06");
		}
		
		public String getMapValueByKey(String mykey) {
			String value = "00";
			for (Map.Entry entry : monthMap.entrySet()) {

				String key = entry.getKey().toString();
				if (mykey.toLowerCase().startsWith(key)) {
					value = entry.getValue().toString();
					break;

				}

			}
			return value;

		}

		public void setup(Context context) throws IOException,
				InterruptedException {
			initMonthmonthMap();
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {

			String rawid = "";
			String doi = "";
			String title = "";
			String identifier_pissn = "";
			String creator = "";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String date = "";
			String volume = "";
			String issue = "";
			String description = "";
			String startpage = "";
			String endpage = "";
			String date_created = "";
			String subject = "";
			String punumber = "";
			String gch = "";
			String cover = "";
			String page = "";
			String identifier_eissn = "";
			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}

			}
			if (title.equals("")){
				return ;
			}
			if (publisher.equals("")){
				return ;
			}
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}

			String lngid = "WILEY_WK_" + rawid.replace('\0', ' ').replace("'", "''").trim();
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			doi = doi.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").replace("，", "").trim();
			creator_institution = creator_institution.replace('\0', ' ')
					.replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			date_created = date_created.replace('/', ' ').replace("'", "''").replace(".", "").trim();
			date = "";
			date_created = date_created.toLowerCase();
			date_created = date_created.replaceAll("\\s+", " ").trim();
			String parseDate = "";
			String[] vec = date_created.split(" ");
			if (vec.length == 3) {
				String year = date_created.split(" ")[2];
				String Month = date_created.split(" ")[1];
				Month = getMapValueByKey(Month);
				String day = date_created.split(" ")[0];
				if(year.length()<4 || year.length()>4){
					year = "1900";
				}
				if(day.length() == 1){
					day = "0" + day;
				}
				if(day.length() > 2){
					day = "00";
				}
				date = year;
				date_created = year + Month + day;
				parseDate = date_created;
			}
			else if (vec.length == 2) {
				String year = date_created.split(" ")[1];
				if(year.length()<4 || year.length()>4){
					year = "1900";
				}
				String Month = date_created.split(" ")[0];
				System.out.println(Month);
				Month = getMapValueByKey(Month);
				String day = "00";
				date = year;
				date_created = year + Month + day;
				parseDate = date_created;
			}
			else{
				
				String pattern = "\\d{4}";
				Pattern r = Pattern.compile(pattern);
				Matcher my = r.matcher(date_created);
				if (my.find()) {
					date = my.group(0);
				}

				// date_created 规范化
				
				if (date != "") {
					parseDate = date + "0000";
				}
				
			}

			if (parseDate == "") {
				parseDate = "19000000";
				date = "1900";
			}

			volume = volume.replace('\0', ' ').replace("'", "''").trim();
			page = page.replace("'", "");
			issue = issue.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''")
					.trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			String language = "EN";
			String country = "US";
			String provider = "wileyjournal";
			String type = "3";

			identifier_pissn = identifier_pissn.replace('\0', ' ')
					.replace("'", "''").trim();
			String batch = (new SimpleDateFormat("yyyyMMdd"))
					.format(new Date()) + "00";
			if(rawid.endsWith("#")){
				punumber = rawid.replace("#", "%23");
			}else {
				punumber = rawid;
			}
			String provider_url = provider+ "@"+"http://onlinelibrary.wiley.com/doi/"+ punumber+"/abstract";
			String medium = "2";
			String provider_id = provider + "@" + rawid;
			gch = "wileyjournal@"+identifier_pissn.replace("-", "");
			

			String sql = "INSERT INTO modify_title_info_zt([lngid],[gch], [rawid], [identifier_doi],  [title], [identifier_pissn], [creator], [creator_institution], [source], [publisher], [date_created], [volume], [issue], [description], [subject], [page], [type], [language], [country], [provider], [batch], [provider_url], [medium], [date],[provider_id],[identifier_eissn]) ";
			sql += " VALUES ( '%s', '%s','%s',  '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s');";
			sql = String.format(sql, lngid,gch, rawid, doi, title,
					identifier_pissn, creator, creator_institution, source,
					publisher, parseDate, volume, issue, description, subject,
					page, type, language, country, provider, batch,
					provider_url, medium, date, provider_id,identifier_eissn);
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
		        String pathfile = "/vipuser/chenyong/log/log_map/" + nowDate + ".txt";
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
		protected void setup(Context context) throws IOException,
				InterruptedException {
			try {
				System.setProperty("sqlite4java.library.path", "/usr/lib64/");

				// 创建存放db3文件的本地临时目录
				String taskId = context.getConfiguration()
						.get("mapred.task.id");
				String JobDir = context.getConfiguration().get("job.local.dir");
				tempDir = JobDir + File.separator + taskId;
				File baseDir = new File(tempDir);
				if (!baseDir.exists()) {
					baseDir.mkdirs();
				}

				//
				hdfs = FileSystem.get(context.getConfiguration());
				sqlCounter = context.getCounter("reduce", "sqlCounter");

				
				String db3PathFile = baseDir.getAbsolutePath() + File.separator
						+ taskId + "_" + postfixDb3 + ".db3";
				Path src = new Path(tempFileDb3); // 模板文件（HDFS路径）
				Path dst = new Path(db3PathFile); // local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath()
						+ File.separator + "." + taskId + "_" + postfixDb3 + ".db3.crc");
				if (crcFile.exists()) {
					if (crcFile.delete()) { // 删除crc文件
						logger.info("***** delete success:"
								+ crcFile.toString());
					} else {
						logger.info("***** delete failed:" + crcFile.toString());
					}
				}

				connSqlite = new SQLiteConnection(new File(db3PathFile));
				connSqlite.open();
			} catch (Exception e) {
				logger.error(
						"****************** setup failed. ******************",
						e);
			}

			logger.info("****************** setup finished  ******************");
		}

		public void insertSql(Context context) {
			String sql = "";
			if (sqlList.size() > 0) {
				try {
					connSqlite.exec("BEGIN TRANSACTION;");
					for (int i = 0; i < sqlList.size(); ++i) {
						sql = sqlList.get(i);
						connSqlite.exec(sql);
						sqlCounter.increment(1);
					}
					connSqlite.exec("COMMIT TRANSACTION;");

					sqlList.clear();

				} catch (Exception e) {
					context.getCounter("reduce", "insert error").increment(1);
					//log2HDFSForMapper(context, sql);
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
				InterruptedException {
			logger.info("****************** Enter cleanup ******************");
			insertSql(context); // 处理余数
			if (connSqlite != null && connSqlite.isOpen()) {
				connSqlite.dispose(); // 关闭sqlite连接
			}

			try {
				File localDir = new File(tempDir);
				if (!localDir.exists()) {
					throw new FileNotFoundException(tempDir + " is not found.");
				}

				// 再次获取，这里并不能感知到pre获取的参数
				outputHdfsPath = context.getConfiguration().get(
						"outputHdfsPath");
				// 最终存放db3的hdf目录。嵌在了MR的输出目录，便于自动清空。
				Path finalHdfsPath = new Path(outputHdfsPath + File.separator
						+ "/db3/");

				/*
				 * if (!hdfs.exists(finalHdfsPath)) {
				 * //hdfs.delete(finalHdfsPath, true);
				 * hdfs.mkdirs(finalHdfsPath); //创建输出目录 }
				 */

				File[] files = localDir.listFiles();
				for (File file : files) {
					if (file.getName().endsWith(".db3")) {
						Path srcPath = new Path(file.getAbsolutePath());
						Path dstPash = new Path(finalHdfsPath.toString() + "/"
								+ file.getName());
						hdfs.moveFromLocalFile(srcPath, dstPash); // 移动文件
						// hdfs.copyFromLocalFile(true, true, srcPath, dstPash);
						// //删除local文件，并覆盖hdfs文件
						logger.info("copy " + srcPath.toString() + " to "
								+ dstPash.toString());
					}
				}
			} catch (Exception e) {
				logger.error(
						"****************** upload file failed. ******************",
						e);
			}
		}

	}
}