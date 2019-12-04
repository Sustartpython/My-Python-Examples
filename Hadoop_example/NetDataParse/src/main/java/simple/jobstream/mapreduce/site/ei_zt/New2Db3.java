package simple.jobstream.mapreduce.site.ei_zt;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.net.URI;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.Map.Entry;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
import com.process.frame.JobStreamRun;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//
public class New2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(New2Db3.class);
	
	private static String postfixDb3 = "ei_new";
	private static String tempFileDb3 = "/RawData/EI_ZT/rel_file/ei_template.db3";
	
	
	private static int reduceNum = 2;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	

	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		
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

	public int getReduceNum(Job job) {
		long total = 0;
		
		try {
			Configuration conf = new Configuration();
			for (Entry<Object, Object> entry : JobStreamRun.getCustomProperties().entrySet())
			{
				conf.set(entry.getKey().toString(), entry.getValue().toString());
			}
			URI uri = URI.create(conf.get("fs.defaultFS"));
			FileSystem hdfs;
			hdfs = FileSystem.get(uri, conf);

			String dir = job.getConfiguration().get("infoDir") + "/MergeXXXXObject2Temp";
			System.out.println("****** dir:" + dir);
			Path path = new Path(dir);
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
						if (line.startsWith("reduce_NewData:")) {
							String item = line.substring("reduce_NewData:".length()).trim();
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

		return (int) Math.ceil(1.0 * total / 100000);
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

		reduceNum = getReduceNum(job);
		reduceNum = 20;
		System.out.println("****** reduceNum:" + reduceNum);		
		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
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
			String AccessionNumber = "";
			String Title = "";
			String Authors = "";
			String AuthorAffiliation = "";
			String CorrAuthorAffiliation = "";
			String CorrespondingAuthor = "";
			String SourceTitle = "";
			String PublicationYear = "";
			String Volume = "";
			String Issue = "";
			String Abstract = "";
			String CODEN = "";
			String ISSN = "";
			String EISSN = "";
			String ISBN13 = "";
			String DocumentType = "";
			String DOI = "";
			String ControlledTerms = "";
			String UncontrolledTterms = "";
			String MainHeading = "";
			String Pages = "";
			String ConferenceLocation = "";
			String IssueDate = "";
			String ConferenceDate = "";
			String Sponsor = "";
			String Publisher = "";
			String ei_cc = "0";
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Accession number")) {
					AccessionNumber = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Title")) {
					Title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Author")) {
					Authors = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Author affiliation")) {
					AuthorAffiliation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CorrAuthorAffiliation")) {
					CorrAuthorAffiliation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Corresponding author")) {
					CorrespondingAuthor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Source")) {
					SourceTitle = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Publication year")) {
					PublicationYear = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Volume")) {
					Volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Issue")) {
					Issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Abstract")) {
					Abstract = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CODEN")) {
					CODEN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ISSN")) {
					ISSN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("E-ISSN")) {
					EISSN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ISBN13")) {
					ISBN13 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Document type")) {
					DocumentType = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DOI")) {
					DOI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Controlled/Subject terms")) {
					ControlledTerms = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Uncontrolled terms")) {
					UncontrolledTterms = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Main Heading")) {
					MainHeading = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Pages")) {
					Pages = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Conference location")) {
					ConferenceLocation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Issue date")) {
					IssueDate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Conference date")) {
					ConferenceDate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Sponsor")) {
					Sponsor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Publisher")) {
					Publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ei_cc")) {
					ei_cc = updateItem.getValue().trim();
				}
			}
			
			
			String lngid = "ELL_" + AccessionNumber;
			String srcid = "ENGINEERINGVILLAGE";
			String title_e = Title.replace('\0', ' ').replace("'", "''").trim();			
			String showwriter = Authors.replace('(', '[').replace(')', ']');
			showwriter = showwriter.replace('\0', ' ').replace("'", "''").trim();
			String author_e = showwriter;
			String showorgan = "";			
			if (AuthorAffiliation.length() > 0) {
				showorgan = AuthorAffiliation.replace('(', '[').replace(')', ']');
			}
			else {
				showorgan = CorrAuthorAffiliation.replace('(', '[').replace(')', ']');
			}
			showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();
			String organ_e = showorgan;
			String wcorrespond = CorrespondingAuthor.replace('\0', ' ').replace("'", "''").trim();
			String media_e = SourceTitle.replace('\0', ' ').replace("'", "''").trim();
			String years = PublicationYear.replace('\0', ' ').replace("'", "''").trim();
			String vol = Volume.replace('\0', ' ').replace("'", "''").trim();
			String num = Issue.replace('\0', ' ').replace("'", "''").trim();
			String remark_e = Abstract.replace('\0', ' ').replace("'", "''").trim();
			String wissn = "";
			String weissn = "";
			if (ISSN.length() == 8) {
				wissn = ISSN.substring(0, 4) + "-" + ISSN.substring(4, 8);
			}
			else if (ISSN.length() > 0) {
				context.getCounter("map", "ISSN.length >0 !=8").increment(1);	
			}
			if (EISSN.length() == 8) {
				weissn = EISSN.substring(0, 4) + "-" + EISSN.substring(4, 8);
			}
			else if (EISSN.length() > 0){
				context.getCounter("map", "EISSN.length >0 !=8").increment(1);	
			}
			String language = "2";
			String type = "1";
			String titletype = "0;2;256;258";
			if (DocumentType.equals("Conference article (CA)") || DocumentType.equals("Conference proceeding (CP)")) {
				type = "3";		//会议
				titletype = "0;2;768;770";
			}
			else if (DocumentType.equals("Dissertation (DS)")) {
				type = "2";		//学位
				titletype = "0;2;512;514";
			}
			String range = "EI";
			String srcproducer = "EI";
			String wdatabase = "Compendex";
			String includeid = "[EI]" + AccessionNumber;
			String processdate = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
			String wdoi = DOI.replace('\0', ' ').replace("'", "''").trim();
			//String keyword_e = ControlledTerms.replace('\0', ' ').replace('-', ';').replaceAll("\\s+?", " ").replaceAll(" ?; ?", ";").replace("'", "''").trim();
			String keyword_e = ControlledTerms.replace('\0', ' ').replace(' ', ' ').replaceAll("\\s*-\\s*", ";").replace("'", "''").trim();	//注意有个特殊空白
			String subjects = MainHeading.replace('\0', ' ').replace("'", "''").trim();
			
			String pages = Pages.replace('\0', ' ').replace("'", "''").trim();
			String beginpage = "";
		    String endpage = "";
		    String[] vec = Pages.split("-");
		    if (vec.length < 2) {
				beginpage = endpage = Pages.replace('\0', ' ').replace("'", "''").trim();
			}
		    else {
				beginpage = vec[0].replace('\0', ' ').replace("'", "''").trim();
				endpage = vec[1].replace('\0', ' ').replace("'", "''").trim();
			}
		    
			String hymeetingplace = ConferenceLocation.replace('\0', ' ').replace("'", "''").trim();
			String hypressdate = IssueDate.replace('\0', ' ').replace("'", "''").trim();
			String hymeetingdate = ConferenceDate.replace('\0', ' ').replace("'", "''").trim();
			String hyhostorganization = Sponsor.replace('\0', ' ').replace("'", "''").trim();
			String hypressorganization = Publisher.replace('\0', ' ').replace("'", "''").trim();
			
		    //String strreftext = "";		//为空即可
			
			//统计
			{
				if (years.startsWith("201")) {
					context.getCounter("years", years).increment(1);
				}
			}
					   
			String sql = "INSERT INTO modify_title_info([lngid], [rawid], [srcid], [title_e], [showwriter], [author_e], [showorgan], [organ_e], [wcorrespond], [media_e], [years], [vol], [num], [remark_e], [wissn], [weissn], [type], [language], [titletype], [range], [srcproducer], [wdatabase], [includeid], [processdate], [wdoi], [keyword_e], [subjects], [pages], [beginpage], [endpage], [hymeetingplace], [hypressdate], [hymeetingdate], [hyhostorganization], [hypressorganization], [ei_cc]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, srcid, title_e, showwriter, author_e, showorgan, organ_e, wcorrespond, media_e, years, vol, num, remark_e, wissn, weissn, type, language, titletype, range, srcproducer, wdatabase, includeid, processdate, wdoi, keyword_e, subjects, pages, beginpage, endpage, hymeetingplace, hypressdate, hymeetingdate, hyhostorganization, hypressorganization, ei_cc);								
			
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
				logger.info("***** finalHdfsPath:" + finalHdfsPath);
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