package simple.jobstream.mapreduce.user.walker.EI;

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

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import com.process.frame.util.SimpleTextInputFormat;

//输入应该为去重后的html
public class ParseDetail4EI extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(ParseDetail4EI.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
	public static final String inputHdfsPath = "/RawData/EI/big_htm/big_htm_20160612/1940.big_htm.gz";
	public static final String outputHdfsPath = "/RawData/EI/product";
	

	
	public void pre(Job job) {
		String jobName = "ParseDetail4EI";
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
		job.setInputFormatClass(SimpleTextInputFormat.class);
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
			Mapper<LongWritable, Text, Text, NullWritable> {
		
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
		
		public void map(LongWritable key, Text value, Context context)
				throws IOException, InterruptedException {
			
			Document doc = Jsoup.parse(value.toString());
			
			Elements eles = doc.select("input[name=\"docidlist\"]");
			if (eles.size() != 1) {
				context.getCounter("map", "docidlist eles.size() != 1").increment(1);	
				return;
			}
			String rawid = eles.get(0).attr("value");
			if (!rawid.startsWith("cpx_")) {
				context.getCounter("map", "rawid not startsWith cpx").increment(1);	
				return;
			}

			//Elements eles = doc.select("table[xmlns:pid]"); 		//这个地方不同批次有问题
			eles = doc.getElementsByTag("table");
			Element table = null;
			for (Element ele: eles) {
				context.getCounter("map", ele.parent().tagName()).increment(1);	   
				if (ele.parent().tagName() == "td") {
					table = ele;
				}
			}

			if (table == null) {		//找不到内层table
				context.getCounter("map", "table == null").increment(1);	   
				return;
			}
							
			String AccessionNumber = "";
			String Title = "";
			String Authors = "";
			String AuthorAffiliation = "";
			String CorrAuthorAffiliation = "";
			String CorrespondingAuthor = "";
			String SourceTitle = "";
			String Volume = "";
			String Issue = "";
			String Abstract = "";
			String ISSN = "";
			String EISSN = "";
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
			for (Element eleTr : table.getElementsByTag("tr")) {
				String text = eleTr.text();
				if (text.startsWith("Accession number:")) {
					AccessionNumber = text.substring("Accession number:".length()).trim();
				}
				else if (text.startsWith("Title:")) {
					Title = text.substring("Title:".length()).trim();
				}
				else if (text.startsWith("Authors:")) {
					eleTr = eleTr.html(eleTr.html().replace("<sup>", "[").replace("</sup>", "]"));	//替换sup为中括号
					Authors = eleTr.text().substring("Authors:".length()).trim();	//去掉前缀					
					Authors = Authors.replaceAll("\\s", " ");	//精简空白
					Authors = Authors.replace(" ;", ";");		//去掉分号前的空格
					Authors = Authors.replace("[]", "");	//去掉空的中括号
					Authors = Authors.trim();
				}
				else if (text.startsWith("Author affiliation:")) {
					for (Element eleTab : eleTr.getElementsByTag("table")) {	//一行机构一个table
						eleTab = eleTab.html(eleTab.html().replace("<sup>", "[").replace("</sup>", "]"));	//替换sup为中括号
						AuthorAffiliation += eleTab.text().replace(';', ',') + ";";		//替换单个机构里面的分号为逗号
					}	
					AuthorAffiliation = AuthorAffiliation.replaceAll("\\s", " ");	//精简空白
					AuthorAffiliation = AuthorAffiliation.replace("] ", "]"); 	//去掉标号后的空格
					AuthorAffiliation = AuthorAffiliation.trim();
				}
				else if (text.startsWith("Corr. author affiliation:")) {
					CorrAuthorAffiliation = text.substring("Corr. author affiliation:".length()).trim();
				}
				else if (text.startsWith("Corresponding author:")) {
					CorrespondingAuthor = text.substring("Corresponding author:".length()).trim();				
				}
				else if (text.startsWith("Source title:")) {
					SourceTitle = text.substring("Source title:".length()).trim();		
				}
				else if (text.startsWith("Volume:")) {
					Volume = text.substring("Volume:".length()).trim();		
				}
				else if (text.startsWith("Issue:")) {
					Issue = text.substring("Issue:".length()).trim();		
				}
				else if (text.startsWith("Abstract:")) {
					Abstract = text.substring("Abstract:".length()).trim();	
				}
				else if (text.startsWith("ISSN:")) {
					ISSN = text.substring("ISSN:".length()).trim();		
				}
				else if (text.startsWith("E-ISSN:")) {
					EISSN = text.substring("E-ISSN:".length()).trim();	
				}
				else if (text.startsWith("Document type:")) {
					DocumentType = text.substring("Document type:".length()).trim();	
				}
				else if (text.startsWith("DOI:")) {
					DOI = text.substring("DOI:".length()).trim();	
				}
				else if (text.startsWith("Controlled terms:")) {
					ControlledTerms = text.substring("Controlled terms:".length()).trim();	
				}
				else if (text.startsWith("Uncontrolled terms:")) {
					UncontrolledTterms = text.substring("Uncontrolled terms:".length()).trim();	
				}
				else if (text.startsWith("Main heading:")) {
					MainHeading = text.substring("Main heading:".length()).trim();	
				}
				else if (text.startsWith("Pages:")) {
					Pages = text.substring("Pages:".length()).trim();	
				}
				else if (text.startsWith("Conference location:")) {
					ConferenceLocation = text.substring("Conference location:".length()).trim();	
				}
				else if (text.startsWith("Issue date:")) {
					IssueDate = text.substring("Issue date:".length()).trim();	
				}
				else if (text.startsWith("Conference date:")) {
					ConferenceDate = text.substring("Conference date:".length()).trim();	
				}
				else if (text.startsWith("Sponsor:")) {
					Sponsor = text.substring("Sponsor:".length()).trim();	
				}
				else if (text.startsWith("Publisher:")) {
					Publisher = text.substring("Publisher:".length()).trim();	
				}
			}
						
			String lngid = "ELL_" + AccessionNumber;
			String srcid = "ENGINEERINGVILLAGE";
			String title_e = Title.replace('\0', ' ').replace("'", "''").trim();			
			String showwriter = Authors.replace('\0', ' ').replace("'", "''").trim();
			String showorgan = "";			
			if (AuthorAffiliation.length() > 0) {
				showorgan = AuthorAffiliation.replace('\0', ' ').replace("'", "''").trim();
			}
			else {
				showorgan = CorrAuthorAffiliation.replace('\0', ' ').replace("'", "''").trim();
			}
			String wcorrespond = CorrespondingAuthor.replace('\0', ' ').replace("'", "''").trim();
			String media_e = SourceTitle.replace('\0', ' ').replace("'", "''").trim();
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
			String keyword_e = ControlledTerms.replace('\0', ' ').replace('-', ';').replaceAll("\\s+?", " ").replaceAll(" ?; ?", ";").replace("'", "''").trim();
			String subjects = MainHeading.replace('\0', ' ').replace("'", "''").trim();
			String pages = Pages.replace('\0', ' ').replace("'", "''").trim();
			String hymeetingplace = ConferenceLocation.replace('\0', ' ').replace("'", "''").trim();
			String hypressdate = IssueDate.replace('\0', ' ').replace("'", "''").trim();
			String hymeetingdate = ConferenceDate.replace('\0', ' ').replace("'", "''").trim();
			String hyhostorganization = Sponsor.replace('\0', ' ').replace("'", "''").trim();
			String hypressorganization = Publisher.replace('\0', ' ').replace("'", "''").trim();
			
		    //String strreftext = "";		//为空即可
					   
			String sql = "INSERT INTO modify_title_info([lngid], [rawid], [srcid], [title_e], [showwriter], [showorgan], [wcorrespond], [media_e], [vol], [num], [remark_e], [wissn], [weissn], [type], [language], [titletype], [range], [srcproducer], [wdatabase], [includeid], [processdate], [wdoi], [keyword_e], [subjects], [pages], [hymeetingplace], [hypressdate], [hymeetingdate], [hyhostorganization], [hypressorganization]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, srcid, title_e, showwriter, showorgan, wcorrespond, media_e, vol, num, remark_e, wissn, weissn, type, language, titletype, range, srcproducer, wdatabase, includeid, processdate, wdoi, keyword_e, subjects, pages, hymeetingplace, hypressdate, hymeetingdate, hyhostorganization, hypressorganization);								
			
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
				
				String tempPathFile = "/vipuser/walker/input/template/ei_template.db3";
				String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + "ei.db3";
				Path src = new Path(tempPathFile);	//模板文件（HDFS路径）
				Path dst = new Path(db3PathFile);	//local路径
				hdfs.copyToLocalFile(src, dst);
				File crcFile = new File(baseDir.getAbsolutePath() + File.separator + "." +  taskId + "_" + "ei.db3.crc");
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