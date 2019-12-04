package simple.jobstream.mapreduce.site.ei_zt;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.DocFlavor.STRING;

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
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2Db3.class);

	private static int reduceNum = 0;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "ei_zt";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		
		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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
	
		//格式化时间字符串到标准时间，如2017/1/1转换成20170101
		public static String formatDate(String date) {
			Map<String, String> monthMap = new HashMap<String, String>(){{
			    put("january", "01");
			    put("february", "02");
			    put("februaryy", "02");
			    put("march", "03");
			    put("april", "04");
			    put("may", "05");
			    put("june", "06");
			    put("july", "07");
			    put("august", "08");
			    put("september", "09");
			    put("october", "10");
			    put("november", "11");
			    put("december", "12");
			    put("jan", "01");
			    put("feb", "02");
			    put("mar", "03");
			    put("apr", "04");
			    put("aug", "08");
			    put("sept", "09");
			    put("oct", "10");
			    put("nov", "11");
			    put("dec", "12");
			    }};
			String year="1900";
			String month = "00";
			String day="00";
			date = date.replaceAll("[^0-9a-zA-Z ]","");
			
			String [] arr = date.split("\\s+");
			for(String ss : arr){
				String regyear = "^\\d{4}";
				if(ss.matches(regyear)){
					 year=ss;
					 continue;
				}
				String regday = "^\\d{1,2}";
				if(ss.matches(regday)){
					 day=ss;
					 continue;
				}
				ss = ss.toLowerCase();
				month=monthMap.get(ss);
			}
			if (year == "1900") {
				return "19000000";
			}
			if (month=="00" || month==null) {
				return year+"0000";
			}
			return year+month+day;
		}
		
		
		public static String[] splitpage(String str) {
			String jump="";
			String pagestart="";
			String pageend="";
			String[] listresult =new String[3];
			String[] listarray = str.split(",");
			if (listarray.length == 1) {
				jump="";
			}else {
				jump=listarray[1];
			}
			String page = listarray[0];
			String[] listpage = page.split("-");
			if (listpage.length == 2) {
				pagestart = listpage[0];
				pageend = listpage[1];
			}
			listresult[0] = pagestart;
			listresult[1] = pageend;
			listresult[2] = jump;
			return listresult;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
			String rawid = "";
			String AccessionNumber = "";
			String Title = "";
			String creator="";
			String creator_institution="";
			String creator_bio="";
			String source="";
			String publisher="";
			String volume="";
			String issue="";
			String page="";
			String date="";
			String provider_subject="";
			String date_created="";
			String ControlledTerms = "";
			String identifier_pissn="";
			String identifier_eissn="";
			String identifier_pisbn="";
			String identifier_doi="";
			String description="";
			String cited_cnt="";
			String batch="";
			String ref_cnt="";
			String DocumentType = ""; 
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Document type")) {
					DocumentType = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Accession number")) {
					AccessionNumber = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Title")) {
					Title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Controlled/Subject terms")) {
					ControlledTerms = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Author")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Author affiliation")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Corresponding author")) {
					creator_bio = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Source")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Volume")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Pages")) {
					page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Issue date")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Monograph title")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Publication year")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ISSN")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("E-ISSN")) {
					identifier_eissn = updateItem.getValue().trim();
				} 
				else if (updateItem.getKey().equals("ISBN13")) {
					identifier_pisbn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DOI")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Abstract")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ei_cc")) {
					cited_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DOWNDate")) {
					batch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Number of references")) {
					ref_cnt = updateItem.getValue().trim();
				}
				
			}
			String lngid = "ELL_" + AccessionNumber;
			String provider="eijournal";
			String type="3";
			String rawtype= DocumentType;
			if (date.isEmpty()) {
				date = "1900";
			}
			date_created = date_created.replace("Februaryy", "February").replace("Febr", "February");
			date_created = formatDate(date_created);
			if (date_created.isEmpty()){
				date_created = date + "0000";
			}
			if (DocumentType.equals("Conference article (CA)") || DocumentType.equals("Conference proceeding (CP)"))
			{ ////会议
				
				lngid = "EI_HY_" + AccessionNumber;
				provider="eiconference";
				type = "6";
				context.getCounter("map", "eiconference_count").increment(1);
			}
			else if (DocumentType.equals("Dissertation (DS)"))
			{
				//学位
				type = "4";		
				lngid = "EI_BS_" + AccessionNumber;
				provider="eithesis";
				context.getCounter("map", "eithesis_count").increment(1);
			}else {
				context.getCounter("map", "eijournal_count").increment(1);
			}
			String srcid = "ENGINEERINGVILLAGE";
			String medium="2";
			String country="US";
			String language="EN";
			String absurl="https://www.engineeringvillage.com/share/document.url?mid="+rawid+"&database=cpx&view=abstract";
			String provider_url=provider + '@' +absurl;
			String provider_id = provider + '@' + AccessionNumber;
			String[] arraypage =  splitpage(page);
			String beginpage = arraypage[0];
			String endpagepage = arraypage[1];
			String jumppage = arraypage[2];
			if (identifier_pissn.length() > 4) {
				StringBuffer sb = new StringBuffer();
				sb.append(identifier_pissn).insert(4, "-");
				identifier_pissn = sb.toString();
			}
			if (identifier_eissn.length() > 4) {
				StringBuffer sb = new StringBuffer();
				sb.append(identifier_eissn).insert(4, "-");
				identifier_eissn = sb.toString();
			}
			creator_institution = creator_institution.replace('(', '[').replace(")", "]").trim();
			creator = creator.replace('(', '[').replace(")", "]").trim();	
			String keyword_e = ControlledTerms.replace(' ', ' ').replaceAll("\\s*-\\s*", ";"); //注意有个特殊空白
			//转义
			{
				Title = Title.replace('\0', ' ').replace("'", "''").trim();	
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();	
				AccessionNumber = AccessionNumber.replace('\0', ' ').replace("'", "''").trim();	
				creator = creator.replace('\0', ' ').replace("'", "''").trim();	
				creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();	
				creator_bio = creator_bio.replace('\0', ' ').replace("'", "''").trim();	
				source = source.replace('\0', ' ').replace("'", "''").trim();	
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();	
				volume = volume.replace('\0', ' ').replace("'", "''").trim();	
				issue = issue.replace('\0', ' ').replace("'", "''").trim();	
				page = page.replace('\0', ' ').replace("'", "''").trim();	
				date = date.replace('\0', ' ').replace("'", "''").trim();	
				provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();
				identifier_eissn = identifier_eissn.replace('\0', ' ').replace("'", "''").trim();
				identifier_pisbn = identifier_pisbn.replace('\0', ' ').replace("'", "''").trim();
				description = description.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
				rawtype = rawtype.replace('\0', ' ').replace("'", "''").trim();
				keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").trim();
				beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
				endpagepage = endpagepage.replace('\0', ' ').replace("'", "''").trim();
				jumppage = jumppage.replace('\0', ' ').replace("'", "''").trim();
			}
			
			Pattern p = Pattern.compile("; (?!\\[)");
			Matcher m = p.matcher(creator_institution);
			//没什么用  想对比下替换前后
			String creator_discipline = creator_institution;
	        if (m.find()){
	            creator_institution = m.replaceAll(",");
	        }
	        
	        creator_institution = creator_institution.replaceAll("\\s+;", ";").replaceAll(";\\s+",";").replaceAll("]\\s+","]"); 	// 去掉分号前的空白
	        creator = creator.replaceAll("\\s+;", ";").replaceAll(";\\s+",";").replaceAll("]\\s+","]"); 
			
//	        if (provider.equals("eijournal")) {
//				String sql = "INSERT INTO modify_title_info_zt([source]) ";
//				sql += " VALUES ('%s');";
//				sql = String.format(sql, source);
//				context.getCounter("map", "count").increment(1);
//
//				context.write(new Text(sql), NullWritable.get());
//	        }else {
//	        	return ;
//	        }
	        
			String sql = "INSERT INTO modify_title_info_zt([lngid], [title],[creator],[creator_institution],[creator_bio],[source],[publisher],[volume],[issue],[page],[date_created],[date],[language],[identifier_pissn],[identifier_eissn],[identifier_pisbn],[identifier_doi],[type],[description],[provider],[cited_cnt],[batch],[ref_cnt],[medium],[country],[provider_url],[provider_id],[rawid],[rawtype],[subject],[beginpage],[endpage],[jumppage],[creator_discipline]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngid, Title, creator, creator_institution,creator_bio,source,publisher,volume,issue,page,date_created,date,language,identifier_pissn,identifier_eissn,identifier_pisbn,identifier_doi,type,description,provider,cited_cnt,batch,ref_cnt,medium,country,provider_url,provider_id, rawid,rawtype,keyword_e,beginpage,endpagepage,jumppage,creator_discipline);								

	        
//	        if (type != "3") {
//	        	return ;
//	        }
//	        String needyear = AccessionNumber.substring(0, 4);
//	        context.getCounter("map", "alljournal").increment(1);
//	        if (needyear.equals("2016")) {
//	        	context.getCounter("map", "2016").increment(1);
//	        	context.getCounter("map", "3yearsall").increment(1);
//	        }else if (needyear.equals("2017")) {
//	        	context.getCounter("map", "2017").increment(1);
//	        	context.getCounter("map", "3yearsall").increment(1);
//			}else if (needyear.equals("2018")) {
//	        	context.getCounter("map", "2018").increment(1);
//	        	context.getCounter("map", "3yearsall").increment(1);
//			}
	        
//	        return ;
	        
	        
	        
////			String sql = "INSERT INTO modify_title_info_zt([lngid],[batch],[provider],[date],[creator],[creator_institution]) ";
////			sql += " VALUES ('%s','%s','%s','%s','%s','%s');";
////			sql = String.format(sql, lngid,batch,provider,date,creator,creator_institution);		
//			
			
	        
//			String sql = "INSERT INTO modify_title_info_zt([rawid],[ref_cnt]) ";
//			sql += " VALUES ('%s','%s');";
//			sql = String.format(sql, AccessionNumber, ref_cnt);		
			
			
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