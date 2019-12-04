package simple.jobstream.mapreduce.site.WOS;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Collections;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
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

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;


public class Update2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Update2Db3.class);
	
	private static String postfixDb3 = "wos_update";
	private static String tempFileDb3 = "/RawData/WOS/rel_file/wos_template_update.db3";

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

	public void SetMRInfo(Job job) {
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
		private  static Map<String, String> mapMonth =new HashMap<String, String>();
		private  static Map<String, String> mapLib =new HashMap<String, String>();
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMapMonth();
			initMapLib();
		}
		
		private static void initMapMonth() {
			mapMonth.put("JAN", "01");
			mapMonth.put("FEB", "02");
			mapMonth.put("MAR", "03");
			mapMonth.put("APR", "04");
			mapMonth.put("MAY", "05");
			mapMonth.put("JUN", "06");
			mapMonth.put("JUL", "07");
			mapMonth.put("AUG", "08");
			mapMonth.put("SEP", "09");
			mapMonth.put("OCT", "10");
			mapMonth.put("NOV", "11");
			mapMonth.put("DEC", "12");
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
		
		private static Map<Integer, String> getWriterMap(String C1){
			Map<Integer, String> writerMap = new HashMap<Integer, String>();
			
	        List<String> ls = new ArrayList<String>();
	        //Pattern pattern = Pattern.compile("(?<=\\[)(.+?)(?=\\])");
	        Pattern pattern = Pattern.compile("\\[(.+?)\\]");
	        Matcher matcher = pattern.matcher(C1);
	        int idx = 0;
	        while(matcher.find()) {
	        	ls.add(matcher.group(1));
	        	writerMap.put(++idx, matcher.group(1));
	        }
	        /*    
	        for (Map.Entry<Integer, String> entry : writerMap.entrySet()) { 
	        	System.out.println(entry.getKey() + "***" + entry.getValue());
			}
	        */
	        return writerMap;
	    }
		
		private static String getShowWriter(String AF, String C1) {
			String showwriter = "";
			
			Map<Integer, String> writerMap = getWriterMap(C1);
			
			List<Integer> idxList = new ArrayList<Integer>();
			for (String writer : AF.split(";")) {
				writer = writer.trim();
				idxList.clear();		//清空list
				for (Map.Entry<Integer, String> entry : writerMap.entrySet()) { 
					for (String writerX : entry.getValue().split(";")) {
						writerX = writerX.trim();
						if (writer.equals(writerX)) {
							idxList.add(entry.getKey());
							break;
						}
					}
				}
				
				Collections.sort(idxList);		//索引排序
				String idxString = StringUtils.join(idxList, ',');
				//System.out.println("idxString:" + idxString);
				if (idxString.length() > 0) {
					showwriter += writer + "[" + idxString + "];";
				}
				else {
					showwriter += writer + ";";
				}
			}
			showwriter = showwriter.replaceAll(";+$", "");
			
			return showwriter;
		}
		
		private static String getShowOrgan(String C1) {
			String showorgan = "";

			C1 = C1.replaceAll("\\[.+?\\]", "");		//去掉中括号
			if (C1.indexOf(';') < 0) {		//一个机构或空机构
				showorgan = C1.trim();
			}
			else {
				int idx = 0;
				for (String organ : C1.split(";")) {
					organ = organ.trim();
					showorgan +=  "[" + (++idx) + "]" + organ + ";";
				}
			}
			showorgan = showorgan.replaceAll(";+$", "");
			
			return showorgan;
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

			String CA = "";
			String BA = "";
			String DE = "";
			String BE = "";
			String HO = "";
			String EM = "";
			String ALL = "";
			String UT = "";
			String GA = "";
			String SC = "";
			String C1 = "";
			String PY = "";
			String MA = "";
			String LA = "";
			String D2 = "";
			String AU = "";
			String CY = "";
			String SE = "";
			String PG = "";
			String TC = "";
			String WC = "";
			String BS = "";
			String DI = "";
			String PI = "";
			String OI = "";
			String SO = "";
			String FU = "";
			String TI = "";
			String SI = "";
			String RI = "";
			String PU = "";
			String AB = "";
			String NR = "";
			String PT = "";
			String RP = "";
			String SP = "";
			String AF = "";
			String BF = "";
			String CL = "";
			String BN = "";
			String U2 = "";
			String EI = "";
			String ID = "";
			String SU = "";
			String PN = "";
			String IS = "";
			String VL = "";
			String PD = "";
			String JI = "";
			String J9 = "";
			String BP = "";
			String AR = "";
			String FX = "";
			String CR = "";
			String PA = "";
			String GP = "";
			String SN = "";
			String EP = "";
			String U1 = "";
			String Z9 = "";
			String CT = "";
			String DT = "";
			String Condition = "";
			String LIBName = "";

			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("CA")) {
					CA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BA")) {
					BA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DE")) {
					DE = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BE")) {
					BE = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("HO")) {
					HO = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("EM")) {
					EM = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ALL")) {
					ALL = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("UT")) {
					UT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("GA")) {
					GA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SC")) {
					SC = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("C1")) {
					C1 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PY")) {
					PY = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("MA")) {
					MA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("LA")) {
					LA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("D2")) {
					D2 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AU")) {
					AU = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CY")) {
					CY = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SE")) {
					SE = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PG")) {
					PG = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("TC")) {
					TC = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("WC")) {
					WC = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BS")) {
					BS = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DI")) {
					DI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PI")) {
					PI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("OI")) {
					OI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SO")) {
					SO = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("FU")) {
					FU = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("TI")) {
					TI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SI")) {
					SI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("RI")) {
					RI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PU")) {
					PU = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AB")) {
					AB = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("NR")) {
					NR = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PT")) {
					PT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("RP")) {
					RP = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SP")) {
					SP = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AF")) {
					AF = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BF")) {
					BF = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CL")) {
					CL = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BN")) {
					BN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("U2")) {
					U2 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("EI")) {
					EI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ID")) {
					ID = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SU")) {
					SU = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PN")) {
					PN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("IS")) {
					IS = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("VL")) {
					VL = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PD")) {
					PD = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("JI")) {
					JI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("J9")) {
					J9 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("BP")) {
					BP = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AR")) {
					AR = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("FX")) {
					FX = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CR")) {
					CR = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("PA")) {
					PA = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("GP")) {
					GP = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SN")) {
					SN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("EP")) {
					EP = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("U1")) {
					U1 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Z9")) {
					Z9 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CT")) {
					CT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DT")) {
					DT = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Condition")) {
					Condition = updateItem.getValue().trim();
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
			
			String rawid = UT;
			String lngid = "WKL_" + UT.substring(4);
			String language = "2";
			String type = "1";
			String titletype = "0;2;256;258";			
			if ((LIBName.indexOf("ISTP") > -1) || (LIBName.indexOf("ISSHP") > -1)) {
				type = "3";
				titletype = "0;2;768;770";				
				context.getCounter("map", "type-3-hy").increment(1);
			}
			
			String srcid = "WEBOFKNOWLEDGE";
			
			String title_e = TI.replace('\0', ' ').replace("'", "''").trim();
			String media_e = SO.replace('\0', ' ').replace("'", "''").trim();
			String showwriter = getShowWriter(AF, C1).replace('\0', ' ').replace("'", "''").trim();
			String author_e = showwriter;
			String showorgan =  getShowOrgan(C1).replace('\0', ' ').replace("'", "''").trim();
			String organ_e = showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();
			String keyword_e = DE.replace('\0', ' ').replace("'", "''").trim();
			String remark_e = AB.replace('\0', ' ').replace("'", "''").trim();
			String years = PY.replace('\0', ' ').replace("'", "''").trim();
			String wmonth = PD.replace('\0', ' ').replace("'", "''").trim();
			String month = "01";		
			String cleanMonth = PD.toUpperCase().replaceAll("[^A-Z]", ""); 	
			if(mapMonth.containsKey(cleanMonth)) {
				month = mapMonth.get(cleanMonth);
			}
			String publishdate = years + month + "01";
			String vol = VL.replace('\0', ' ').replace("'", "''").trim();
			String num = IS.replace('\0', ' ').replace("'", "''").trim();
			String range = getRangeByLibName(LIBName);
			String includeid = getIncludeID(range, UT);
			
		    String beginpage = BP.replace('\0', ' ').replace("'", "''").trim();
		    String endpage = EP.replace('\0', ' ').replace("'", "''").trim();
		    String pagecount = PG.replace('\0', ' ').replace("'", "''").trim();
		    String wdoi = DI.replace('\0', ' ').replace("'", "''").trim();
		    String wissn = SN.replace('\0', ' ').replace("'", "''").trim();
		    String strreftext = "";		//为空即可
		    
		    String af = AF.replace('\0', ' ').replace("'", "''").trim();
		    String c1 = C1.replace('\0', ' ').replace("'", "''").trim();
		    String wos_tc = TC.replace('\0', ' ').replace("'", "''").trim();
		    String  wos_z9 = Z9.replace('\0', ' ').replace("'", "''").trim();
					   
			String sql = "INSERT INTO modify_title_info([lngid], [wos_tc], [wos_z9], [range], [includeid]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, wos_tc, wos_z9, range, includeid);
							
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