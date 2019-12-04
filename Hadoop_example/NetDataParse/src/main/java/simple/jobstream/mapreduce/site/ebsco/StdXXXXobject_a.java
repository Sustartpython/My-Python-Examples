package simple.jobstream.mapreduce.site.ebsco;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;

import javax.xml.transform.Source;

import java.util.Map;
import java.util.HashMap;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
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
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdXXXXobject_a extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdXXXXobject_zt.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "ebscojournal";
	

	
	public void pre(Job job) {
		String jobName = "Ebsco." + this.getClass().getSimpleName();

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
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		 job.setOutputValueClass(BytesWritable.class);
		 JobConfUtil.setTaskPerReduceMemory(job, 6144);
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
		private static Map<String, String> langShort = new HashMap<String,String>();
		private static Map<String, String> nationMap = new HashMap<String, String>();

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
		
		private static void InitLangShortMap(){
			langShort.put("spanish", "ES");
			langShort.put("french", "FR");
			langShort.put("german", "DE");
			langShort.put("chinese", "ZH");
			langShort.put("portuguese", "PT");
			langShort.put("croatian", "HR");
			langShort.put("russian", "RU");
			langShort.put("italian", "IT");
			langShort.put("japanese", "JA");

		}
		private static void InitNationMap(){
			nationMap.put("EN", "US");
			nationMap.put("ES", "ES");
			nationMap.put("FR", "FR");
			nationMap.put("DE", "DE");
			nationMap.put("ZH", "CN");
			nationMap.put("PT", "PT");
			nationMap.put("HR", "HR");
			nationMap.put("RU", "RU");
			nationMap.put("IT", "IT");
			nationMap.put("JA", "JP");
		}
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMonthmonthMap();
			InitLangShortMap();
			InitNationMap();
			
			
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
				//获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		        FSDataOutputStream fout = null;
		        String pathfile = "/user/dgy/log/" + nowDate + ".txt";
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
		        	fout = fs.create(new Path(pathfile));
		        }
		        out = new BufferedWriter(new OutputStreamWriter(fout, "UTF-8"));
			    out.write(text);
			    out.close();    
			} catch (Exception ex){
				bException = true;
			}
			
			if (bException){
				return false;
			}
			else {
				return true;
			}
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
				
			String rawid = "";
			String db = "";
			String doi="";
			String title = "";
			String title_alt = "";
			String issn="";
			String author="";
			String author_1st="";
			String organ = "";
			String organ_1st="";
			String title_series = "";
			String abstract_ = "";//关键字冲突，加下划线以区分
			String abstract_alt = "";
			String language = "EN";
			String raw_type= "";
			String subject= "";
			String source_all= "";
			String batch= "";
			String subject_word= "";
			String email= "";
			String corr_author= "";
			String pub_place= "";
			String ref_cnt= "";
			String keyword= "";
			
			batch  = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("db")) {
					db = updateItem.getValue().trim();
				}
//				if (updateItem.getKey().equals("pub_place")) {
//					pub_place = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
//				}
				if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("title_alternative")) {
					title_alt = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim().split("\\◆")[0];
				}
				if (updateItem.getKey().equals("identifier_pissn")) {
					issn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("creator")) {
					author = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("corr_author")) {
					corr_author = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("email")) {
					email = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				
				if (updateItem.getKey().equals("creator_institution")) {
					organ = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				
				if (updateItem.getKey().equals("source_all")) {
					source_all = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("description")) {
					abstract_ = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("description_en")) {
					abstract_alt = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}

				if (updateItem.getKey().equals("subject")) {
					keyword = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("subject_word")) {
					subject_word = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				
					language = langShort.get(language.toLowerCase());
					if (language == null){
						language = "EN";
					}

				}

			}
			
			String lngid = "";
			String provider = "";
			String rawidString = "";
			String sub_db_id = "";
			String product = "";
			String sub_db = "";
			String down_date= "";
			String source_type= "3";
			
			
			rawidString = rawid;
			
//	        down_date = (new SimpleDateFormat("yyyyMMdd")).format(new Date());// 获取当前时间 
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			Calendar c = Calendar.getInstance();
			c.setTime(new Date());
			c.add(Calendar.DATE, -2);
			Date start = c.getTime();
			down_date= format.format(start);
	        
			if (db.equals("a9h")){
				sub_db_id="00098";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
				product="EBSCOHOST";
				sub_db="A9H";
				provider = "EBSCO";
				context.getCounter("map", "a9h").increment(1);	
				
			}else if(db.equals("bth")){
				sub_db_id="00099";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
				product="EBSCOHOST";
				sub_db="BTH";
				provider = "EBSCO";
				context.getCounter("map", "bth").increment(1);
		
			}else if(db.equals("aph")){
				sub_db_id="00100";
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
				product="EBSCOHOST";
				sub_db="APH";
				provider = "EBSCO";
				context.getCounter("map", "aph").increment(1);
		
			}else if(db.equals("buh")){
				lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
				sub_db_id="00101";
				product="EBSCOHOST";
				sub_db="BUH";
				provider = "EBSCO";
				context.getCounter("map", "buh").increment(1);
		
			}else{
				context.getCounter("map", db).increment(1);
				return;
			}
			
			String jounal_date ="";
			String vol = "";
			String num = "";
			String page_info = "";
			String jounal = "";
			String pub_year = "";
			String begin_page= "";
			String end_page= "";
			
			//匹配页码page
			Pattern pattern = Pattern.compile("\\sp\\S{0,}-{0,1}\\S{0,}");
			Matcher matcher = pattern.matcher(source_all);
			if(matcher.find()){
				if (matcher.group(0).trim().equals("preceding")) {
					if(matcher.find()){
						page_info = matcher.group(0);
					}
				}
				else if (matcher.group(0).trim().equals("pN.PAG-N.PAG.")) {
					page_info = "";
				}else {
					page_info = matcher.group(0);
				}
			}else{
				page_info = "";
			}
			
			
			//匹配Vol
			
			pattern = Pattern.compile("Vol.\\s\\d{0,}");
			matcher = pattern.matcher(source_all);
			if(matcher.find()){
				vol = matcher.group(0);
			}else{
				vol = "";
			}
			
			//匹配ISSUE
			pattern = Pattern.compile("Issue\\s\\d{0,}");
			matcher = pattern.matcher(source_all);
			if(matcher.find()){
				num = matcher.group(0);
			}else{
				num = "";
				
			}
			
			//匹配刊名和日期

			if(vol != ""){
				pattern = Pattern.compile(".{0,}\\sVol.");
			}else if(num != ""){
				pattern = Pattern.compile(".{0,}\\sIssue");
			}else if(page_info != ""){
				pattern = Pattern.compile(".{0,}\\sp\\S{0,}-{0,1}\\S{0,}");

			}
			matcher = pattern.matcher(source_all);
			if(matcher.find()){
				 jounal_date = matcher.group(0);
				 jounal_date = jounal_date.replaceAll("\\sp\\S{0,}-{0,1}\\S{0,}", "").replaceAll("\\sVol\\.", "").replaceAll("\\sIssue", "");
				 pattern = Pattern.compile(".{0,}\\.");
				 matcher = pattern.matcher(jounal_date);
				 if(matcher.find()){
					jounal = matcher.group(0);
					pub_year = jounal_date.replaceAll(".{0,}\\.", "");
				 }else{
					jounal = "";
				 }
			}else{
				jounal_date = "";
			}
			if(jounal.length()>1){
				
				jounal = jounal.substring(0, jounal.length()-1);
			}
			vol = vol.replace("Vol.", "").trim();
			num = num.replace("Issue", "").trim();
			page_info = page_info.trim();
			pub_year = pub_year.trim();
			if(page_info.length()>1){
				page_info = page_info.substring(1,page_info.length()-1);
			}
			if(pub_year.length()>1){
				pub_year = pub_year.substring(0, pub_year.length()-1);
			}
			
			//date 规范化 date_created
			String year = "1900";
			String month ="00";
			String day = "00";
			String pa = "\\d{4}";
			String pub_date = "";
			String journal_name= "";
			Pattern r = Pattern.compile(pa);
			Matcher mt = r.matcher(pub_year);
			if (mt.find()) {
				year = mt.group(0);
			}
			
			//  12/18/2014
			pa = "\\d{1,2}\\/\\d{1,2}\\/\\d{4}";
			r = Pattern.compile(pa);
			mt = r.matcher(pub_year);
			if(mt.find()){
				month = pub_year.split("\\/")[0];

				day = pub_year.split("\\/")[1];
				if(year.length()!=4){
					year = "1900";
				}
				
				if(month.length()<2){
					month = "0" + month;
				}
				if(day.length()<2){
					day = "0" + day;
				}
				
				
			}else{
				month = getMapValueByKey(pub_year);
			}
			
			pub_date = year + month + day;
			pub_year = year;			
			
			
			String country = "US";
			country = nationMap.get(language);
			
			
			//处理作者和机构
			author = author.replaceAll("AUTHOR", "").replaceAll("\\](?!;)", "\\];").replaceAll(" \\(\\);", "").replaceAll(";$", "").replaceAll("; \\(\\)", "");
			//author_1st
			if (author != null) {
				if (author.contains(";")) {
					if (author.split(";").length >1) {
						author_1st=author.split(";")[0].replace("[1]", "").replace("[1,2]", "");
					}
				} else {
					author_1st=author.replace("[1]", "").replace("[1,2]", "");
				}
			}
			organ = organ.replaceAll(";(?!\\[)",",");
			//organ_1st
			if (organ != null) {
				if (organ.contains(";")) {
					if (organ.split(";").length >1) {
						organ_1st=organ.split(";")[0].replace("[1]", "");
					}
					
				} else {
					organ_1st=organ.replace("[1]", "");
				}
			}
			
			title_series=jounal;
			String provider_url ="http://search.ebscohost.com/login.aspx?direct=true&db=" + db + "&AN=" + rawid +"&lang=zh-cn&site=ehost-live";
			journal_name=title_series;
			
			//生成begin_page，end_page
			int count = page_info.length()-page_info.replace("-", "").length();
			if ( count==1 && !page_info.equals("-1")) {
				begin_page=page_info.split("-")[0];
				end_page=page_info.split("-")[1];
			}
			if (page_info.equals("N.PAG")) {
				page_info="";
			}
			
			String sql = "INSERT INTO base_obj_meta_a([rawid],[doi],[title],[title_alt],[issn],[author],[author_1st],[organ],[organ_1st],[title_series],[abstract],[abstract_alt],[subject_word],[language],[raw_type],[lngid],[provider],[sub_db_id],[product],[sub_db],[down_date],[source_type],[vol],[num],[page_info],[pub_year],[pub_date],[batch],[country],[down_date],[provider_url],[journal_name],[begin_page],[end_page],[email],[corr_author],[pub_place],[ref_cnt],[keyword]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, rawidString,doi,title,title_alt,issn,author,author_1st,organ,organ_1st,title_series,abstract_,abstract_alt,subject_word,language,raw_type,lngid,provider,sub_db_id,product,sub_db,down_date,source_type,vol,num,page_info,pub_year,pub_date,batch,country,down_date,provider_url,journal_name,begin_page,end_page,email,corr_author,pub_place,ref_cnt,keyword);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());			
		}
	}

}