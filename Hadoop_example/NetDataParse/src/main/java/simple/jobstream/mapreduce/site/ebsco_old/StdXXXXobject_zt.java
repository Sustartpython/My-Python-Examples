package simple.jobstream.mapreduce.site.ebsco_old;

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
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
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
public class StdXXXXobject_zt extends InHdfsOutHdfsJobInfo {
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
//		
//		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
//		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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
			String title_alternative = "";
			String identifier_pissn="";
			String creator="";
			String creator_institution = "";
			String source = "";
			String description = "";
			String description_en = "";
			String subject ="";
			String language = "";
			String lang = "EN";
			String source_all= "";
			String if_html_fulltext= "";
			String if_pdf_fulltext= "";
			String ref_cnt= "";
			String rawtype= "";
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("db")) {
					db = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''");
				}
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("title_alternative")) {
					title_alternative = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim().split("\\◆")[0];
				}
				if (updateItem.getKey().equals("if_html_fulltext")) {
					if_html_fulltext = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim().split("\\◆")[0];
				}
				if (updateItem.getKey().equals("if_pdf_fulltext")) {
					if_pdf_fulltext = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim().split("\\◆")[0];
				}
				if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("raw_type")) {
					rawtype = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("source_all")) {
					source_all = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}
				if (updateItem.getKey().equals("description_en")) {
					description_en = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").trim();
				}

				if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				}
				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim().replace('\0', ' ').replace("'", "''").replace("◆", ";").trim();
				
					lang = langShort.get(language.toLowerCase());
					if (lang == null){
						lang = "EN";
					}

				}

			}
			
			String lngid = "";
			String provider = "";
			String rawidString = "";
			rawidString =  rawid;
			
			if (db.equals("a9h")){
				lngid = VipIdEncode.getLngid("00098", rawid, false);
				provider = "ebscoa9hjournal";
				context.getCounter("map", "a9h").increment(1);	
				
			}else if(db.equals("bth")){
				lngid = VipIdEncode.getLngid("00099", rawid, false);
				provider = "ebscobthjournal";
				context.getCounter("map", "bth").increment(1);
		
			}else if(db.equals("aph")){
				lngid = VipIdEncode.getLngid("00100", rawid, false);
				provider = "ebscoaphjournal";
				context.getCounter("map", "aph").increment(1);
		
			}else if(db.equals("buh")){
				lngid = VipIdEncode.getLngid("00101", rawid, false);
				provider = "ebscobuhjournal";
				context.getCounter("map", "buh").increment(1);
		
			}else{
				context.getCounter("map", db).increment(1);
				return;
			}
			
			String jounal_date ="";
			String vol = "";
			String issue = "";
			String page = "";
			String jounal = "";
			String date = "";
			String beginpage= "";
			String endpage= "";
			String pagecount= "";
			
			//匹配页码page
			Pattern pattern = Pattern.compile("\\sp\\S{0,}-{0,1}\\S{0,}");
			Matcher matcher = pattern.matcher(source_all);
			if(matcher.find()){
				if (matcher.group(0).trim().equals("preceding")) {
					if(matcher.find()){
						page = matcher.group(0);
					}
				}
				else if (matcher.group(0).trim().equals("pN.PAG-N.PAG.")) {
					page = "";
				}else {
					page = matcher.group(0);
				}
			}else{
				page = "";
			}
			//匹配pagecount
			pattern = Pattern.compile("[0-9]+p");
			matcher = pattern.matcher(source_all);
			if(matcher.find()){
				pagecount = matcher.group(0).replace("p", "");
				
			}else{
				pagecount = "";
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
				issue = matcher.group(0);
			}else{
				issue = "";
				
			}
			
			//匹配刊名和日期

			if(vol != ""){
				pattern = Pattern.compile(".{0,}\\sVol.");
			}else if(issue != ""){
				pattern = Pattern.compile(".{0,}\\sIssue");
			}else if(page != ""){
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
					date = jounal_date.replaceAll(".{0,}\\.", "");
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
			issue = issue.replace("Issue", "").trim();
			page = page.trim();
			pagecount=pagecount.trim();
			
			date = date.trim();
			if(page.length()>1){
				page = page.substring(1,page.length()-1);
			}
			if(date.length()>1){
				date = date.substring(0, date.length()-1);
			}
			
			//date 规范化 date_created
			String year = "1900";
			String month ="00";
			String day = "00";
			String pa = "\\d{4}";
			String date_created = "";
			Pattern r = Pattern.compile(pa);
			Matcher mt = r.matcher(date);
			if (mt.find()) {
				year = mt.group(0);
			}
			
			//  12/18/2014
			pa = "\\d{1,2}\\/\\d{1,2}\\/\\d{4}";
			r = Pattern.compile(pa);
			mt = r.matcher(date);
			if(mt.find()){
				month = date.split("\\/")[0];

				day = date.split("\\/")[1];
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
				month = getMapValueByKey(date);
			}
			
			date_created = year + month + day;
			date = year;			
			//
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			
			String country = "US";
			country = nationMap.get(lang);
			
			String type = "3";
			
			//处理作者和机构
			creator = creator.replaceAll("AUTHOR", "").replaceAll("\\](?!;)", "\\];").replaceAll(" \\(\\);", "").replaceAll(";$", "").replaceAll("; \\(\\)", "");
			creator_institution = creator_institution.replaceAll(";(?!\\[)",",");
			
			String medium = "2";
			String provider_id = provider + "@" + rawidString;
			String provider_url = provider + "@http://search.ebscohost.com/login.aspx?direct=true&db=" + db + "&AN=" + rawid +"&lang=zh-cn&site=ehost-live";
			//生成beginpage，endpage
			int count = page.length()-page.replace("-", "").length();
			if ( count==1 && !page.equals("-1")) {
				beginpage=page.split("-")[0];
				if (page.split("-").length>1) {
					endpage=page.split("-")[1];
				} else {
					endpage="";
				}
			}
			if (page.equals("N.PAG")) {
				page="";
			}
			
			
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid], [identifier_doi],  [title], [title_alternative], [identifier_pissn], [creator], [creator_institution], [source], [volume], [issue], [description], [description_en], [subject], [page], [batch], [date] ,[provider], [language],[country] ,[type],[provider_url],[medium], [provider_id], [date_created],[beginpage],[endpage],[if_html_fulltext],[if_pdf_fulltext],[pagecount],[ref_cnt],[rawtype]) ";
			sql += " VALUES ('%s', '%s','%s', '%s', '%s','%s','%s','%s', '%s', '%s', '%s', '%s', '%s','%s',  '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');";
			sql = String.format(sql, lngid, rawidString, doi, title, title_alternative,identifier_pissn, creator, creator_institution, jounal,  vol, issue, description, description_en ,subject, page,  batch ,date, provider,lang,country,type,provider_url, medium, provider_id, date_created,beginpage,endpage,if_html_fulltext,if_pdf_fulltext,pagecount,ref_cnt,rawtype);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());			
		}
	}

}