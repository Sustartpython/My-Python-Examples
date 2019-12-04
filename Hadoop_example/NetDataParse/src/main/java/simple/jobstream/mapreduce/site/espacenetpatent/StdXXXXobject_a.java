package simple.jobstream.mapreduce.site.espacenetpatent;

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
	private static String postfixDb3 = "espacenetpatent";
	

	
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
			//成品数据字段
			String rawid = "";
			String pub_date = "";
			String down_date = "";
			String batch = "";
			String sub_db_id = "sub_db_id";
			String product = "product";
			String sub_db = "sub_db";
			String provider = "provider";
			String lngid = "";
			String source_type = "7";
			String provider_url = "";
			String title = "";
			String title_alt= "";
			String abstract_= "";  //摘要
			String author= "";
			String author_1st= "";
			String pub_year= "";
			String country= "";
			String language= "";
			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("url")) {
					provider_url = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("title_alternative")) {
					title_alt = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("description")) {
					abstract_ = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("date")) {
					pub_year = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("creator")) {
					author = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
			}
			
//	        down_date = (new SimpleDateFormat("yyyyMMdd")).format(new Date());// 获取当前时间 
			SimpleDateFormat format = new SimpleDateFormat("yyyyMMdd");
			Calendar c = Calendar.getInstance();
			c.setTime(new Date());
			c.add(Calendar.DATE, -2);
			Date start = c.getTime();
			down_date= format.format(start);
			batch  = (new SimpleDateFormat("yyyyMMdd_HHmmss")).format(new Date());
			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			if (author.contains(";")) {
				if (author.split(";").length>1) {
					author_1st=author.split(";")[0];
				}
			} else {
				author_1st=author;
			}
	        
			
			
			String sql = "INSERT INTO base_obj_meta_a([rawid],[pub_date],[down_date],[batch],[sub_db_id],[product],[sub_db],[provider],[lngid],[source_type],[provider_url],[title],[title_alt],[abstract],[author],[author_1st],[pub_year],[country],[language]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, rawid,pub_date,down_date,batch,sub_db_id,product,sub_db,provider,lngid,source_type,provider_url,title,title_alt,abstract_,author,author_1st,pub_year,country,language);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());			
		}
	}

}