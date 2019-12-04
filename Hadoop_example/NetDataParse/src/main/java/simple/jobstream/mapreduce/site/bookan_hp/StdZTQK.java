package simple.jobstream.mapreduce.site.bookan_hp;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.math.BigInteger;
import java.text.DateFormat;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import javax.ws.rs.core.NewCookie;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.binary.Base64;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;
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
import com.google.gson.Gson;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdZTQK extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZTQK.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
//	private static String postfixDb3 = "gxbookanjournal";
//	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	public void pre(Job job) {
		String jobName = job.getConfiguration().get("jobName");
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
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
		job.setReducerClass(SqliteReducer.class);

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
		private static Set<String> setFilename = new HashSet<String>();		//获取-9状态的filename
		public void setup(Context context) throws IOException,
				InterruptedException {
			// 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        initSetFilename(fs);		
		}
		
		private static void initSetFilename(FileSystem fs) throws IOException {
			FSDataInputStream fin = fs.open(new Path("/RawData/cnki/bz/ref_file/filename.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 2) {
						continue;
					}
		        	setFilename.add(line);        	
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }	        
	        System.out.println("setFilename size:" + setFilename.size());
	
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
		        String pathfile = "/tangmao/log/log_map/" + nowDate + ".txt";
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
		
		//获得馆藏号，通过转换source为base32编码
		public static String Convert2B32(String toEncodeContent) throws UnsupportedEncodingException{
			if(toEncodeContent == null){
				return null;
			}
			
			Base32 encoderBase32 = new Base32();			
			
			return encoderBase32.encodeToString(toEncodeContent.getBytes("utf-8")).replace("=", "9").toLowerCase();
			
		}
		
		//格式化时间字符串到标准时间，如2017/1/1转换成20170101
		public static String formatDate(String date) {
			String new_date = "";
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyy/MM/dd");
			Date fotmatDate;
			try {
				fotmatDate = formatter.parse(date);
				formatter = new SimpleDateFormat ("yyyyMMdd"); 
				new_date = formatter.format(fotmatDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return new_date;
		}
		
		public static String getJsonStrByKV(String Key, List<String> Value) {
			Gson gson = new Gson();
			HashMap<String, List<String>> tmpMap = new HashMap<String, List<String>>();  
			tmpMap.put(Key,Value);
			return gson.toJson(tmpMap);
		}
		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
//			String rawid = "";
//			String id = "";
//			String page = "";
//			String url = "";
//			String listid = "";
//			String source = "";
//			String title = "";
//			String issue = "";
//			String post_code = "";
//			String date_created = "";
			
			
			//标题
	    	String title = "";
			//issn (在json里是isbn字段)
	    	String issn = "";
			//出版社
	    	String publisher = "";
	    	//出版日期
	    	String pub_date = "";
	    	//文献类型
	    	String source_type = "3";
	    	String down_date = "";
	    	String provider_url = "";
	    	//页码
	    	String page_info = "";
	    	//
	    	String raw_type = "";
	    	//栏目信息
	    	String column_info = "";
	    	//期刊刊名id
	    	String journal_raw_id = "";
	    	//期刊名
	    	String journal_name = "";
	    	//出版年
	    	String pub_year = "";
	    	//期
	    	String num = "";
	    	//图片地址
	    	String cover_path = "";
	    	//国家
	    	String country = "CN";
	    	//语言
	    	String language = "ZH";
	    	
	    	String rawid = "";
	    	
	    	String sub_db_id = "00031";
	    	
	    	String product = "CTGUBOOKAN";
	    	
	    	String sub_db = "QK";
	    	
	    	String provider = "BOOKAN";
	    	
	    	String batch = "";

			
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("source_type")) {
					source_type = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("column_info")) {
					column_info = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("journal_raw_id")) {
					journal_raw_id = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("journal_name")) {
					journal_name = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("cover_path")) {
					cover_path = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				}
			}
			
			
			provider = "ctgubookanjournal";
			String type = "3";
			String medium = "2";
			String provider_id = provider + "@" + rawid;
			provider_url = provider + "@" + provider_url;
			String date = pub_year;
			String date_created = pub_date;	
			String issue = num;
	        String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
	        
			String gch = provider + "@" + journal_raw_id;// Convert2B32(source + post_code);
			String page = page_info;
			String source = journal_name;
			
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyyMMdd");
			batch = formatter.format(new Date());
			batch += "00";
			
			//转义
			{	
//				id = id.replace('\0', ' ').replace("'", "''").trim();
//				listid = listid.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();	
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();	
				page = page.replace('\0', ' ').replace("'", "''").trim();	
				column_info = column_info.replace('\0', ' ').replace("'", "''").trim();	
			}
			
			
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid], [title], [beginpage],[page], [source], [issue], [date], [date_created], "
					+ "[gch], [language], [country], [provider], [provider_url], [provider_id], [type], [medium], [batch],[title_series],[publisher],[identifier_eissn])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');";
			sql = String.format(sql, lngid, rawid, title, page, page, source, issue, date, date_created, gch, language, country, provider, provider_url, 
					provider_id, type, medium, batch,column_info,publisher,issn);	
		
			
			sql = "INSERT INTO modify_title_info_zt([lngid],[batch],[provider],[date],[type],[medium],[date_created])";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngid,batch,provider,date,type,medium,date_created);
			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
			
		}
	}
}