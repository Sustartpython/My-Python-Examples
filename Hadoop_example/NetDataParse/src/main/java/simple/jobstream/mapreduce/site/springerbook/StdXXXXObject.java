package simple.jobstream.mapreduce.site.springerbook;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
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
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdXXXXObject extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdXXXXObject.class);
	
	private static boolean testRun = true;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/springer/springerbook/ref_file/20190717/bookid20190715.txt";
	private static String postfixDb3 = "springerbook";
	

	
	public void pre(Job job) {
		String jobName = "springerbook." + this.getClass().getSimpleName();

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

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		public static Set<String> isbnSet = new HashSet<String>();
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			
			InitBookidSet(context);
			
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
		        String pathfile = "/user/qianjun/log/log_map/" + nowDate + ".txt";
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
		
		public static void InitBookidSet(Context context) throws IOException{
	        FileSystem fs = FileSystem.get(context.getConfiguration());

	        Path path = new Path(ref_file_path);
            FSDataInputStream is = fs.open(path);
            // get the file info to create the buffer
            FileStatus stat = fs.getFileStatus(path);
            
            // create the buffer
            byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
            is.readFully(0, buffer);
            
            String coveridString = new String(buffer);
            
			for (String co: coveridString.split("★")){
				isbnSet.add(co.trim());
			}
		}
		
		

		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String title = "";
			String title_sub = "";
			String title_series = "";
			String identifier_eisbn = "";
			String identifier_pisbn = "";
			String identifier_pissn = "";
			String identifier_doi = "";
			String creator = "";
			String creator_institution = "";
			String source_fl = "";
			String publisher = "";
			String date = "";
			String date_created = "";
			String volume = "";
			String description = "";
			String subject = "";
			String subject_dsa = "";
			
			
			String lngid = "";
			String rawid = "";

			String cover = "";

			String language = "EN";
			String country = "US";
			String provider = "springerbook";
			String provider_url = "";
			String provider_id = "";
			String type = "1";
			String medium = "2";
			String batch = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_sub")) {
					title_sub = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_series")) {
					title_series = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pisbn")) {
					identifier_pisbn = updateItem.getValue().trim().replace("-", "");
				}
				
				else if (updateItem.getKey().equals("identifier_eisbn")) {
					identifier_eisbn = updateItem.getValue().trim().replace("-", "");
				}

				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim().replace(" [", "[");
				}

				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				
				else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				}
				
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}

				else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				}else if(updateItem.getKey().equals("subject_dsa")){
					subject_dsa = updateItem.getValue().trim();
				}else if(updateItem.getKey().equals("identifier_pissn")){
					identifier_pissn = updateItem.getValue().trim();
				}
				
			}
			
			
			
			 rawid = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
			 lngid = "SPRINGER_TS_" + rawid;
			 title  = title.replace('\0', ' ').replace("'", "''").trim();
			 title_sub = title_sub.replace('\0', ' ').replace("'", "''").trim();
			 title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			 creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			 
			 creator = creator.replace('\0', ' ').replace("'", "''").trim();
			 publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			 description = description.replace('\0', ' ').replace("'", "''").trim();
			 subject = subject.replace('\0', ' ').replace("'", "''").trim();
			 title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			 if (date.length() < 1){
				 
				 date = "1900";
			 }
			 date_created = date + "0000";

			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
		    provider_url = provider + "@https://link.springer.com/book/" +rawid;
			provider_id = provider + "@" + rawid;
			
			


			if (isbnSet.contains(identifier_eisbn)){
				cover = "/smartlib/springerbook/" + identifier_eisbn.substring(0,7) + "/" + identifier_eisbn + ".jpg";
			}

	  
			String sql = "INSERT INTO modify_title_info_zt([lngid],[identifier_doi],[rawid],[title_sub],[title],[title_series],[identifier_eisbn],[identifier_pisbn],[creator],[creator_institution],[publisher],[date],[description],[subject],[type],[language],[country],[provider],[batch],[provider_url],[medium],[date],[provider_id],[cover], [volume],[date_created], [subject_dsa], [identifier_pissn]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s','%s','%s');";
			sql = String.format(sql, lngid,identifier_doi, rawid, title_sub, title, title_series,identifier_eisbn, identifier_pisbn, creator, creator_institution,publisher, date,  description, subject, type, language, country, provider, batch, provider_url, medium,date,provider_id,cover,volume,date_created,subject_dsa,identifier_pissn);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}