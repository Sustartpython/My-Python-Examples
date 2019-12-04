package simple.jobstream.mapreduce.site.jstor_book;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.print.DocFlavor.STRING;

import org.apache.hadoop.fs.FSDataInputStream;
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

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2Db3.class);

	private static int reduceNum = 0;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "jstorbook";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
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
		job.setReducerClass(SqliteReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		
		job.setNumReduceTasks(reduceNum);
		
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		private static HashSet<String> pubNoSet =  new HashSet<String>();
		
		private static void initPubNoSet(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/jstor/qk/txt/20181118/delate.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					String pub_no = line.trim();
					pubNoSet.add(pub_no);
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("pubNoSet size:" + pubNoSet.size());
		}

		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initPubNoSet(context);
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		
		public static String[] splitpage(String str) {
			String pagestart="";
			String pageend="";
			String[] listresult =new String[2];
			if (str.indexOf("-") != -1 && !str.endsWith("-")) {
				String[] listarray = str.split("-");
				pagestart = listarray[0];
				pageend = listarray[1];
			}else {
				pagestart = "";
				pageend = "";
			}
			listresult[0] = pagestart;
			listresult[1] = pageend;
			return listresult;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			

			String rawid = "";
			String description = "";
			String publisher = "";
			String provider_subject="";
			String title="";
			String page="";
			String identifier_doi="";
			String date="";
			String date_created="";
			String country = "";
			String language="";
			String url="";
			String creator="";
			//系列
			String seriesstring = "";
			String eisbn = "";
			String parse_time = "";
			String down_date = "";
			String cover = "";
			String edition = "";
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_subject")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				} 
				else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("seriesstring")) {
					seriesstring = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("parse_time")) {
					parse_time = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("cover")) {
					cover = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("eisbn")) {
					eisbn = updateItem.getValue().trim();
				}
				else if(updateItem.getKey().equals("edition")){
					edition = updateItem.getValue().trim();
					
				}
			}
			String lngid = "JSTOR_TS_" + rawid;
			String provider="jstorbook";
			String type="1";
			String medium="2";

			String provider_url=provider + '@' +url;
			String provider_id = provider + '@' + rawid;
			
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyyMMdd");
			String batch = formatter.format(new Date());
			batch += "00";
			
			eisbn = eisbn.replace("-", "");

			//转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();	
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();	
				title = title.replace('\0', ' ').replace("'", "''").trim();	
				description = description.replace('\0', ' ').replace("'", "''").trim();	
				provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();	
				page = page.replace('\0', ' ').replace("'", "''").trim();	
				identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();	
				date = date.replace('\0', ' ').replace("'", "''").trim();
				date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				seriesstring = seriesstring.replace('\0', ' ').replace("'", "''").trim();
			}					   
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid],[description],[publisher],"
					+ "[provider_subject],[title],[pagecount],[identifier_doi],[date],"
					+ "[date_created],[country],[language],[provider_url],[creator],"
					+ "[title_series],[identifier_eisbn],[type],[medium],[batch],"
					+ "[provider_id],[provider],[cover],[title_edition]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s');";
			sql = String.format(sql, lngid, rawid, description,publisher, 
					provider_subject,title,page,identifier_doi,date,
					date_created,country,language,provider_url,creator,
					seriesstring,eisbn,type,medium,batch,
					provider_id,provider,cover,edition);								
						
			
					
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}