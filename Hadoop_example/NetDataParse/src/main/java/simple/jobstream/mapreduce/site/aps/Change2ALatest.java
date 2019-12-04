package simple.jobstream.mapreduce.site.aps;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//输入应该为去重后的html
public class Change2ALatest extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Change2ALatest.class);
	
	private static String postfixDb3 = "apsjournal";
	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		String jobName = "StdAPS";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
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
		//job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		SequenceFileOutputFormat.setCompressOutput(job, false);

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {
		private  static Map<String, String> mapSource =new HashMap<String, String>();
		
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMapSource();
			
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}
		
		private static void initMapSource() {
			mapSource.put("apsjournal@pra", "Physical Review A");
			mapSource.put("apsjournal@prab", "Physical Review Accelerators and Beams");
			mapSource.put("apsjournal@prapplied", "Physical Review Applied");
			mapSource.put("apsjournal@prb", "Physical Review B");
			mapSource.put("apsjournal@prc", "Physical Review C");
			mapSource.put("apsjournal@prd", "Physical Review D");
			mapSource.put("apsjournal@pre", "Physical Review E");
			mapSource.put("apsjournal@prfluids", "Physical Review Fluids");
			mapSource.put("apsjournal@prl", "Physical Review Letters");
			mapSource.put("apsjournal@prmaterials", "Physical Review Materials");
			mapSource.put("apsjournal@prper", "Physical Review Physics Education Research");
			mapSource.put("apsjournal@prx", "Physical Review X");
			mapSource.put("apsjournal@rmp", "Reviews of Modern Physics");
			mapSource.put("apsjournal@ppf", "Physics Physique Fizika");
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
		        String pathfile = "/lqx/log/log_map/" + nowDate + ".txt";
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
			
			String url = "";
				
			String title = "";//标题
//			String title_alternative="";
			String creator = "";//作者
			String publisher = "";//发行商
			String identifier_pisbn = "";//isbn号
			String date="";//出版日期
			String creator_institution="";//机构
			String description = "";//内容提要
			String provider_subject = "";
			String rawid = "";//
			String subject = "";
			String title_series = "";
			String subject_clc = "";
			String page = "";
			
			
            String source = "";
            String identifier_pissn = "";
            String identifier_eissn = "";

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider_url="";
		    String provider_id="";
		    String country="US";
		    String cover="";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "EN";
			String type = "3";
			String medium = "2";
			
			String beginpage = "";
			String endpage = "";
			String volume = "";
			String issue = "";
			String identifier_doi = "";
			
			String product = "APS";
			String sub_db = "QK";
			String provider = "APS";
			String sub_db_id = "00145";
			String source_type = "3";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
					creator = creator.replaceAll("\\p{Z}+", " ");
				}
				else if (updateItem.getKey().equals("beginpage")) {
					beginpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("endpage")) {
					endpage = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("insitution")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				}
			}
			rawid = key.toString();

			publisher = "American Physical Society";

			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";

			provider_id = provider + "@" + rawid;
			String journal_raw_id = url.replace("https://journals.aps.org/", "").split("/")[0];
			gch = "apsjournal@" + journal_raw_id;
			
			source = mapSource.get(gch);
			
			lngID = VipIdEncode.getLngid(sub_db_id,rawid,false);
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			else {
				date = "1900";
				date_created = "19000000";
			}
			
			if (!beginpage.equals("")) {
				if (endpage.equals("")) {
					endpage = beginpage;
				}
				page = beginpage + "-" + endpage;
			}
			
			// 中信所更新a层（大于2015年）信息
			if (Integer.parseInt(date) < 2015) {
				return;
			}
			
			
						

			
		
			
			XXXXObject xObjout = new XXXXObject();
			
			xObjout.data.put("lngid",lngID);
			xObjout.data.put("rawid",rawid);
			xObjout.data.put("product",product);
			xObjout.data.put("sub_db",sub_db);
			xObjout.data.put("provider",provider);
			xObjout.data.put("sub_db_id",sub_db_id);
			xObjout.data.put("source_type",source_type);
			xObjout.data.put("provider_url",url);
			xObjout.data.put("country",country);
			xObjout.data.put("language",language);
			xObjout.data.put("title",title);
			xObjout.data.put("author",creator);
			xObjout.data.put("organ",creator_institution);
			xObjout.data.put("pub_date",date_created);
			xObjout.data.put("pub_year",date);
			xObjout.data.put("publisher",publisher);
			xObjout.data.put("begin_page",beginpage);
			xObjout.data.put("end_page",endpage);
			xObjout.data.put("page_info",page);
			
			xObjout.data.put("journal_raw_id",journal_raw_id);
			xObjout.data.put("journal_name",source);
			xObjout.data.put("abstract",description);
			xObjout.data.put("issn",identifier_pissn);
			xObjout.data.put("eissn",identifier_eissn);
			xObjout.data.put("vol",volume);
			xObjout.data.put("num",issue);
			xObjout.data.put("keyword",subject);
			xObjout.data.put("doi",identifier_doi);

			xObjout.data.put("down_date","20190713");
			xObjout.data.put("batch","20190723_171426");			
			

			
			byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(key, new BytesWritable(bytes));
			
			
					   
				
	
			
		}
	}
}