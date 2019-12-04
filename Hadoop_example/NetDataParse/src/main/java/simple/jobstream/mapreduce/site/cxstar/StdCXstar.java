package simple.jobstream.mapreduce.site.cxstar;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdCXstar extends InHdfsOutHdfsJobInfo {
	
	private static boolean testRun = false;
	private static int testReduceNum = 2;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
		
	public void pre(Job job) {
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		
		
		
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
		        String pathfile = "/venter/mirror_chaoxing/log/log_map/" + nowDate + ".txt";
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
		    String provider="cxstarbook";
		    String provider_url="";
		    String provider_id="";
		    String country="CN";
		    String cover="";
		    
		    String title_edition=""; //版本说明
		    String date_created="";
			String language = "ZH";
			String type = "1";
			String medium = "2";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pisbn")) {
					identifier_pisbn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_series")) {
					title_series = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_clc")) {
					subject_clc = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_subject")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
			}
			rawid = key.toString();			

			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
			url = "http://www.cxstar.com/Book/Detail?ruid=" + rawid;
			provider_url = provider + "@" + url;
			provider_id = provider + "@" + rawid;
			lngID = "CXSTAR_TS_" + rawid;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			else {
				date = "1900";
				date_created = "19000000";
			}
			
			cover = "/smartlib/" + provider + "/" + rawid.substring(0,3).toLowerCase() + "/" + rawid.toLowerCase() + ".jpg";

			//date_created = date + "0000";
			
			//转义sql字符
//			title = StringEscapeUtils.escapeSql(title.replaceAll("《","").replaceAll("》","")).trim();//标题
//			title_alternative = StringEscapeUtils.escapeSql(title_alternative.replaceAll("《","").replaceAll("》","")).trim();//标题
//			creator = StringEscapeUtils.escapeSql(creator.replaceAll("；",";").replaceAll("，",";").replaceAll("; ", ";").replaceAll(", ",";").replaceAll(",","")).trim();//作者
			//identifier_pisbn = StringEscapeUtils.escapeSql(identifier_pisbn).trim();//isbn号

//			subject=StringEscapeUtils.escapeSql(subject);//关键字
//			description = StringEscapeUtils.escapeSql(description).trim();//内容提要
//			provider_subject = StringEscapeUtils.escapeSql(provider_subject).trim();
			//publishers = StringEscapeUtils.escapeSql(publishers).trim();
			
			creator = creator.replace("，", ";");
			
			if (description.equals("暂无简介")) {
				description = "";
			}
			
			subject_clc = subject_clc.replace(':', ';').replace('：', ';');
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			title_series = title_series.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			subject_clc = subject_clc.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			
			
					   
			String sql = "insert into modify_title_info_zt(lngid,rawid,cover,title,creator,identifier_pisbn,subject,description,date,date_created,publisher,title_series,subject_clc,provider_subject,page,language,country,provider,provider_url,provider_id,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,cover,title,creator,identifier_pisbn,subject,description,date,date_created,publisher,title_series,subject_clc,provider_subject,page,language,country,provider,provider_url,provider_id,type,medium,batch);					
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
			
		}
	}
}