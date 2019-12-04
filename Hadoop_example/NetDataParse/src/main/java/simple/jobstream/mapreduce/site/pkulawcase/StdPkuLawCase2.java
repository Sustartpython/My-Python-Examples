package simple.jobstream.mapreduce.site.pkulawcase;

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
public class StdPkuLawCase2 extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
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

		job.setNumReduceTasks(reduceNum);
		
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
				
			String creator ="";
            String title = "";
            String date_created = "";
            String subject="";
            String description="";
            String creator_release="";
            String identifier_standard="";
            String date_impl="";
            String description_type="";
            String subject_dsa="";
            String contributor="";
            String legal_status="";
            String provider_subject="";
            String agents="";
            String agency="";
            String title_edition="";
            String source="";
			

		    String lngID = "";
		    String batch="";
		    String gch="";
		    String provider="pkucaselaw";
		    String provider_url="";
		    String provider_id="";
		    String country="CN";
		    String date="1900";
		    
			String language = "ZH";
			String type = "8";
			String medium = "2";
			
			String db = "";
			String gid = "";
			String rawid="";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("db")) {
					db = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("gid")) {
					gid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator_release")) {
					creator_release = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_dsa")) {
					subject_dsa = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_standard")) {
					identifier_standard = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_subject")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_impl")) {
					date_impl = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("agents")) {
					agents = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("agency")) {
					agency = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_edition")) {
					title_edition = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
			}
			if (title.length()<2) {
				return;
			}
			rawid = key.toString();
		
			
			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate+"00";
			
			
			url = String.format("http://www.pkulaw.cn/case/%s_%s.html?match=Exact", db, gid);
			provider_url = provider + "@" + url;
			provider_id = provider + "@" + rawid;
			lngID = "PKU_LAW_CASE_" + rawid;
			if (!date_created.equals("")) {
				date = date_created.substring(0, 4).trim();
			}
			else {
				if (!date_impl.equals("")) {
					date = date_impl.substring(0, 4).trim();
					date_created = date + "0000";
				}
				else {
					date = "1900";
					date_created = "19000000";
					date_impl = "19000000";
				}		
			}
			if (date_impl.equals("")) {
				date_impl = date + "0000";;
			}
									

			
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator_release = creator_release.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			subject_dsa = subject_dsa.replace('\0', ' ').replace("'", "''").trim();
			identifier_standard = identifier_standard.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();
			contributor = contributor.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			agents = agents.replace('\0', ' ').replace("'", "''").trim();
			agency = agency.replace('\0', ' ').replace("'", "''").trim();
			title_edition = title_edition.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			
			
					   
			String sql = "insert into modify_title_info_zt(lngid,rawid,creator_release,description,title,subject_dsa,identifier_standard,subject,contributor,provider_subject,date_impl,agents,agency,date,date_created,title_edition,source,language,country,provider,provider_url,provider_id,type,medium,batch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngID,rawid,creator_release,description,title,subject_dsa,identifier_standard,subject,contributor,provider_subject,date_impl,agents,agency,date,date_created,title_edition,source,language,country,provider,provider_url,provider_id,type,medium,batch);					
			
			context.getCounter("map", "count").increment(1);
			//String lineOutput = AccessionNumber + "\t" + Authors + "\t" + AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}