package simple.jobstream.mapreduce.site.mirrordqlibckrdjournal;

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
public class StdXXXXObject extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	

	
	public void pre(Job job) {
		
		String jobName = "mirrordqlibckrdjournal." + this.getClass().getSimpleName();

		job.setJobName(jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));

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
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String lngid = "";
			String rawid = "";
			String title = "";
			String creator_release = "";
			String issue = "";
			String language = "";
			String provider = "";
			String provider_id = "";
			String provider_url = "";
			String country = "";
			String identifier_pissn = "";
			String type = "";
			String source = "";
			String date_created = "";
			String gch = "";
			String publisher = "";
			String description_cycle = "";
			String date ="";
			String medium = "";
			String batch="";
			String provider_subject="";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("school_type")) {
					provider_subject = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator_release")) {
					creator_release = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_id")) {
					provider_id = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("types")) {
					type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("gch")) {
					gch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description_cycle")) {
					description_cycle = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("medium")) {
					medium = updateItem.getValue().trim();
				}
	
				
			}
			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator_release = creator_release.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();;
				   
			String sql = "insert into modify_title_info_zt(lngid,rawid,title,creator_release, description_cycle,issue,language,provider,provider_id,provider_url,country,batch,identifier_pissn,type,medium,date, source, date_created, gch,publisher,provider_subject)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, lngid,rawid,title,creator_release, description_cycle,issue,language,provider,provider_id,provider_url,country,batch,identifier_pissn,type,medium,date, source, date_created, gch,publisher,provider_subject);					
			
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}