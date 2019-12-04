package simple.jobstream.mapreduce.site.govceiinfo;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdGovceiinfo4zhitu extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "govceiinfo." + this.getClass().getSimpleName();
//		String jobName = job.getConfiguration().get("jobName");
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

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		
		// 记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt = new Date();// 如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");// 设置显示格式
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示

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
				} else {
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
			} else {
				return true;
			}
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String down_date = "";
			String title = "";
			String date = "";
			String description = "";
			String date_created = "";
			String lngID = "";
			String batch = "";
			String provider = "govceiinfo";
			String provider_url = "";
			String provider_id = "";
			String country = "CN";
			String language = "ZH";
			String type = "14";
			String medium = "2";
			String rawid = "";
			String url= "";
			String provider_subject = "";
			String referCode = "";
			String columnId = "";
			String description_source = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider_subject")) {
					provider_subject = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("referCode")) {
					referCode = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("columnId")) {
					columnId = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("description_source")) {
					description_source = updateItem.getValue().trim();
				}
			}
			
			if (title.length() < 2) {
				return;
			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			if(columnId.equals("4028c7ca-37115425-0137-11560b62-0046")) {
				provider_subject = "综合频道;总编时评";
			}else if(columnId.equals("4028c7ca-37115425-0137-1156093f-002d")) {
				provider_subject = "宏观频道;主编点评";
			}else if(columnId.equals("4028c7ca-37115425-0137-11560a1a-0039")) {
				provider_subject = "金融频道;主编点评";
			}else if(columnId.equals("4028c7ca-37115425-0137-1156097e-0030")) {
				provider_subject = "行业频道;主编点评";
			}else if(columnId.equals("4028c7ca-37115425-0137-11560671-0015")) {
				provider_subject = "区域频道;主编点评";
			}else if(columnId.equals("4028c7ca-37115425-0137-11560817-0024")) {
				provider_subject = "国际频道;主编点评";
			}
			
			
			provider_url = provider + "@" + url;
			provider_id = provider + "@" + rawid;
			lngID = "GOV_CEI_ZX_" + rawid;
			
			
			down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			date = date.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			date_created = date_created.replace('\0', ' ').replace("'", "''").trim();
			lngID = lngID.replace('\0', ' ').replace("'", "''").trim();
			batch = batch.replace('\0', ' ').replace("'", "''").trim();
			provider = provider.replace('\0', ' ').replace("'", "''").trim();
			provider_id = provider_id.replace('\0', ' ').replace("'", "''").trim();
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
			country = country.replace('\0', ' ').replace("'", "''").trim();
			language = language.replace('\0', ' ').replace("'", "''").trim();
			type = type.replace('\0', ' ').replace("'", "''").trim();
			medium = medium.replace('\0', ' ').replace("'", "''").trim();
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			batch = batch.replace('\0', ' ').replace("'", "''").trim();
			url = url.replace('\0', ' ').replace("'", "''").trim();
			provider_subject = provider_subject.replace('\0', ' ').replace("'", "''").trim();
			columnId = columnId.replace('\0', ' ').replace("'", "''").trim();
			referCode = referCode.replace('\0', ' ').replace("'", "''").trim();
			description_source = description_source.replace('\0', ' ').replace("'", "''").trim();

//			down_date,title,date,description,date_created,
//			lngID,batch,provider,provider_url,provider_id,
//			country,language,type,medium,rawid,
//			parse_time,url,provider_subject,referCode,columnId			
			
			
			String sql = "insert into modify_title_info_zt(title,date,description,date_created,"
					+ "lngID,batch,provider,provider_url,provider_id,"
					+ "country,language,type,medium,rawid,"
					+ "provider_subject,description_source)";
			sql += " VALUES ('%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s');";
			sql = String.format(sql,title,date,description,date_created,
					lngID,batch,provider,provider_url,provider_id,
					country,language,type,medium,rawid,
					provider_subject,description_source);

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
		}
	}

}