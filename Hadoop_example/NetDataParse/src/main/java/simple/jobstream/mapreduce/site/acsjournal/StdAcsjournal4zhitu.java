package simple.jobstream.mapreduce.site.acsjournal;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdAcsjournal4zhitu extends InHdfsOutHdfsJobInfo {

	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	// 特殊处理文件,通过馆藏号gch在该文件中查找相应的pissn和eissn
	public static String ref_file_path = "/RawData/acsjournal/big_json/supplement/supplement.txt";

	public void pre(Job job) {
		String jobName = "acsjournal." + this.getClass().getSimpleName();
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

		// 初始化补充issn的字典<jid,pissn&eissn>
		public static HashMap<String, String> supplementMap = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException, InterruptedException {
			JournalMapIssn(context); //注意这里的初始化
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}


		// 特殊处理方法
		public static void JournalMapIssn(Context context) throws IOException {
			FileSystem fs = FileSystem.get(context.getConfiguration());

			Path path = new Path(ref_file_path);
			FSDataInputStream is = fs.open(path);
			// get the file info to create the buffer
			FileStatus stat = fs.getFileStatus(path);

			// create the buffer
			byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
			is.readFully(0, buffer);

			// 处理text传过来的字符串
			String supplementString = new String(buffer);
			String[] supp_string = supplementString.split(";");
			for (String ss : supp_string) {
				String supp_key = ss.split(":")[0];
				String supp_value = ss.split(":")[1];
				supplementMap.put(supp_key, supp_value);
			}
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
			
			String lngID = "";
			String rawid = "";
			String sub_db_id = "00022";
			String product = "acs";
			String provider = "acsjournal";
			String down_date = "";
			String parse_time = "";
			String doi = "";
			String source_type = "3";
			String provider_url = "";
			String title = "";
			String keyword = "";
			String description = "";
			String begin_page = "";
			String end_page = "";
			String raw_type = "";
			String recv_date = "";
			String accept_date = "";
			String pub_date = "";
			String pub_date_alt = "";
			String author = "";
			String organ = "";
			String journal_id = "";
			String journal_name = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String issn = "";
			String eissn = "";
			String publisher = "";
			String country = "US";
			String language = "EN";
			
			String medium = "2";
			String gch = "";
			String batch = "";
			String provider_id = "";
			String page = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("parse_time")) {
					parse_time = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					begin_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					end_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("recv_date")) {
					recv_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("accept_date")) {
					accept_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("pub_date_alt")) {
					pub_date_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_id")) {
					journal_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					journal_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("page")) {
					page = updateItem.getValue().trim();
				}
			}
			
			if (title.length() < 2) {
				return;
			}

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyyMMdd");
			String nowDate = df.format(dt);
			batch = nowDate + "00";

			// 传issn和eissn
			if (supplementMap.containsKey(journal_id)) {
				String[] value_string = supplementMap.get(journal_id).split("&");
				if(value_string.length>3) {
					issn = value_string[1];
					eissn = value_string[3];
				}
			}
			
			//处理description
			if(description.startsWith("View: PDF")) {
				description="";
			}

			//处理作者机构编号
			String[] result = new String[2];
			try {
				result = AuthorOrgan.renumber(author, organ);
				author = result[0];
				organ = result[1];
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			
					
			gch = provider + "@" + journal_id;
			provider_url = provider + "@" + provider_url;
			provider_id = provider + "@" + rawid;
			
			//新版lngid,sub_db_id由邱虹阳给出
			
			lngID = VipIdEncode.getLngid(sub_db_id, rawid ,false);

			
			down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
			doi = doi.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
			issn = issn.replace('\0', ' ').replace("'", "''").trim();
			eissn = eissn.replace('\0', ' ').replace("'", "''").trim();
			author = author.replace('\0', ' ').replace("'", "''").trim();
			organ = organ.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			pub_year = pub_year.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
			vol = vol.replace('\0', ' ').replace("'", "''").trim();
			num = num.replace('\0', ' ').replace("'", "''").trim();
			pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();
			begin_page = begin_page.replace('\0', ' ').replace("'", "''").trim();
			end_page = end_page.replace('\0', ' ').replace("'", "''").trim();
			journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim();
			parse_time = parse_time.replace('\0', ' ').replace("'", "''").trim();
			batch = batch.replace('\0', ' ').replace("'", "''").trim();
			provider_id = provider_id.replace('\0', ' ').replace("'", "''").trim();
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
			lngID = lngID.replace('\0', ' ').replace("'", "''").trim();
			provider = provider.replace('\0', ' ').replace("'", "''").trim();
			country = country.replace('\0', ' ').replace("'", "''").trim();
			language = language.replace('\0', ' ').replace("'", "''").trim();
			source_type = source_type.replace('\0', ' ').replace("'", "''").trim();
			medium = medium.replace('\0', ' ').replace("'", "''").trim();
			gch = gch.replace('\0', ' ').replace("'", "''").trim();
			page = page.replace('\0', ' ').replace("'", "''").trim();

//			down_date, identifier_doi, title, title_series, identifier_pissn, creator, creator_institution, 
//			publisher, date, description, subject, volume, issue, date_created, page, beginpage, endpage,pagecount,source,parse_time ,
//			batch, provider_id, provider_url, lngID, provider, country, language, type, medium

			String sql = "insert into modify_title_info_zt(rawid,identifier_doi,title,rawtype,identifier_pissn,identifier_eissn,creator,"
					+ "creator_institution,publisher,date,description,subject,volume,issue,date_created,page,beginpage,"
					+ "endpage,source,batch,provider_id,provider_url,lngID,provider,country,language,type,medium,gch)";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, rawid, doi, title, raw_type, issn, eissn,
					author, organ, publisher, pub_year, description, keyword, vol, num, pub_date,
					page, begin_page, end_page, journal_name, batch, provider_id, provider_url, lngID, provider,
					country, language, source_type, medium, gch);

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());
		}
	}

}