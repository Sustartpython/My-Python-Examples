package simple.jobstream.mapreduce.site.hkmolib;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.File;
import java.io.FileInputStream;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.io.UnsupportedEncodingException;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.Map;
import java.util.HashMap;

import org.apache.commons.codec.binary.Base32;
import org.apache.commons.codec.digest.DigestUtils;
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
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import simple.jobstream.mapreduce.common.util.StringHelper;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//输入应该为去重后的html
public class hkmolib_db extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(hkmolib_db.class);

	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	private static String postfixDb3 = "hkmolib_db";

	public void pre(Job job) {
		String jobName = "hkmolib_db.StdXXXXObject";
		

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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);
		job.setOutputValueClass(BytesWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);
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

		public void setup(Context context) throws IOException,
				InterruptedException {

		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {

		}
		public static String encodeID(String strRaw) {
			Base32 base32 = new Base32();
			String strEncode = "";
			try {
				strEncode = base32.encodeAsString(strRaw.getBytes("utf8"));
				if (strEncode.endsWith("======")) {
					strEncode = strEncode.substring(0, strEncode.length() - 6) + "0";
				} else if (strEncode.endsWith("====")) {
					strEncode = strEncode.substring(0, strEncode.length() - 4) + "1";
				} else if (strEncode.endsWith("===")) {
					strEncode = strEncode.substring(0, strEncode.length() - 3) + "8";
				} else if (strEncode.endsWith("=")) {
					strEncode = strEncode.substring(0, strEncode.length() - 1) + "9";
				}
				strEncode = StringHelper.makeTrans("0123456789ABCDEFGHIJKLMNOPQRSTUVWXYZ",
						"ZYXWVUTSRQPONMLKJIHGFEDCBA9876543210", strEncode);
			} catch (UnsupportedEncodingException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			return strEncode;
		}
		public static String getLngid(String sub_db_id, String rawid, boolean case_insensitive) {
			String uppercase_rawid = ""; // 大写版 rawid
			if (case_insensitive) { // 源网站的 rawid 区分大小写
				char[] rawlist = rawid.toCharArray();
				for (char ch : rawlist) {
					if (Character.toUpperCase(ch) == ch) {
						uppercase_rawid += ch;
					} else {
						uppercase_rawid += Character.toUpperCase(ch) + "_";
					}
				}
			} else {
				uppercase_rawid = rawid.toUpperCase();
			}
			String limited_id = uppercase_rawid;
			if (limited_id.length() > 20) {
				limited_id = DigestUtils.md5Hex(uppercase_rawid).toUpperCase();
			} else {
				limited_id = encodeID(uppercase_rawid);
			}
			String lngid = sub_db_id + limited_id;
			return lngid;
		}

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			String rawid = "";
			String lngid = "";
			String title = "";
			String creator = ""; //作者
			String creator_institution = ""; //学校
			String contributor = ""; //导师
			String source_institution = ""; //学位授予单位
			String date = ""; //years
			String description = ""; //摘要
			String creator_degree = ""; //学位
			String creator_discipline = ""; //作者专业
			String subject = ""; //关键词
			String language = "";
			String country = "";
			String provider_url = "";
			String provider_id = "";
			String provider = "hkmothesis";
			String sub_db_id = "00054";
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date())+ "00";
			String date_created = "";
			String type = "4";
			String medium = "2";
			
			

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
					if (creator.length() != 0 && creator.endsWith(";")) {
						creator = creator.substring(0, creator.length() - 1);
						}
				}
				else if (updateItem.getKey().equals("degree")) {
					creator_degree = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_word")) {
					subject = updateItem.getValue().trim();
					if (subject.length() != 0 && subject.endsWith(";")) {
						subject = subject.substring(0, subject.length() - 1);
					  
					     }
				}
				else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject")) {
					creator_discipline = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					creator_institution = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstract")) {
					description = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("contributor")) {
					contributor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
				
			}
			{
				contributor = contributor.replace("'", "''");
				description = description.replace("'", "''");
				creator_discipline = creator_discipline.replace("'", "''");
				creator_institution = creator_institution.replace("'", "''");
				subject = subject.replace("'", "''");
				creator_degree = creator_degree.replace("'", "''");
				creator = creator.replace("'", "''");
				title = title.replace("'", "''");
				
			}
			lngid = getLngid( sub_db_id,  rawid, false);
			provider_url = provider + "@" +"http://www.hkmolib.com/down/html/?" + rawid+".html";
			provider_id = provider + "@" + rawid;
			date_created = date+"0000";
			String sql = "INSERT INTO modify_title_info_zt([rawid],[lngid],[title],[creator],[creator_institution],"
					+ "[contributor],[source_institution],[date],[description],[creator_degree],[creator_discipline],"
					+ "[subject],[language],[country],[provider_url],[provider_id],[provider],[batch],"
					+ "[date_created],[type],[medium]) ";
			sql += " VALUES ('%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, rawid,lngid,title,creator,creator_institution,contributor,creator_institution,date,description,creator_degree,creator_discipline,
					subject,language,country,provider_url,provider_id,provider,batch,date_created,type,medium);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());

			}
		}

	}