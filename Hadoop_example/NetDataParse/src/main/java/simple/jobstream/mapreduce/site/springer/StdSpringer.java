package simple.jobstream.mapreduce.site.springer;

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
import java.util.List;
import java.util.Map;
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
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdSpringer extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdSpringer.class);

	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/CQU/springer/ref_file/coverid.txt";


	public void pre(Job job) {
		String jobName = "springerlink." + this.getClass().getSimpleName();

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
		job.setOutputValueClass(BytesWritable.class);
		JobConfUtil.setTaskPerReduceMemory(job, 6144);

		
		
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

		
		public String[] getCoveridArray() throws IOException {

			Configuration conf = new Configuration();
			FileSystem fs = FileSystem.get(conf);

			// check if the file exists
			Path path = new Path(ref_file_path);
			if (fs.exists(path)) {
				FSDataInputStream is = fs.open(path);
				// get the file info to create the buffer
				FileStatus stat = fs.getFileStatus(path);

				// create the buffer
				byte[] buffer = new byte[Integer.parseInt(String.valueOf(stat.getLen()))];
				is.readFully(0, buffer);

				String coveridString = new String(buffer);

				return coveridString.split("\\*");
			} else {
				return null;
			}

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			String lngid = "";
			String rawid = "";
			String identifier_doi = "";
			String title = "";
			String identifier_pissn = "";
			String identifier_eissn = "";
			String creator = "";
			String creator_institution = "";
			String source = "";
			String publisher = "";
			String date = "";
			String volume = "";
			String issue = "";
			String description = "";
			String subject = "";
			String page = "";
			String firstPage = "";
			String lastPage = "";
			String date_created = "";
			String language = "EN";
			String country = "US";
			String provider = "springerjournal";
			String provider_url = "";
			String provider_id = "";
			String type = "3";
			String medium = "2";
			String gch = "";
			String batch ="";
			String journal_ID = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_pissn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("identifier_eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator")) {
					creator = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("creator_institution")) {
					creator_institution = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source")) {
					source = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("volume")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("issue")) {
					issue = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("description")) {
					description = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("date_created")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject")) {
					subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("firstPage")) {
					firstPage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lastPage")) {
					lastPage = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journalId")) {
					journal_ID = updateItem.getValue().trim();
					gch = provider + "@" + updateItem.getValue().trim();
				} 

			}

			if (rawid.length() > 0) {
				lngid = VipIdEncode.getLngid("00021", rawid, false);
				
			}
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			identifier_doi = identifier_doi.replace('\0', ' ').replace("'", "''").trim();
			title = title.replace('\0', ' ').replace("'", "''").trim();
			creator = creator.replace('\0', ' ').replace("'", "''").trim();
			creator_institution = creator_institution.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
			description = description.replace('\0', ' ').replace("'", "''").trim();
			subject = subject.replace('\0', ' ').replace("'", "''").trim();

			date = date_created.split("/")[0];
			date_created = date_created.replace("/", "");
			page = firstPage + "-" + lastPage;

			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			provider_url = provider + "@http://link.springer.com/article/" + identifier_doi;
			provider_id = provider + "@" + rawid;
			

			
			if(creator.equals("    [1]")) {
				// 此时应当将作者置为空
				
				creator = "";
				
				// 并且将机构的编号取消
				creator_institution =creator_institution.replaceAll("\\[\\d\\]", "");
				context.getCounter("map", "deal_c").increment(1);
			}
			

			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid], [identifier_doi],  [title], [identifier_pissn], [identifier_eissn], [creator], [creator_institution], [source], [publisher], [date_created], [volume], [issue], [description], [subject], [page], [type], [language], [country], [provider], [batch], [provider_url], [medium], [date],[provider_id],[gch]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s','%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			sql = String.format(sql, lngid, rawid, identifier_doi, title, identifier_pissn, identifier_eissn, creator,
					creator_institution, source, publisher, date_created, volume, issue, description, subject, page,
					type, language, country, provider, batch, provider_url, medium, date, provider_id, gch);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			


		}
	}

}