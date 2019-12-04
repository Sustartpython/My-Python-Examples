package simple.jobstream.mapreduce.site.asceProceedings;

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

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.site.cssci.StdCsscimeta.ProcessMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//输入应该为去重后的html
public class Std_2015 extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;

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

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		// TextOutputFormat.setCompressOutput(job, false);
		SequenceFileOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);


	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

			String title = "";
			String down_cnt = "";
			String author = "";
			String author_1st = "";
			String abstract_ = "";
			String author_intro = "";
			String provider_url = "";
			String pub_date = "";
			String pub_year = "";
			String begin_page = "";
			String end_page = "";
			String accept_time = "";
			String meeting_intro = "";
			String meeting_name = "";
			String accept_date = "";
			String down_date = "";
			String subject_word = "";
			String eisbn ="";
			String isbn = "";
			String page_info = "";
			String meeting_record_name = "";
			String meeting_place = "";
			String meeting_date_raw = "";
			String batch = "";
			
			
					
			//固定字段
			String lngid = "";
			String rawid = "";
			String sub_db_id = "00157";
			String is_oa = "0";
			String sub_db = "HY";
			String product = "ASCE";
			String publisher = "American Society of Civil Engineers";
			String provider = "ASCE";
			
			String doi = "";
			String source_type = "6";
			String country = "US";
			String language = "EN";
			
			String[] temp;
			String authors = "";
			String author_intros = "";
			String base_url = "https://ascelibrary.org/doi";
			
			String year = "";
			String month_days = "";
			String month_day = "";
			String all_time = "";
			String name_place = "";

			XXXXObject xObj = new XXXXObject();

			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {

				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_cnt")) {
					down_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("accept_time")) {
					accept_time = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("subject_word")) {
					subject_word = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					abstract_ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_intro")) {
					author_intro = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eisbn")) {
					eisbn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("isbn")) {
					isbn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					begin_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					end_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_name")) {
					meeting_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_record_name")) {
					meeting_record_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_intro")) {
					meeting_intro = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_place")) {
					meeting_place = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("meeting_date_raw")) {
					meeting_date_raw = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source_type")) {
					source_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
			}
			
			// 中信所更新a层(大于2015年）
			if (pub_year.equals("")) {
				return;
			} else {
				int a = Integer.parseInt(pub_year);
				if (a < 2015) {
					return;
				}
			}
			
			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}
	}
}