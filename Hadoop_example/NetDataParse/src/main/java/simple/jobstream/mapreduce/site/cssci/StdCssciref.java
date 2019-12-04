package simple.jobstream.mapreduce.site.cssci;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.commons.lang.StringEscapeUtils;
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
public class StdCssciref extends InHdfsOutHdfsJobInfo {

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
			String nowTime = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

			df = new SimpleDateFormat("yyyyMMdd");// 设置显示格式
			String nowDate = df.format(dt);// 用DateFormat的format()方法在dt中获取并以yyyy/MM/dd
											// HH:mm:ss格式显示

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

			String author = "";
			String author_1st = "";
			String title = "";
			String title_alt = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String publisher = "";
			String source_name = "";
			String raw_type = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String jump_page = "";
			String refer_text_raw = "";
			String refer_text_site = "";
			String refer_text = "";
			String strtype = "";
			String lngid = "";
			String cited_id = "";
			String product = "";
			String sub_db = "";
			String provider = "";
			String sub_db_id = "";
			String batch = "";
			String down_date = "";

			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_alt")) {
					title_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_name")) {
					source_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					begin_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					end_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("jump_page")) {
					jump_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_raw")) {
					refer_text_raw = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_site")) {
					refer_text_site = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text")) {
					refer_text = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strtype")) {
					strtype = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_id")) {
					cited_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}
			}

			String sql = "insert into base_obj_ref_a (author,author_1st,title,title_alt,pub_year,vol,num,publisher,source_name,raw_type,page_info,begin_page,end_page,jump_page,refer_text_raw,refer_text_site,refer_text,strtype,lngid,cited_id,product,sub_db,provider,sub_db_id,batch,down_date) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql, StringEscapeUtils.escapeSql(author),
					StringEscapeUtils.escapeSql(author_1st),
					StringEscapeUtils.escapeSql(title),
					StringEscapeUtils.escapeSql(title_alt),
					StringEscapeUtils.escapeSql(pub_year),
					StringEscapeUtils.escapeSql(vol),
					StringEscapeUtils.escapeSql(num),
					StringEscapeUtils.escapeSql(publisher),
					StringEscapeUtils.escapeSql(source_name),
					StringEscapeUtils.escapeSql(raw_type),
					StringEscapeUtils.escapeSql(page_info),
					StringEscapeUtils.escapeSql(begin_page),
					StringEscapeUtils.escapeSql(end_page),
					StringEscapeUtils.escapeSql(jump_page),
					StringEscapeUtils.escapeSql(refer_text_raw),
					StringEscapeUtils.escapeSql(refer_text_site),
					StringEscapeUtils.escapeSql(refer_text),
					StringEscapeUtils.escapeSql(strtype),
					StringEscapeUtils.escapeSql(lngid),
					StringEscapeUtils.escapeSql(cited_id),
					StringEscapeUtils.escapeSql(product),
					StringEscapeUtils.escapeSql(sub_db),
					StringEscapeUtils.escapeSql(provider),
					StringEscapeUtils.escapeSql(sub_db_id),
					StringEscapeUtils.escapeSql(batch),
					StringEscapeUtils.escapeSql(down_date));

			context.getCounter("map", "count").increment(1);
			// String lineOutput = AccessionNumber + "\t" + Authors + "\t" +
			// AuthorAffiliation + "\t" + CorrAuthorAffiliation;
			context.write(new Text(sql), NullWritable.get());

		}
	}

}