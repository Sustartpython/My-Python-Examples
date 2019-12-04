package simple.jobstream.mapreduce.user.walker.cnki_qk_ref;

import java.io.IOException;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//按智立方格式导出db3
public class Std2Db3ZLF4Ref extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2Db3ZLF4Ref.class);

	private static int reduceNum = 0;

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
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);

		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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
		private static String lngid = "";
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String title = "";
		private static String title_alt = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String raw_type = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_alt = "";
		private static String year_vol_num = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String publisher = "";
		private static String cited_id = "";
		private static String linked_id = "";
		private static String refer_text_raw = "";
		private static String refer_text_raw_alt = "";
		private static String refer_text_site = "";
		private static String refer_text_site_alt = "";
		private static String refer_text = "";
		private static String refer_text_alt = "";
		private static String source_name = "";
		private static String source_name_alt = "";
		private static String strtype = "";

		public void setup(Context context) throws IOException, InterruptedException {

		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		private static String getLngIDByCnkiID(String cnkiID) {
			cnkiID = cnkiID.toUpperCase();
			String lngID = "";
			for (int i = 0; i < cnkiID.length(); i++) {
				lngID += String.format("%d", cnkiID.charAt(i) + 0);
			}
			
			return lngID;
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			{
				lngid = "";
				sub_db_id = "";
				product = "";
				sub_db = "";
				provider = "";
				down_date = "";
				batch = "";
				doi = "";
				title = "";
				title_alt = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				raw_type = "";
				author_1st = "";
				author = "";
				author_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				publisher = "";
				cited_id = "";
				linked_id = "";
				refer_text_raw = "";
				refer_text_raw_alt = "";
				refer_text_site = "";
				refer_text_site_alt = "";
				refer_text = "";
				refer_text_alt = "";
				source_name = "";
				source_name_alt = "";
				strtype = "";
			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_alt")) {
					title_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page_info = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("begin_page")) {
					begin_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("end_page")) {
					end_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("jump_page")) {
					jump_page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author_alt")) {
					author_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("year_vol_num")) {
					year_vol_num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					pub_year = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("publisher")) {
					publisher = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_id")) {
					cited_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("linked_id")) {
					linked_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_raw")) {
					refer_text_raw = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_raw_alt")) {
					refer_text_raw_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_site")) {
					refer_text_site = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_site_alt")) {
					refer_text_site_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text")) {
					refer_text = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("refer_text_alt")) {
					refer_text_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_name")) {
					source_name = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_name_alt")) {
					source_name_alt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("strtype")) {
					strtype = updateItem.getValue().trim();
				}
			}

			
			String[] vec = cited_id.split("@");
			String rawsourceid = vec[1];
			String lngsourceid = getLngIDByCnkiID(rawsourceid);
			lngid = lngsourceid + lngid.substring(lngid.length()-4);

			String sql = "INSERT INTO ref_zk([id], [rawsourceid], [lngsourceid], [refertext_tag], [refertext_cnki], [strtitle], [strtype], [strname], [strwriter1], [stryearvolnum], [strpubwriter], [strpages], [cnki_filename], [down_date]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			// 转义
			{
				String refertext_tag = refer_text_raw.replace('\0', ' ').replace("'", "''").trim();
				String refertext = refer_text_site.replace('\0', ' ').replace("'", "''").trim();
				String strtitle = title.replace('\0', ' ').replace("'", "''").trim();
				if (strtype.length() < 1) {
					strtype = "K";
				}
				strtype = strtype.replace('\0', ' ').replace("'", "''").trim();
				String strname = source_name.replace('\0', ' ').replace("'", "''").trim();
				String strwriter1 = author.replace('\0', ' ').replace("'", "''").trim();
				String stryearvolnum = year_vol_num.replace('\0', ' ').replace("'", "''").trim();
				String strpubwriter = publisher.replace('\0', ' ').replace("'", "''").trim();
				String strpages = page_info.replace('\0', ' ').replace("'", "''").trim();
//				doi = doi.replace('\0', ' ').replace("'", "''").trim();
				String cnki_filename = linked_id.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
				
				sql = String.format(sql, lngid, rawsourceid, lngsourceid, refertext_tag, refertext, strtitle, strtype, strname, strwriter1,
						stryearvolnum, strpubwriter, strpages, cnki_filename, down_date);
			}
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
		}
	}
}