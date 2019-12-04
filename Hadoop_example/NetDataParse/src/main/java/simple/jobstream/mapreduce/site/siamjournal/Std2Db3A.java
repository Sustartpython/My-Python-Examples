package simple.jobstream.mapreduce.site.siamjournal;

import java.io.BufferedReader;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;

import javax.print.DocFlavor.STRING;

import org.apache.hadoop.fs.FSDataInputStream;
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

//输入应该为去重后的html
public class Std2Db3A extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2Db3A.class);

	private static int reduceNum = 0;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
//	private static String postfixDb3 = "siamjournal";
//	private static String tempFileDb3 = "/RawData/_rel_file/zt_template.db3";
	
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		private static HashSet<String> pubNoSet =  new HashSet<String>();

		public void setup(Context context) throws IOException,
				InterruptedException {
		}

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}		
		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
	         String rawid = "";
	         String product = "";
	         String down_date = "";
	         String batch = "";
	         String doi = "";
	         String title = "";
	         String author ="";
	         String pub_year = "";
	         String begin_page_sort = "";
	         String begin_page = "";
	         String end_page = "";
	         String page_cnt  = "";
	         String abstracts  = "";
	         //提交时间
	         String recv_date = "";
	         // 接受时间
	         String accept_date ="";
	         //出版时间
	         String pub_date = "";
	         String keyword = "";
	         //主题词
	         String subject_word = "";
	         //eissn 
	         String eissn = "";
	         //issn
	         String issn = "";
	         //publisher
	         String publisher = "";
	         //期刊名简写
	         String journal_id = "";
	         //期刊名
	         String journal_name = "";
	         //期
	         String issue = "";
	         //卷
	         String vol = "";
	         
	         String source_type="";
	         
	         String sub_db_id = "";
	         String sub_db = "";
	         
	        String provider = "SIAM";
	        String provider_url = "";
	        String raw_type = "";
	        String column_info= "";
	        String cited_cnt="";
			String country = "";
			String language= "";
			String lngid = "";
			String author_1st = "";
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("cited_cnt")) {
				    cited_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("sub_db_id")) {
				    sub_db_id = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
				    title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("rawid")) {
				    rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("raw_type")) {
				    raw_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("journal_name")) {
				    journal_name = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("sub_db")) {
				    sub_db = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("publisher")) {
				    publisher = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("country")) {
				    country = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("accept_date")) {
				    accept_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issn")) {
				    issn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider_url")) {
				    provider_url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("column_info")) {
				    column_info = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
				    issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("provider")) {
				    provider = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author")) {
				    author = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("begin_page_sort")) {
				    begin_page_sort = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("subject_word")) {
				    subject_word = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub_year")) {
				    pub_year = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("batch")) {
				    batch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstracts")) {
				    abstracts = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("product")) {
				    product = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("eissn")) {
				    eissn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("recv_date")) {
				    recv_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page_cnt")) {
				    page_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_date")) {
				    down_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
				    language = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
				    vol = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("source_type")) {
				    source_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("journal_id")) {
				    journal_id = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
				    doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("end_page")) {
				    end_page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub_date")) {
				    pub_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword")) {
				    keyword = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("begin_page")) {
				    begin_page = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("lngid")) {
				    lngid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				}
			}

			//转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				abstracts = abstracts.replace('\0', ' ').replace("'", "''").trim();
				recv_date = recv_date.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				source_type = source_type.replace('\0', ' ').replace("'", "''").trim();
				author = author.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				page_cnt = page_cnt.replace('\0', ' ').replace("'", "''").trim();
				accept_date = accept_date.replace('\0', ' ').replace("'", "''").trim();
				eissn = eissn.replace('\0', ' ').replace("'", "''").trim();
				sub_db = sub_db.replace('\0', ' ').replace("'", "''").trim();
				subject_word = subject_word.replace('\0', ' ').replace("'", "''").trim();
				journal_id = journal_id.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
				pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();
				journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim();
				product = product.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				column_info = column_info.replace('\0', ' ').replace("'", "''").trim();
				begin_page_sort = begin_page_sort.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();
				vol = vol.replace('\0', ' ').replace("'", "''").trim();
				end_page = end_page.replace('\0', ' ').replace("'", "''").trim();
				sub_db_id = sub_db_id.replace('\0', ' ').replace("'", "''").trim();
				begin_page = begin_page.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				doi = doi.replace('\0', ' ').replace("'", "''").trim();
				pub_year = pub_year.replace('\0', ' ').replace("'", "''").trim();
				keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
				issn = issn.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				author_1st = author_1st.replace('\0', ' ').replace("'", "''").trim();
			}
			
			provider_url = provider+"@"+provider_url;
			//新库对应表
			String sql = "INSERT INTO base_obj_meta_a([sub_db],[journal_id],[subject_word],[product],[column_info],[country],[issn],[abstract],[cited_cnt],[page_cnt],[source_type],[doi],[begin_page],[publisher],[vol],[provider_url],[language],[author],[down_date],[sub_db_id],[pub_date],[rawid],[provider],[eissn],[pub_year],[batch],[keyword],[page_info],[recv_date],[journal_name],[end_page],[lngid],[accept_date],[title],[raw_type],[num],[author_1st])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
			sql = String.format(sql,sub_db,journal_id,subject_word,product,column_info,country,issn,abstracts,cited_cnt,page_cnt,source_type,doi,begin_page,publisher,vol,provider_url,language,author,down_date,sub_db_id,pub_date,rawid,provider,eissn,pub_year,batch,keyword,begin_page_sort,recv_date,journal_name,end_page,lngid,accept_date,title,raw_type,issue,author_1st);
			
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

	
}