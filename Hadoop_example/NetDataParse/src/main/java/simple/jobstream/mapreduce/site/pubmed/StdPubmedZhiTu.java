package simple.jobstream.mapreduce.site.pubmed;

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
import org.apache.hadoop.hdfs.qjournal.protocol.QJournalProtocolProtos.JournalIdProto;
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

//输入应该为去重后的html
public class StdPubmedZhiTu extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(StdPubmedZhiTu.class);

	private static int reduceNum = 60;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static String ref_file_path = "/RawData/CQU/springer/ref_file/coverid.txt";

	public void pre(Job job) {
		String jobName = "pubmed." + this.getClass().getSimpleName();

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

		private static String lngid="";
		private static String rawid="";
		private static String title="";
		private static String title_string="";
		private static String title_alternative="";
		private static String title_alternativestring="";
		private static String title_sub="";
		private static String title_edition="";
		private static String title_series="";
		private static String title_seriesstring="";
		private static String identifier_eisbn="";
		private static String identifier_pisbn="";
		private static String identifier_pissn="";
		private static String identifier_eissn="";
		private static String identifier_cnno="";
		private static String identifier_standard="";
		private static String identifier_doi="";
		private static String creator="";
		private static String applicant="";
		private static String creator_cluster="";
		private static String creator_en="";
		private static String creator_institution="";
		private static String creator_institution_cluster="";
		private static String creator_bio="";
		private static String creator_discipline="";
		private static String creator_degree="";
		private static String creator_drafting="";
		private static String creator_release="";
		private static String contributor="";
		private static String source="";
		private static String source_string="";
		private static String source_fl="";
		private static String source_en="";
		private static String source_id="";
		private static String source_institution="";
		private static String publisher="";
		private static String date="";
		private static String volume="";
		private static String issue="";
		private static String description="";
		private static String description_en="";
		private static String description_fund="";
		private static String description_core="";
		private static String description_cycle="";
		private static String description_type="";
		private static String description_unit="";
		private static String description_location="";
		private static String description_source="";
		private static String subject="";
		private static String subject_en="";
		private static String page="";
		private static String beginpage="";
		private static String endpage="";
		private static String jumppage="";
		private static String pagecount="";
		private static String subject_clc="";
		private static String subject_clc_g1="";
		private static String subject_clc_g2="";
		private static String subject_esc="";
		private static String subject_esc_g1="";
		private static String subject_esc_g2="";
		private static String subject_dsa="";
		private static String subject_csc="";
		private static String subject_isc="";
		private static String date_impl="";
		private static String date_created="";
		private static String pct_enter_nation_date="";
		private static String pct_app_data="";
		private static String pct_pub_data="";
		private static String priority_number="";
		private static String cover="";
		private static String language="EN";
		private static String country="US";
		private static String if_html_fulltex="";
		private static String if_pdf_fulltext="";
		private static String if_pub1st="";
		private static String ref_cnt="";
		private static String cited_cnt="";
		private static String down_cnt="";
		private static String rawtype="";
		private static String agents="";
		private static String agency="";
		private static String legal_status="";
		private static String province_code="";
		private static String folio_size="";
		private static String price="";
		private static String type="3";
		private static String provider="";
		private static String provider_status="";
		private static String provider_url="";
		private static String provider_id="";
		private static String provider_bn="";
		private static String provider_subject="";
		private static String provider_combined="";
		private static String provider_owner="";
		private static String owners="";
		private static String combined="";
		private static String medium="2";
		private static String medium_owner="";
		private static String batch="";
		private static String is_oa="";
		private static String gch="";
		private static String journalID="";


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
		
			{
				lngid="";
				rawid="";
				title="";
				title_string="";
				title_alternative="";
				title_alternativestring="";
				title_sub="";
				title_edition="";
				title_series="";
				title_seriesstring="";
				identifier_eisbn="";
				identifier_pisbn="";
				identifier_pissn="";
				identifier_eissn="";
				identifier_cnno="";
				identifier_standard="";
				identifier_doi="";
				creator="";
				applicant="";
				creator_cluster="";
				creator_en="";
				creator_institution="";
				creator_institution_cluster="";
				creator_bio="";
				creator_discipline="";
				creator_degree="";
				creator_drafting="";
				creator_release="";
				contributor="";
				source="";
				source_string="";
				source_fl="";
				source_en="";
				source_id="";
				source_institution="";
				publisher="";
				date="";
				volume="";
				issue="";
				description="";
				description_en="";
				description_fund="";
				description_core="";
				description_cycle="";
				description_type="";
				description_unit="";
				description_location="";
				description_source="";
				subject="";
				subject_en="";
				page="";
				beginpage="";
				endpage="";
				jumppage="";
				pagecount="";
				subject_clc="";
				subject_clc_g1="";
				subject_clc_g2="";
				subject_esc="";
				subject_esc_g1="";
				subject_esc_g2="";
				subject_dsa="";
				subject_csc="";
				subject_isc="";
				date_impl="";
				date_created="";
				pct_enter_nation_date="";
				pct_app_data="";
				pct_pub_data="";
				priority_number="";
				cover="";
				language="";
				country="";
				if_html_fulltex="";
				if_pdf_fulltext="";
				if_pub1st="";
				ref_cnt="";
				cited_cnt="";
				down_cnt="";
				rawtype="";
				agents="";
				agency="";
				legal_status="";
				province_code="";
				folio_size="";
				price="";
				type="3";
				provider="pubmedjournal";
				provider_status="";
				provider_url="";
				provider_id="";
				provider_bn="";
				provider_subject="";
				provider_combined="";
				provider_owner="";
				owners="";
				combined="";
				medium="2";
				medium_owner="";
				batch="";
				is_oa="";
				gch="";
				journalID ="";

			}

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("doi")) {
					identifier_doi = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					subject = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("subject")) {
					provider_subject = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstract")) {
					description= updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					date_created = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					creator_institution= updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_year")) {
					date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("vol")) {
					volume = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					issue = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("issn")) {
					identifier_pissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("eissn")) {
					identifier_eissn = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("is_oa")) {
					is_oa = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("page_info")) {
					page = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_raw_id")) {
					journalID = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("journal_name")) {
					source = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("author")) {
					creator = updateItem.getValue().trim();
				} 
				  else if (updateItem.getKey().equals("publisher")) {
					  publisher = updateItem.getValue().trim();
					} 
			}
			
			// 处理gch
			gch = provider + "@" + journalID;
			provider_id = provider + "@" + rawid;
			provider_url = provider + "@https://www.ncbi.nlm.nih.gov/pubmed/" + rawid;
			
			// 生成新的batch
			batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";

			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				provider_id=provider_id.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				batch = batch.replace('\0', ' ').replace("'", "''").trim();
				identifier_doi=identifier_doi.replace('\0', ' ').replace("'", "''").trim();		
				title = title.replace('\0', ' ').replace("'", "''").trim();
				subject = subject.replace('\0', ' ').replace("'", "''").trim();
				volume = volume.replace('\0', ' ').replace("'", "''").trim();
				issue = issue.replace('\0', ' ').replace("'", "''").trim();		
				identifier_eissn = identifier_eissn.replace('\0', ' ').replace("'", "''").trim();
				identifier_pissn = identifier_pissn.replace('\0', ' ').replace("'", "''").trim();		
				is_oa = is_oa.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				source = source.replace('\0', ' ').replace("'", "''").trim();
				creator = creator.replace('\0', ' ').replace("'", "''").trim();
				creator_institution =creator_institution.replace('\0', ' ').replace("'", "''").trim();
				gch =gch.replace('\0', ' ').replace("'", "''").trim();
				description=description.replace('\0', ' ').replace("'", "''").trim();
				date=date.replace('\0', ' ').replace("'", "''").trim();
				language =language.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				date_created =date_created.replace('\0', ' ').replace("'", "''").trim();
				publisher=publisher.replace('\0', ' ').replace("'", "''").trim();
	
			}
				
			String sql = "INSERT INTO modify_title_info_zt([lngid],[rawid],[provider_id],[provider],[provider_url],[batch],[identifier_doi],[title],[subject],[volume],[issue],[identifier_eissn],[identifier_pissn],[is_oa],[country],[language],[source],[creator],[creator_institution],[gch],[language],[country],[type],[medium],[description],[date],[date_created],[publisher]) ";
			sql += " VALUES ('%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s','%s');";
			sql = String.format(sql,lngid,rawid,provider_id,provider,provider_url,batch,identifier_doi,title,subject,volume,issue,identifier_eissn,identifier_pissn,is_oa,country,language,source,creator,creator_institution,gch,language,country,type,medium,description,date,date_created,publisher);

			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
					

		

		}
	}

}