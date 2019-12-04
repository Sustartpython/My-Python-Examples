package simple.jobstream.mapreduce.site.intlpressjournal;

import java.io.IOException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
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

//输入应该为去重后的html
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2Db3.class);

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
				
			   String lngid = "";
		         String doi = "";
		         //作者机构原始信息
		         String author_raw = "";
		         String author ="";
		         String organ = "";
		         String author_1st = "";
	    		 String organ_1st = "";
		         String abstract_ = "";
		         String recv_date = "";
		         String pub_date = "";
		         String revision_date = "";
		         String accept_date = "";
		         String fund = "";
		         String title = "";
		         String pub_year = "";
		         //期
		         String num = "";
		         //卷
		         String vol = "";
		         
		         String page_info  = "";
		         
		         String begin_page = "";
		         
		         String end_page = "";
		         
		         String journal_raw_id = "";
		         
		         String provider_url = "";
		         
		         //期刊名
		         String journal_name = "";
		         
		         //eissn 
		         String eissn = "";
		         //issn
		         String issn = "";
		         
		         //总引用次数
		         String cited_cnt= "";
		         
		         // 期刊影响因子(a表暂时没有)
		         String factor = "";
		         
		         String rawid = "";
		         String product = "INTLPRESS";
		         String down_date = "";
		         
		         String sub_db_id = "00125";
		         String sub_db = "QK";
		         
		        String provider = "INTLPRESS";
		        String provider_zt = "intlpressjournal";
		         
		         
		        String raw_type = "Article";
		        
		        String country = "US";
				String language= "EN";
				
				String source_type = "3";
				
				String publisher = "International Press of Boston, Inc.";
				
				String fulltext_type = "pdf";
				//主编
				String Chief = "";

			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
				    title = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("pub_year")) {
				    pub_year = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("abstract")) {
				    abstract_ = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("source_type")) {
				    source_type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("end_page")) {
				    end_page = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("provider")) {
				    provider = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("num")) {
				    num = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("sub_db")) {
				    sub_db = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("eissn")) {
				    eissn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("sub_db_id")) {
				    sub_db_id = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("accept_date")) {
				    accept_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("organ")) {
				    organ = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("raw_type")) {
				    raw_type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("journal_name")) {
				    journal_name = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("down_date")) {
				    down_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("pub_date")) {
				    pub_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("fund")) {
				    fund = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("begin_page")) {
				    begin_page = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("vol")) {
				    vol = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("page_info")) {
				    page_info = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("publisher")) {
				    publisher = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("country")) {
				    country = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("issn")) {
				    issn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("fulltext_type")) {
				    fulltext_type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("journal_raw_id")) {
				    journal_raw_id = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Chief")) {
				    Chief = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("provider_zt")) {
				    provider_zt = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("rawid")) {
				    rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("lngid")) {
				    lngid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("factor")) {
				    factor = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author_raw")) {
				    author_raw = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("revision_date")) {
				    revision_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("language")) {
				    language = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("doi")) {
				    doi = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("recv_date")) {
				    recv_date = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("organ_1st")) {
				    organ_1st = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("cited_cnt")) {
				    cited_cnt = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author_1st")) {
				    author_1st = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("provider_url")) {
				    provider_url = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author")) {
				    author = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("product")) {
				    product = updateItem.getValue().trim();
				}
			}

			//转义
			{
				issn = issn.replace('\0', ' ').replace("'", "''").trim();
				pub_year = pub_year.replace('\0', ' ').replace("'", "''").trim();
				sub_db = sub_db.replace('\0', ' ').replace("'", "''").trim();
				pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();
				revision_date = revision_date.replace('\0', ' ').replace("'", "''").trim();
				Chief = Chief.replace('\0', ' ').replace("'", "''").trim();
				author_1st = author_1st.replace('\0', ' ').replace("'", "''").trim();
				provider = provider.replace('\0', ' ').replace("'", "''").trim();
				journal_name = journal_name.replace('\0', ' ').replace("'", "''").trim();
				begin_page = begin_page.replace('\0', ' ').replace("'", "''").trim();
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				page_info = page_info.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
				end_page = end_page.replace('\0', ' ').replace("'", "''").trim();
				sub_db_id = sub_db_id.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				organ_1st = organ_1st.replace('\0', ' ').replace("'", "''").trim();
				abstract_ = abstract_.replace('\0', ' ').replace("'", "''").trim();
				doi = doi.replace('\0', ' ').replace("'", "''").trim();
				provider_zt = provider_zt.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				factor = factor.replace('\0', ' ').replace("'", "''").trim();
				publisher = publisher.replace('\0', ' ').replace("'", "''").trim();
				accept_date = accept_date.replace('\0', ' ').replace("'", "''").trim();
				fund = fund.replace('\0', ' ').replace("'", "''").trim();
				raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
				vol = vol.replace('\0', ' ').replace("'", "''").trim();
				author = author.replace('\0', ' ').replace("'", "''").trim();
				product = product.replace('\0', ' ').replace("'", "''").trim();
				eissn = eissn.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				author_raw = author_raw.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				fulltext_type = fulltext_type.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
				source_type = source_type.replace('\0', ' ').replace("'", "''").trim();
				recv_date = recv_date.replace('\0', ' ').replace("'", "''").trim();
				organ = organ.replace('\0', ' ').replace("'", "''").trim();
				num = num.replace('\0', ' ').replace("'", "''").trim();
				journal_raw_id = journal_raw_id.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
			}
			
			
			String provider_id = provider_zt +"@"+rawid;
			provider_url = provider_zt+"@"+provider_url;
			String gch = provider_zt +"@" +journal_raw_id;
			String medium = "2";
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyyMMdd");
			String batch = formatter.format(new Date());
			batch += "00";
			
			//智图与a表的字段对应
			lngid = lngid;
			rawid = rawid;
			String identifier_doi = doi;
			String creator = author;
			String creator_institution = organ;
			String description = abstract_;
			String date_created = pub_date;
			String description_fund = fund;
			title = title;
	        String date = pub_year;
	        String issue = num;
	        String volume = vol;
	        String page = page_info;
	        String beginpage = begin_page;
	        String endpage = end_page;
	        String source = journal_name;
	        String identifier_pissn = issn;
	        String identifier_eissn = eissn;
	        provider = provider_zt; 
	        String rawtype = raw_type;
	        country = country;
			language= language;
			String type_ = source_type;
			publisher = publisher;
			String if_pdf_fulltext = "1";
			String applicant = Chief.split("\\(")[0].trim();

			
			//智图对应表
			String sql = "INSERT INTO modify_title_info_zt([issue],[description],[date_created],[provider_url],[creator_institution],[title],[if_pdf_fulltext],[creator],[rawtype],[lngid],[batch],[country],[language],[identifier_doi],[source],[provider],[identifier_pissn],[date],[provider_id],[identifier_eissn],[volume],[description_fund],[beginpage],[page],[medium],[rawid],[gch],[endpage],[type],[publisher])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s')";
			sql = String.format(sql,issue,description,date_created,provider_url,creator_institution,title,if_pdf_fulltext,creator,rawtype,lngid,batch,country,language,identifier_doi,source,provider,identifier_pissn,date,provider_id,identifier_eissn,volume,description_fund,beginpage,page,medium,rawid,gch,endpage,type_,publisher);

			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

	
}