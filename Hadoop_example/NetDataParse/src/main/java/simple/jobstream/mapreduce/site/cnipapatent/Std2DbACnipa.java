package simple.jobstream.mapreduce.site.cnipapatent;

import java.io.IOException;
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

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class Std2DbACnipa extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(Std2DbACnipa.class);

	private static int reduceNum = 0;
	
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
				
			// 申请号
			String app_no = "";
			// 申请日
			String app_date = "";
			// 申请公布号
			String pub_no = "";
			// 申请公布日
			String pub_date = "";
			// 主分类号
			String ipc_no_1st = "";
			// 分类号
			String ipc_no = "";
			// 名称
			String title = "";
			// 申请人
			String applicant = "";
			// 发明人
			String author = "";
			// 地址
			String applicant_addr = "";
			// 国省代码
			String organ_area = "";
			// 专利代理机构
			String agency = "";
			// 代理人
			String agent = "";
			// 优先权
			String priority = "";
			// 优先权号
			String priority_no = "";
			// 优先权日
			String priority_date = "";
			// PCT进入国家阶段日
			String pct_enter_nation_date = "";
			// PCT申请数据
			String pct_app_data = "";
			// PCT公布数据
			String pct_pub_data = "";
			// 分案原申请号(我们的表中没有)
			String old_app_no = "";
			// 法律状态
			String legal_status = "";
			// 引证文献(没有 但是保留在hadoop平台)
			String Citing_literature = "";
			// 摘要
			String abstracts = "";
			// 关键词
			String keyword = "";
			// 专利类型
			String raw_type = "";
			// 主权项
			String claim = "";
			// 国家
			String country = "CN";
			// 语言
			String language = "ZH";
			// 图片地址
			String cover_path = "";
			//图片url
			String coverurl = "";
			
			String lngid = "";
			
			String rawid = "";
			
			String cpc_no_1st ="";
			
			String cpc_no = "";
			
			String cited_id = "";
			String cited_cnt = "";
			String ref_id = "";
			String ref_cnt = "";

			String batch = "";
	
			String down_date = "";
			String family_pub_no = "";
			String fulltext = "";
			String sub_db_id = "";
			String product = "";
			String sub_db = "";
			String provider = "";
			String source_type = "";
			String postcode = "";
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("app_no")) {
					app_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("app_date")) {
					app_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_no")) {
					pub_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ipc_no_1st")) {
					ipc_no_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ipc_no")) {
					ipc_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("applicant")) {
					applicant = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("author")) {
					author = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("applicant_addr")) {
					applicant_addr = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ_area")) {
					organ_area = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("agency")) {
					agency = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("agent")) {
					agent = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("priority")) {
					priority = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("priority_no")) {
					priority_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("priority_date")) {
					priority_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pct_enter_nation_date")) {
					pct_enter_nation_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pct_app_data")) {
					pct_app_data = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("pct_pub_data")) {
					pct_pub_data = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("legal_status")) {
					legal_status = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("abstracts")) {
					abstracts = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("claim")) {
					claim = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cover_path")) {
					cover_path = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("coverurl")) {
					coverurl = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cpc_no_1st")) {
					cpc_no_1st = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cpc_no")) {
					cpc_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_id")) {
					cited_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_id")) {
					ref_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("family_pub_no")) {
					family_pub_no = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("fulltext")) {
					fulltext = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db_id")) {
					sub_db_id = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("product")) {
					product = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("sub_db")) {
					sub_db = updateItem.getValue().trim();
				}  else if (updateItem.getKey().equals("provider")) {
					provider = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("source_type")) {
					source_type = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("postcode")) {
					postcode = updateItem.getValue().trim();
				}
			}
			
		
			pct_enter_nation_date = pct_enter_nation_date.replace(".", "").trim();
			app_date = app_date.replace(".", "").trim();
			pub_date = pub_date.replace(".", "").trim();
			ipc_no = StringHelper.cleanSemicolon(ipc_no);
			
			ref_cnt = ref_cnt+"@"+down_date;
			cited_cnt = cited_cnt +"@"+down_date;
			
			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				app_no = app_no.replace('\0', ' ').replace("'", "''").trim();
				app_date = app_date.replace('\0', ' ').replace("'", "''").trim();
				pub_no = pub_no.replace('\0', ' ').replace("'", "''").trim();
				pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();
				ipc_no_1st = ipc_no_1st.replace('\0', ' ').replace("'", "''").trim();
				ipc_no = ipc_no.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				applicant = applicant.replace('\0', ' ').replace("'", "''").trim();
				author = author.replace('\0', ' ').replace("'", "''").trim();
				applicant_addr = applicant_addr.replace('\0', ' ').replace("'", "''").trim();
				organ_area = organ_area.replace('\0', ' ').replace("'", "''").trim();
				agency = agency.replace('\0', ' ').replace("'", "''").trim();
				agent = agent.replace('\0', ' ').replace("'", "''").trim();
				priority = priority.replace('\0', ' ').replace("'", "''").trim();
				priority_no = priority_no.replace('\0', ' ').replace("'", "''").trim();
				priority_date = priority_date.replace('\0', ' ').replace("'", "''").trim();
				pct_enter_nation_date = pct_enter_nation_date.replace('\0', ' ').replace("'", "''").trim();
				pct_app_data = pct_app_data.replace('\0', ' ').replace("'", "''").trim();
				pct_pub_data = pct_pub_data.replace('\0', ' ').replace("'", "''").trim();
				legal_status = legal_status.replace('\0', ' ').replace("'", "''").trim();
				abstracts = abstracts.replace('\0', ' ').replace("'", "''").trim();
				keyword = keyword.replace('\0', ' ').replace("'", "''").trim();
				raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
				claim = claim.replace('\0', ' ').replace("'", "''").trim();
				country = country.replace('\0', ' ').replace("'", "''").trim();
				language = language.replace('\0', ' ').replace("'", "''").trim();
				cover_path = cover_path.replace('\0', ' ').replace("'", "''").trim();
				fulltext = fulltext.replace('\0', ' ').replace("'", "''").trim();
			}
			
			
			String sql = "INSERT INTO base_obj_meta_a("
					+ "[lngid], [app_no], [app_date], [pub_no], [pub_date], "
					+ "[ipc_no_1st], [ipc_no], [cpc_no_1st], [cpc_no], [title],"
					+ " [applicant], [author], [applicant_addr], [organ_area], [agency],"
					+ " [agent], [priority], [priority_no], [priority_date], [pct_enter_nation_date],"
					+ " [pct_app_data], [pct_pub_data], [legal_status], [cited_id], "
					+ "[abstract], [keyword], [raw_type], [claim], [country], "
					+ "[language], [cover_path], [batch], [down_date], [family_pub_no],"
					+ " [fulltext_txt], [sub_db_id], [product], [sub_db], [provider], "
					+ "[source_type], [rawid],[cited_cnt],[ref_id],[ref_cnt])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s',"
					+ " '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s')";
			sql = String.format(sql,lngid,app_no,app_date,pub_no,pub_date,
					ipc_no_1st,ipc_no,cpc_no_1st,cpc_no,title,
					applicant,author,applicant_addr,organ_area,agency,
					agent,priority,priority_no,priority_date,pct_enter_nation_date,
					pct_app_data,pct_pub_data,legal_status,cited_id,
					abstracts,keyword,raw_type,claim,country,
					language,cover_path,batch,down_date,family_pub_no,
					fulltext,sub_db_id,product,sub_db,provider,
					source_type,rawid,cited_cnt,ref_id,ref_cnt,
					postcode);
			


			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
		}
	}

	
}