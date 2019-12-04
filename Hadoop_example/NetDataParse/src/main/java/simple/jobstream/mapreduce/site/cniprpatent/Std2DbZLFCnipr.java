package simple.jobstream.mapreduce.site.cniprpatent;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
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
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//这个db3是数据到智立方
public class Std2DbZLFCnipr extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Std2DbZLFCnipr.class);

	private static int reduceNum = 0;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

//	public static String postfixDb3 = "";
//	public static String tempFileDb3 = "/RawData/_rel_file/zlf_zl_template.db3";

	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();

		job.setJobName("cnipr." + jobName);

		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
//		postfixDb3 = job.getConfiguration().get("postfixDb3");
//		tempFileDb3 = job.getConfiguration().get("tempFileDb3");
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
		
		private static HashSet<String> pubNoSet =  new HashSet<String>();

		private static void initPubNoSet(Context context) throws IOException {
			// 获取HDFS文件系统
			FileSystem fs = FileSystem.get(context.getConfiguration());
			FSDataInputStream fin = fs.open(new Path("/RawData/cniprpatent/big_cover/text_20181029/txtcover.txt"));
			BufferedReader in = null;
			String line;
			try {
				in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
				while ((line = in.readLine()) != null) {
					String pub_no = line.trim();
					if (pub_no.length() < 7) {
						continue;
					}
					
					pubNoSet.add(pub_no.toLowerCase());
				}
			} finally {
				if (in != null) {
					in.close();
				}
			}

			System.out.println("pubNoSet size:" + pubNoSet.size());
		}

		public void setup(Context context) throws IOException, InterruptedException {
//			initPubNoSet(context);
		}

		public void cleanup(Context context) throws IOException, InterruptedException {

		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {

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
			String language = "1";
			// 图片地址
			String cover_path = "";
			//图片url
			String coverurl = "";

			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("app_no")) {
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
				}
			}
			
			

			String rawid = pub_no;
			String provider = "cniprpatent";
			pct_enter_nation_date = pct_enter_nation_date.replace(".", "").trim();
			pub_date = pub_date.replace(".", "").trim();
			if (pub_date.length() != 8) {
				context.getCounter("map", "pub_date length err").increment(1);
			}
			app_date = app_date.replace(".", "").trim();
			if (app_date.length() != 8) {
				context.getCounter("map", "app_date length err").increment(1);
			}
			String date = app_date.substring(0, 4);
		
			String url = "http://epub.cnipa.gov.cn/tdcdesc.action?strWhere=" + pub_no;
			String provider_url = provider + '@' + url;
			String provider_id = provider + '@' + rawid;
			String medium = "2";
			String type_ = "7";

			SimpleDateFormat formatter = new SimpleDateFormat("yyyyMMdd");
			String batch = formatter.format(new Date());
			batch += "00";
			
		    String sub_db_id = "00029";
		    String product = "CNIPR";
		    String sub_db = "ZL";
		    
	        String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
	        String srcid = "JSS";
	        String type = "4";
	        String titletype = "0;1;1024;1025";
	        language = "1";
	        
	        context.getCounter("map", "raw_type:" + raw_type).increment(1);
	        
			if (raw_type.equals("1") || raw_type.equals("8")){
				raw_type = "发明专利";
			}else if (raw_type.equals("2") || raw_type.equals("9")) {
				raw_type = "实用新型";
			}else if (raw_type.equals("3")) {
				raw_type = "外观设计";
			}
	        
//			cover_path = coverurl;
			
			
//			String cover = "";
//			if (pubNoSet.contains(pub_no.toLowerCase())) {
//				String cndir = pub_no.toLowerCase().substring(0,3);
//				String cndir2 = pub_no.toLowerCase().substring(3,7);
//				cover = "/smartlib/cniprpatent/"+cndir+"/"+cndir2+"/"+pub_no.toLowerCase()+".jpg";
//			}
			

			// 转义
			{
				lngid = lngid.replace('\0', ' ').replace("'", "''").trim();
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				provider_url = provider_url.replace('\0', ' ').replace("'", "''").trim();
				provider_id = provider_id.replace('\0', ' ').replace("'", "''").trim();
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
			}
			
			
			
//			if (applicant.indexOf("重庆大学") > -1 || applicant.indexOf("重庆建筑大学") > -1 || applicant.indexOf("重庆高等建筑学院") > -1) {
//				if (pub_date.substring(0, 4).equals("2018")) {
//					context.getCounter("map", "cqdxcount").increment(1);
//				}
//			}else {
//				return;
//			}


			String sql = "INSERT INTO modify_title_info([lngid]," + "[title_c]," + "[zlmaintype],"
						+ "[Showwriter]," + "[Showorgan]," +"[zlapplicantaddr]," +"[zlprovincecode]," +	"[zlapplicationnum],"
						+ "[zlapplicationdata]," + "[media_c]," + "[zlopendata]," + "[zlpriority]," + "[zlmainclassnum],"
						+ "[zlclassnum]," + "[zlinternationalpub]," + "[zlinternationalapp]," + "[zlcomeindata]," + "[remark_c],"
						+ "[keyword_c]," + "[zlsovereignty]," +"[zlagents]," +"[zlagency]," +"[zllegalstatus],"
						+"[srcid],"+"[years],"+"[language],"+"[type],"+"[titletype],"
						+ "[rawid])";
			sql += " VALUES ('%s', '%s', '%s',"
					+ "'%s', '%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s','%s','%s','%s','%s',"
					+ "'%s');";
			sql = String.format(sql, lngid, title,raw_type,
					author,applicant,applicant_addr,organ_area,app_no,
					app_date,pub_no,pub_date,priority,ipc_no_1st,
					ipc_no,pct_pub_data,pct_app_data,pct_enter_nation_date,abstracts,
					keyword,claim,agent,agency,legal_status,
					srcid,date,language,type,titletype,
					rawid);


//			String sql = "INSERT INTO modify_title_info_zt([lngid],[batch],[provider],[applicant])";
//			sql += " VALUES ('%s','%s','%s','%s');";
//			sql = String.format(sql, lngid, batch,provider,applicant); 
			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());

		}
	}
}