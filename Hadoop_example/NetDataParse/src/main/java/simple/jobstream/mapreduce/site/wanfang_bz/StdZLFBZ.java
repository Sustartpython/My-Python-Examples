package simple.jobstream.mapreduce.site.wanfang_bz;

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
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FSDataOutputStream;
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
import com.google.gson.Gson;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdZLFBZ extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZLFBZ.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
//	private static String postfixDb3 = "zlf_bz";
//	private static String tempFileDb3 = "/RawData/wanfang/bz/template/wanfang_bz_template.db3";
//	
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
		
		//job.setInputFormatClass(SimpleTextInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

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
	
		//记录日志到HDFS
		public boolean log2HDFSForMapper(Context context, String text) {
			Date dt=new Date();//如果不需要格式,可直接用dt,dt就是当前系统时间
			DateFormat df = new SimpleDateFormat("yyyy/MM/dd HH:mm:ss");//设置显示格式
			String nowTime = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			df = new SimpleDateFormat("yyyyMMdd");//设置显示格式
			String nowDate = df.format(dt);//用DateFormat的format()方法在dt中获取并以yyyy/MM/dd HH:mm:ss格式显示
			
			text = nowTime + "\n" + text + "\n\n";
			
			boolean bException = false;
			BufferedWriter out = null;
			try {
				// 获取HDFS文件系统  
		        FileSystem fs = FileSystem.get(context.getConfiguration());
		  
		        FSDataOutputStream fout = null;
		        String pathfile = "/tangmao/log/log_map/" + nowDate + ".txt";
		        if (fs.exists(new Path(pathfile))) {
		        	fout = fs.append(new Path(pathfile));
				}
		        else {
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
			}
			else {
				return true;
			}
		}
		
		private static String getLngIDByWanID(String wanID) {
			wanID = wanID.toUpperCase();
			String lngID = "W_";
			for (int i = 0; i < wanID.length(); i++) {
				lngID += String.format("%d", wanID.charAt(i) + 0);
			}
			
			return lngID;
		}
		
		public static String getJsonStrByKV(String Key, List<String> Value) {
			Gson gson = new Gson();
			HashMap<String, List<String>> tmpMap = new HashMap<String, List<String>>();  
			tmpMap.put(Key,Value);
			return gson.toJson(tmpMap);
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			//新的键
			String rawid = "";
			//标题
			String title = "";
			//英文标题
			String title_alt = "";
			//摘要
			String abstracts = "";
			//标准编号
			String std_no = "";
			//标准类型
			String raw_type = "";
			//起草单位
			String organ = "";
			//发布日期
			String pub_date = "";
			//状态
			String legal_status = "";
			//强制性标准
			String is_mandatory = "";
			//实施日期
			String impl_date = "";
			//开本页数
			String page_cnt = "";
			//采用关系
			String adopt_relation = "";
			//中图分类号
			String clc_no = "";
			//中国标准分类号
			String ccs_no = "";
			// 国际标准分类号
			String ics_no = "";
			// 国别
			String country = "";
			//语言
			String language = "";
			//关键词
			String keyword = "";
			//第二关键词
			String keyword_alt = "";
			
			String batch = "";
			String donwdate = "";
			
			String url = "";
			
			//非a表数据 兼容旧表需要字段
			String date_created = "";
			String date = "";
			String bznum2 = "";

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_alt")) {
					title_alt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("abstracts")) {
					abstracts = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("std_no")) {
					std_no = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("raw_type")) {
					raw_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub_date")) {
					pub_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("legal_status")) {
					legal_status = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("is_mandatory")) {
					is_mandatory = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("impl_date")) {
					impl_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("page_cnt")) {
					page_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("adopt_relation")) {
					adopt_relation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("clc_no")) {
					clc_no = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ccs_no")) {
					ccs_no = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ics_no")) {
					ics_no = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("country")) {
					country = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_alt")) {
					keyword_alt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("batch")) {
					batch = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("donwdate")) {
					donwdate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bznum2")) {
					bznum2 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
			}
			
			keyword = keyword.replace(" ", ";");
			keyword_alt = keyword_alt.replace(" ", ";");
			
			
			//转义
			{	
				rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
				title = title.replace('\0', ' ').replace("'", "''").trim();
				title_alt = title_alt.replace('\0', ' ').replace("'", "''").trim();
				abstracts = abstracts.replace('\0', ' ').replace("'", "''").trim();
				std_no = std_no.replace('\0', ' ').replace("'", "''").trim();
				raw_type = raw_type.replace('\0', ' ').replace("'", "''").trim();
				organ = organ.replace('\0', ' ').replace("'", "''").trim();
				pub_date = pub_date.replace('\0', ' ').replace("'", "''").trim();	
				legal_status = legal_status.replace('\0', ' ').replace("'", "''").trim();	
				is_mandatory = is_mandatory.replace('\0', ' ').replace("'", "''").trim();	
				impl_date = impl_date.replace('\0', ' ').replace("'", "''").trim();	
				page_cnt  = page_cnt.replace('\0', ' ').replace("'", "''").trim();	
				adopt_relation = adopt_relation.replace('\0', ' ').replace("'", "''").trim();	
				clc_no = clc_no.replace('\0', ' ').replace("'", "''").trim();	
				ccs_no = ccs_no.replace('\0', ' ').replace("'", "''").trim();	
				ics_no = ics_no.replace('\0', ' ').replace("'", "''").trim();	
				country = country.replace('\0', ' ').replace("'", "''").trim();	
				language = language.replace('\0', ' ').replace("'", "''").trim();	
				keyword = keyword.replace('\0', ' ').replace("'", "''").trim();				
				keyword_alt = keyword_alt.replace('\0', ' ').replace("'", "''").trim();			
				batch = batch.replace('\0', ' ').replace("'", "''").trim();	
				donwdate = donwdate.replace('\0', ' ').replace("'", "''").trim();	
				date = date.replace('\0', ' ').replace("'", "''").trim();					
				bznum2 = bznum2.replace('\0', ' ').replace("'", "''").trim();	
				url = url.replace('\0', ' ').replace("'", "''").trim();
				
			}	

		    String sub_db_id = "00030";
		    String product = "WANFANG";
		    String sub_db = "WFSD";
		    
	        String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			// 以这个为例  工业闭式齿轮油 发布单位被取消 bzissued
	        // bzsubsbz 替代标准已无   射频阻抗计量器具检定系统
			String sql = "INSERT INTO modify_title_info([lngid], [title_c], [title_e], [bzmaintype], [years], "
					+ "[media_c], [bznum2], [bzpubdate], [bzimpdate], [bzstatus], "
					+ "[bzcountry], [showorgan], [class], [keyword_c], [keyword_e], "
					+ "[bzintclassnum], [remark_c], [bzpagenum], [netfulltextaddr], [rawid],"
					+ "[type], [language], [titletype], [srcid], [bzcnclassnum],"
					+ "[is_mandatory], [pagecount])";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s',"
					+ " '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s',"
					+ "'%s', '%s', '%s', '%s', '%s',"
					+ "'%s','%s');";
			sql = String.format(sql, lngid, title, title_alt, raw_type, date, 
					std_no, bznum2, pub_date, impl_date, legal_status, 
					country, organ, ccs_no, keyword,keyword_alt,
					 ics_no, abstracts, page_cnt, url, rawid,
					 "5", "1", "0;1;1280;1281", "VIP", clc_no,
					 is_mandatory,page_cnt);
//			String sql = "INSERT INTO modify_title_info([rawid])";
//			sql += " VALUES ('%s');";
//			sql = String.format(sql, rawid);
			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}