package simple.jobstream.mapreduce.site.rucjournal;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;
import java.util.Set;
import java.util.regex.*;
import java.util.HashMap;
import java.util.HashSet;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//输入应该为去重后的html
public class StdZT_new extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZT_new.class);
	
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 2;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	private static String postfixDb3 = "ezuezhe";

	
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
		
		Pattern patYearNum = Pattern.compile("(\\d{4})年([a-zA-Z0-9]+)期");
		

		public void cleanup(Context context) throws IOException,
				InterruptedException {
			
		}

		Set setid = new HashSet();
		
		public void setup(Context context) throws IOException,
	    InterruptedException {
	         // 获取HDFS文件系统  
	         FileSystem fs = FileSystem.get(context.getConfiguration());
	   
	         FSDataInputStream fin = fs.open(new Path("/RawData/ruc/rdfybk/newid/id.txt")); 
	         BufferedReader in = null;
	         String line;
	         try {
	          in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
	          while ((line = in.readLine()) != null) {
	        	  line = line.trim();
	        	  if (line.length() < 2) {
	        		  continue;
	        	  }
	        	  setid.add(line);
	          	}
	         } finally {
	          if (in!= null) {
	           in.close();
	          }
	         }
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
		        String pathfile = "/zengfanrong/log/log_map/" + nowDate + ".txt";
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
		
		
		public static JSONObject createnetfulltextaddr_all_std(String rawid){
			String [] attr = {rawid};
			String key = "RDFYBKZL@RUC";
			JSONObject o = null;
			try {
				HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
				JSONArray array = new JSONArray(attr);
				map.put(key, array);
				o = new JSONObject(map);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return o;
		}
		
		public static JSONObject createnetfulltextaddr_all(String rawid){
			String [] attr = {"http://ipub.exuezhe.com/paper.html?id=" + rawid};
			String key = "RDFYBKZL@RUC";
			JSONObject o = null;
			try {
				HashMap<String, JSONArray> map = new HashMap<String, JSONArray>();
				JSONArray array = new JSONArray(attr);
				map.put(key, array);
				o = new JSONObject(map);
			} catch (Exception e) {
				e.printStackTrace();
			}
			
			return o;
		}
		
		
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String lngid = "";
			String rawid = "";
			String titletype = "0;1;256;257";
			String years = "";
			String title = "";
			String aut = "";
			String showorgan = "";
			String etil = "";
			String keyword = "";
			String keyword_alt = "";
			String abstracts = "";
			String east = "";
			String medias_qk = "";
			String language = "";
			String type = "";
			String author_1st = "";
			String Introduce = "";
			String srcID = "";
			String range = "";
			String srcproducer = "";
			String includeid = "";
//			String netfulltextaddr_all = "" ;
//			String netfulltextaddr_all_std = "" ;
			String provider_url = "";
			String aino = "";
			String taut = "";
			String still = "";
			String opc = "";
			String oad = "";
			String opy = "";
			String opn = "";
			String opg = "";
			String source = "";
			String num = "";
			String til = "";
			String stil = "";
			String ori_src = "";
			String description_fund = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("lngid")) {
					lngid = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("stil")) {
					stil = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("til")) {
					til = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("aut")) {
					aut = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				}
				
				if (updateItem.getKey().equals("etil")) {
					etil = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword")) {
					keyword = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_alt")) {
					keyword_alt = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("media_qk")) {
					medias_qk = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("abstracts")) {
					abstracts = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("east")) {
					east = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("pna")) {
					source = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author_1st")) {
					author_1st = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Introduce")) {
					Introduce = updateItem.getValue().trim();
				}
//				if (updateItem.getKey().equals("netfulltextaddr_all")) {
//					netfulltextaddr_all = updateItem.getValue().trim();
//				}
				if (updateItem.getKey().equals("provider_url")) {
					provider_url = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("aino")) {
					aino = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("taut")) {
					taut = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("still")) {
					still = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opc")) {
					opc = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("oad")) {
					oad = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opy")) {
					opy = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opn")) {
					opn = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("opg")) {
					opg = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("ori_src")) {
					ori_src = updateItem.getValue().trim();
				}
				if(updateItem.getKey().equals("tno")){
					description_fund = updateItem.getValue().trim();
				}
			}
			
			context.getCounter("map", "allcount").increment(1);
			if (!setid.contains(rawid)) {
				return ;
			}
			
			title = til+stil;
			
			
//			netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawid).toString();
//			netfulltextaddr_all = createnetfulltextaddr_all(rawid).toString();
			rawid = rawid.replace('\0', ' ').replace("'", "''").trim();
			
			String jump_page="";
			String begin_page = "";
			String end_page = "";
			// 分割页面 page 获取里里面的详细信息
			opg = opg.replace("，", ",");
			String[] opgStrArray = opg.split(",");
			if (opgStrArray.length == 2) {
				jump_page = opgStrArray[1].trim();
			}
			String[] pageArray = opgStrArray[0].split("-");
			if (pageArray.length == 2) {
				begin_page = pageArray[0].trim();
				end_page = pageArray[1].trim();
			}
			
			srcID = "RUC";
			range = "RDFYBKZL";
			srcproducer = "RDFYBKZL";
			includeid = "[RDFYBKZL]" + rawid;
			description_fund = description_fund.replace('\0', ' ').replace("'", "''").trim();
			titletype = titletype.replace('\0', ' ').replace("'", "''").trim();
			source = source.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title = title.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			aut = aut.replace('\0', ' ').replace("'", "''").trim().replace("#", ";");
			showorgan = showorgan.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			etil = etil.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword = keyword.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_alt = keyword_alt.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			abstracts = abstracts.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			east = east.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			medias_qk = medias_qk.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			language = language.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			type = type.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			author_1st = author_1st.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Introduce = Introduce.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcID = srcID.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			range = range.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcproducer = srcproducer.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			includeid = includeid.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			//netfulltextaddr_all = netfulltextaddr_all.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			provider_url = provider_url.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			//netfulltextaddr_all_std = netfulltextaddr_all_std.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			aino = aino.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			taut = taut.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			still = still.replace('\0', ' ').replace("'", "''").trim().replace("——", "").replace("\n", "");
			opc = opc.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			oad = oad.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opy = opy.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opn = opn.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opg = opg.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Matcher matYearNum = patYearNum.matcher(medias_qk);
			if (matYearNum.find()) {
				years = matYearNum.group(1);
				num = matYearNum.group(2);
			}else{
				context.getCounter("map", "years null").increment(1);
			}
			type = "3";
			String medium = "2";
			String provider = "rdfybkjournal";
			provider_url = provider+"@"+"http://www.rdfybk.com/qk/pdfview?id="+rawid;
			String provider_id = provider+"@"+rawid;
			String sub_db_id = "00114";
			String product = "RDFYBK";
			String sub_db = "QK";
			String date_created = years+"0000";
			

			lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
			String batch = (new SimpleDateFormat("yyyyMMdd")).format(new Date()) + "00";
			String sql = "INSERT INTO modify_title_info_zt([lngid], [rawid] , [source] , [date] , [issue] , "
					+ "[title] , [title_alternative] , [subject] , [subject_en], [description] ,[description_en], [creator] , "
					+ "[creator_bio] , [page] , [country] , [language] , "
					+ "[type] , [medium] , [provider] , [provider_url] , "
					+ "[provider_id] , [batch], [beginpage], [endpage], "
					+ "[jumppage],[description_fund],[date_created])";
			sql += " VALUES ('%s','%s', '%s', '%s', '%s', "
					+ "'%s', '%s',  '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s');";
			sql = String.format(sql, lngid, rawid , source , years , num ,
					title , etil , keyword ,keyword_alt, abstracts ,east, aut, 
					Introduce , opg , "CN" , language , 
					type , medium , provider , provider_url , 
					provider_id , batch ,begin_page,end_page,jump_page,
					description_fund,date_created);
			
//			String sql = "INSERT INTO modify_title_info_zt([rawid])";
//			sql += " VALUES ('%s');";
//			sql = String.format(sql, rawid);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}