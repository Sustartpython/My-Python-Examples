package simple.jobstream.mapreduce.site.rucjournal.back;

import java.io.BufferedWriter;
import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.regex.*;
import java.util.HashMap;

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
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;
import org.json.JSONArray;
import org.json.JSONObject;
import com.almworks.sqlite4java.SQLiteConnection;
import com.google.common.base.Strings;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

//输入应该为去重后的html
public class StdZLF extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger
			.getLogger(StdZLF.class);
	
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
			
			String lngID = "";
			String rawID = "";
			String titletype = "0;1;256;257";
			String media_c = "";
			String years = "";
			String title_c = "";
			String showwriter = "";
			String showorgan = "";
			String title_e = "";
			String keyword_c = "";
			String keyword_e = "";
			String remark_c = "";
			String remark_e = "";
			String medias_qk = "";
			String language = "";
			String type = "";
			String FirstWriter = "";
			String Introduce = "";
			String srcID = "";
			String range = "";
			String srcproducer = "";
			String includeid = "";
			String netfulltextaddr_all = "" ;
			String netfulltextaddr_all_std = "" ;
			String netfulltextaddr = "";
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
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawID = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("lngid")) {
					lngID = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("stil")) {
					stil = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("til")) {
					til = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("aut")) {
					showwriter = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				}
				
				if (updateItem.getKey().equals("etil")) {
					title_e = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword")) {
					keyword_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("keyword_alt")) {
					keyword_e = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("media_qk")) {
					medias_qk = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("abstracts")) {
					remark_c = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("east")) {
					remark_e = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("pna")) {
					media_c = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}

				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("author_1st")) {
					FirstWriter = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("Introduce")) {
					Introduce = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("netfulltextaddr_all")) {
					netfulltextaddr_all = updateItem.getValue().trim();
				}
				if (updateItem.getKey().equals("netfulltextaddr")) {
					netfulltextaddr = updateItem.getValue().trim();
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
				if (updateItem.getKey().equals("pna")) {
					source = updateItem.getValue().trim();
				}
				if(updateItem.getKey().equals("netfulltextaddr_all_std")){
					netfulltextaddr_all_std = updateItem.getValue().trim();
				}
			}
			
			title_c = til+stil;
			
			
			netfulltextaddr_all_std = createnetfulltextaddr_all_std(rawID).toString();
			netfulltextaddr_all = createnetfulltextaddr_all(rawID).toString();
			rawID = rawID.replace('\0', ' ').replace("'", "''").trim();
			
			srcID = "RUC";
			range = "RDFYBKZL";
			srcproducer = "RDFYBKZL";
			includeid = "[RDFYBKZL]" + rawID;
			titletype = titletype.replace('\0', ' ').replace("'", "''").trim();
			media_c = media_c.replace('\0', ' ').replace("'", "''").trim();
			years = years.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title_c = title_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			showwriter = showwriter.replace('\0', ' ').replace("'", "''").trim().replace("#", ";");
			showorgan = showorgan.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			title_e = title_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			remark_c = remark_c.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			remark_e = remark_e.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			medias_qk = medias_qk.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			language = language.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			type = type.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			FirstWriter = FirstWriter.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Introduce = Introduce.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcID = srcID.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			range = range.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			srcproducer = srcproducer.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			includeid = includeid.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr_all = netfulltextaddr_all.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr = netfulltextaddr.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			netfulltextaddr_all_std = netfulltextaddr_all_std.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			aino = aino.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			taut = taut.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			still = still.replace('\0', ' ').replace("'", "''").trim().replace("——", "").replace("\n", "");
			opc = opc.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			oad = oad.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opy = opy.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opn = opn.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			opg = opg.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			source = source.replace('\0', ' ').replace("'", "''").replace("\n", "").trim();
			Matcher matYearNum = patYearNum.matcher(medias_qk);
			if (matYearNum.find()) {
				years = matYearNum.group(1);
				num = matYearNum.group(2);
			}else{
				context.getCounter("map", "years null").increment(1);
			}
			
			String sql = "INSERT INTO modify_title_info([lngID], [rawID] , [titletype] , [media_c] , [years] , [title_c] , [title_e] , [keyword_c] , [keyword_e] , [remark_c] , [remark_e] , [showwriter] , [showorgan] , [medias_qk] , [language] , [type] , [FirstWriter] , [Introduce] , [srcID] , [range] , [srcproducer] , [includeid] , [netfulltextaddr_all], [netfulltextaddr], [auto],[autoinfo],[tauto],[still],[opc],[oad],[opy],[opn],[opg],[source],[netfulltextaddr_all_std],[num])";
			sql += " VALUES ('%s','%s', '%s', '%s', '%s', '%s', '%s',  '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s','%s');";
			sql = String.format(sql, lngID, rawID , titletype , media_c , years , title_c , title_e , keyword_c , keyword_e , remark_c , remark_e , showwriter , showorgan , medias_qk , language , type , FirstWriter , Introduce , srcID , range , srcproducer , includeid , netfulltextaddr_all , netfulltextaddr, showwriter,aino, taut ,still,opc,oad,opy,opn,opg,source,netfulltextaddr_all_std,num);
			context.getCounter("map", "count").increment(1);
			context.write(new Text(sql), NullWritable.get());
			
		}
	}

}