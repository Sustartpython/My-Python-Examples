package simple.jobstream.mapreduce.site.aip;

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
import java.util.List;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringEscapeUtils;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.almworks.sqlite4java.SQLiteConnection;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.StdCsscimeta.ProcessMapper;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;

//输入应该为去重后的html
public class StdAIP_2015 extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
		String jobName = "StdAIP_2015";
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

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		// TextOutputFormat.setCompressOutput(job, false);
		SequenceFileOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);


	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, BytesWritable> {

		private  static Map<String, String> mapSource =new HashMap<String, String>();
		private  static Map<String, String> mapPissn =new HashMap<String, String>();
		private  static Map<String, String> mapEissn =new HashMap<String, String>();
		
		
		private static void initMapSource() {
			mapSource.put("adv", "AIP Advances");
			mapSource.put("apc", "AIP Conference Proceedings");
			mapSource.put("apb", "APL Bioengineering");
			mapSource.put("apm", "APL Materials");
			mapSource.put("app", "APL Photonics");
			mapSource.put("apl", "Applied Physics Letters");
			mapSource.put("are", "Applied Physics Reviews");
			mapSource.put("bmf", "Biomicrofluidics");
			mapSource.put("cha", "Chaos: An Interdisciplinary Journal of Nonlinear Science");
			mapSource.put("cip", "Computers in Physics");
			mapSource.put("csx", "Computing in Science & Engineering");
			mapSource.put("jap", "Journal of Applied Physics");
			mapSource.put("phy", "Journal of Applied Physics");
			mapSource.put("jcp", "The Journal of Chemical Physics");
			mapSource.put("jmp", "Journal of Mathematical Physics");
			mapSource.put("jpr", "Journal of Physical and Chemical Reference Data");
			mapSource.put("rse", "Journal of Renewable and Sustainable Energy");
			mapSource.put("ltp", "Low Temperature Physics");
			mapSource.put("phf", "Physics of Fluids");
			mapSource.put("pfl", "Physics of Fluids");
			mapSource.put("pfa", "Physics of Fluids");
			mapSource.put("php", "Physics of Plasmas");
			mapSource.put("pfb", "Physics of Plasmas");
			mapSource.put("pto", "Physics Today");
			mapSource.put("rsi", "Review of Scientific Instruments");
			mapSource.put("sdy", "Structural Dynamics");

		}
		
		private static void initMapPissn() {
			mapPissn.put("adv", "");
			mapPissn.put("apc", "");
			mapPissn.put("apb", "2473-2877");
			mapPissn.put("apm", "2166-532X");
			mapPissn.put("app", "2378-0967");
			mapPissn.put("apl", "0003-6951");
			mapPissn.put("are", "");
			mapPissn.put("bmf", "1932-1058");
			mapPissn.put("cha", "1054-1500");
			mapPissn.put("cip", "");
			mapPissn.put("csx", "");
			mapPissn.put("jap", "0021-8979");
			mapPissn.put("phy", "0021-8979");
			mapPissn.put("jcp", "0021-9606");
			mapPissn.put("jmp", "0022-2488");
			mapPissn.put("jpr", "0047-2689");
			mapPissn.put("rse", "");
			mapPissn.put("ltp", "1063-777X");
			mapPissn.put("phf", "1070-6631");
			mapPissn.put("pfl", "1070-6631");
			mapPissn.put("pfa", "1070-6631");
			mapPissn.put("php", "1070-664X");
			mapPissn.put("pfb", "1070-664X");
			mapPissn.put("pto", "0031-9228");
			mapPissn.put("rsi", "0034-6748");
			mapPissn.put("sdy", "");			
		}
		
		private static void initMapEissn() {
			mapEissn.put("adv", "2158-3226");
			mapEissn.put("apc", "");
			mapEissn.put("apb", "");
			mapEissn.put("apm", "");
			mapEissn.put("app", "");
			mapEissn.put("apl", "1077-3118");
			mapEissn.put("are", "1931-9401");
			mapEissn.put("bmf", "");
			mapEissn.put("cha", "1089-7682");
			mapEissn.put("cip", "");
			mapEissn.put("csx", "");
			mapEissn.put("jap", "1089-7550");
			mapEissn.put("phy", "1089-7550");
			mapEissn.put("jcp", "1089-7690");
			mapEissn.put("jmp", "1089-7658");
			mapEissn.put("jpr", "1529-7845");
			mapEissn.put("rse", "1941-7012");
			mapEissn.put("ltp", "");
			mapEissn.put("phf", "1089-7666");
			mapEissn.put("pfl", "1089-7666");
			mapEissn.put("pfa", "1089-7666");
			mapEissn.put("php", "1089-7674");
			mapEissn.put("pfb", "1089-7674");
			mapEissn.put("pto", "1945-0699");
			mapEissn.put("rsi", "1089-7623");
			mapEissn.put("sdy", "");
		}
		
		public void setup(Context context) throws IOException,
				InterruptedException {
			initMapSource();
			initMapPissn();
			initMapEissn();
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
		        String pathfile = "/venter/mirror_chaoxing/log/log_map/" + nowDate + ".txt";
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
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String url = "";
	    	String vol = "";
	    	String num = "";
	    	String catalog = "";
	    	String doi = "";
				
			String title = "";//标题
			String author = "";//作者
			String author_1st = "";
			String pub_year="";
			String organ="";//机构
			String abstract_ = "";//内容提要
			String rawid = "";
            String journal_name = "";
            String journal_raw_id = "";
            String issn = "";
            String eissn = "";
            

		    String lngid = "";
		    String batch="";
		    String provider="AIP";
		    String product = "AIP";
		    String sub_db = "QK";
		    String sub_db_id = "00166";
		    String provider_url="";
		    String country="US";
		    String pub_date="";
			String language = "EN";
			String source_type = "3";
			String publisher = "American Institute of Physics";
			String down_date = "20190825";
			

			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("title")) {
					title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("url")) {
					url = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issue")) {
					num = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("catalog")) {
					catalog = updateItem.getValue().trim();
					journal_raw_id = catalog;
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("creator")) {
					author = updateItem.getValue().trim();
					if(author.contains(";")) {
						author_1st = author.split(";")[0];
					}else {
						author_1st = author;
					}
				}
				else if (updateItem.getKey().equals("insitution")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("press_year")) {
					pub_year = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("description")) {
					abstract_ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("date")) {
					pub_date = updateItem.getValue().trim();
				}
			}
			rawid = doi;			

			
//			Date dt = new Date();
//			DateFormat df = new SimpleDateFormat("yyyyMMdd");
//			String nowDate = df.format(dt);
//			batch = nowDate+"";
			batch = "20191014_164320";
			
			provider_url = url;
			
			lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
			
			journal_name = mapSource.get(catalog);
			issn = mapPissn.get(catalog);
			eissn = mapEissn.get(catalog);

			if (pub_year.equals("")) {
				return;
			} else {
				int a = Integer.parseInt(pub_year);
				if (a < 2015) {
					return;
				}
			}
			
			XXXXObject xxObj = new XXXXObject();

			xxObj.data.put("author",author);
			xxObj.data.put("author_1st",author_1st);
			xxObj.data.put("title",title);
			xxObj.data.put("doi",doi);
			xxObj.data.put("pub_date",pub_date);
			xxObj.data.put("pub_year",pub_year);
			xxObj.data.put("issn",issn);
			xxObj.data.put("eissn",eissn);
			xxObj.data.put("organ",organ);
			xxObj.data.put("abstract",abstract_);
			xxObj.data.put("publisher",publisher);
			xxObj.data.put("journal_name", journal_name);
			xxObj.data.put("vol", vol);
			xxObj.data.put("num", num);
			xxObj.data.put("journal_raw_id", journal_raw_id);
			
			xxObj.data.put("lngid",lngid);
			xxObj.data.put("rawid",rawid);
			xxObj.data.put("product",product);
			xxObj.data.put("sub_db",sub_db);
			xxObj.data.put("provider",provider);
			xxObj.data.put("sub_db_id",sub_db_id);
			xxObj.data.put("source_type",source_type);
			xxObj.data.put("provider_url",provider_url);
			xxObj.data.put("country",country);
			xxObj.data.put("language",language);
			xxObj.data.put("down_date",down_date);
			xxObj.data.put("batch",batch);
			
			
				

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xxObj);
			context.write(new Text(rawid), new BytesWritable(bytes));

		}
	}
}