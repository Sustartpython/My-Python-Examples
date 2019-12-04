package simple.jobstream.mapreduce.site.wf_qk;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;

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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

/**
 * <p>Description: 根据某些特殊需求导出db3 </p>  
 * @author qiuhongyang 2018年11月27日 下午1:58:00
 */
public class Std2Db3Filter extends InHdfsOutHdfsJobInfo {
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
		
		private static String rawid = "";
		private static String lngid = "";
		private static String bookid = "";
		private static String pykm = "";
		private static String issn = "";
		private static String cnno = "";
		private static String title_c = "";
		private static String title_e = "";
		private static String remark_c = "";
		private static String remark_e = "";
		private static String doi = "";
		private static String author_c = "";
		private static String author_e = "";		
		private static String firstwriter = "";	
		private static String showwriter  = "";	
		private static String cbmwriter = "";	
		private static String writer = "";	
		private static String organ = "";
		private static String firstorgan = "";	
		private static String showorgan = "";		
		private static String name_c = "";
		private static String name_e = "";
		private static String years = "";
		private static String vol = "";
		private static String num = "";
		private static String sClass = "";
		private static String firstclass = "";
		private static String auto_class = "";
		private static String keyword_c = "";
		private static String keyword_e = "";
		private static String imburse = "";
		private static String pageline = "";
		private static String pagecount = "";
		private static String ref_cnt = "";
		private static String cited_cnt = "";
		private static String beginpage = "";
		private static String endpage = "";
		private static String jumppage = "";
		private static String muinfo = "";
		private static String pub1st = "0";		//是否优先出版
		private static String fromtype = "WANFANG";
		private static String down_date = "";
		
		private static Map<String, String> mapJournalID = new HashMap<String, String>();
		public void setup(Context context) throws IOException,
				InterruptedException {
	        // 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/detail/_ref_file/journal_id.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.toLowerCase().trim();
		        	if (line.length() < 1) {
						continue;
					}
		        	mapJournalID.put(line, "");
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }
	        System.out.println("mapJournalID size: " + mapJournalID.size());
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
		        String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
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
			String lngID = "Wd";
			for (int i = 0; i < wanID.length(); i++) {
				lngID += String.format("%d", wanID.charAt(i) + 0);
			}
			
			return lngID;
		}
		
		private static String getBookId(String pykm, String years, String num) {
			String bookid = "";
			String line = pykm + years;
			if (0 == num.length()) {
				line += "00";
			}
			else if (1 == num.length()) {
				line += "0";
			}
			line += num;
			bookid = getLngIDByWanID(line);
			
			return bookid;
		}
		
		private String[] parsePageInfo(String line) {
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			
			int idx = line.indexOf(',');
			if (idx > 0) {
				jumppage = line.substring(idx+1).trim();
				line = line.substring(0, idx).trim();	//去掉加号及以后部分
			}
			idx = line.indexOf('-');
			if (idx > 0) {
				endpage = line.substring(idx+1).trim();
				line = line.substring(0, idx).trim();	//去掉减号及以后部分
			}
			beginpage = line.trim();
			if (endpage.length() < 1) {
				endpage = beginpage;
			}
			
			String[] vec = {beginpage, endpage, jumppage};
			return vec;
		}
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
				
			{
				rawid = "";
				lngid = "";
				bookid = "";
				pykm = "";
				issn = "";
				cnno = "";
				title_c = "";
				title_e = "";
				remark_c = "";
				remark_e = "";
				doi = "";
				author_c = "";
				author_e = "";		
				firstwriter = "";	
				showwriter  = "";	
				cbmwriter = "";	
				writer = "";	
				organ = "";
				firstorgan = "";	
				showorgan = "";		
				name_c = "";
				name_e = "";
				years = "";
				vol = "";
				num = "";
				sClass = "";
				firstclass = "";
				auto_class = "";
				keyword_c = "";
				keyword_e = "";
				imburse = "";
				pageline = "";
				pagecount = "";
				ref_cnt = "";
				cited_cnt = "";
				beginpage = "";
				endpage = "";
				jumppage = "";
				muinfo = "";
				pub1st = "0";		//是否优先出版
				fromtype = "WANFANG";
				down_date = "";
			}
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cnno")) {
					cnno = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_e")) {
					remark_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_c")) {
					author_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_e")) {
					author_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_c")) {
					name_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_e")) {
					name_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("vol")) {
					vol = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("sClass")) {
					sClass = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("auto_class")) {
					auto_class = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_e")) {
					keyword_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("imburse")) {
					imburse = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pageline")) {
					pageline = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pagecount")) {
					pagecount = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("muinfo")) {
					muinfo = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub1st")) {
					pub1st = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}
			}

		
			showwriter = author_c.length() > 0 ? author_c : author_e;
			showorgan = organ;
			lngid = getLngIDByWanID(rawid);		
			bookid = getBookId(pykm, years, num);
			sClass = sClass.replaceAll("\\s+", ";");
			String[] vec = sClass.split(";");
			if (vec.length > 0) {
				firstclass = vec[0].trim();
			}
			vec = showwriter.split(";");
			if (vec.length > 0) {
				firstwriter = vec[0].trim();
				firstwriter = firstwriter.replaceAll("\\[.*?\\]$", "");		//去掉后面的标号
			}
			writer = cbmwriter = showwriter;
			//writer = showwriter.replace(';', ' ');
			
			vec = showorgan.split(";");
			if (vec.length > 0) {
				firstorgan = vec[0].trim();
				firstorgan = firstorgan.replaceAll("^\\[.*?\\]", "");		//去掉前面的标号
			}
			
			vec = parsePageInfo(pageline);
			beginpage = vec[0];
			endpage = vec[1];
			jumppage = vec[2];
			
			// 补缺
			{
				if (title_c.length() < 1) {
					title_c = title_e;
				}
			}
			
			if (mapJournalID.containsKey(pykm.toLowerCase())) {				
				return;
			}
			else {
				context.getCounter("map", "not active").increment(1);
				lngid = "_delete_wangfangjournal" + lngid;
			}
			
			//转义
			{
				issn = issn.replace('\0', ' ').replace("'", "''").trim();	
				cnno = cnno.replace('\0', ' ').replace("'", "''").trim();	
				title_c = title_c.replace('\0', ' ').replace("'", "''").trim();	
				title_e = title_e.replace('\0', ' ').replace("'", "''").trim();	
				author_c = author_c.replace('\0', ' ').replace("'", "''").trim();	
				author_e = author_e.replace('\0', ' ').replace("'", "''").trim();	
				firstwriter = firstwriter.replace('\0', ' ').replace("'", "''").trim();	
				showwriter  = showwriter.replace('\0', ' ').replace("'", "''").trim();	
				cbmwriter = cbmwriter.replace('\0', ' ').replace("'", "''").trim();
				writer = writer.replace('\0', ' ').replace("'", "''").trim();
				firstorgan = firstorgan.replace('\0', ' ').replace("'", "''").trim();	
				showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();	
				organ = organ.replace('\0', ' ').replace("'", "''").trim();	
				name_c = name_c.replace('\0', ' ').replace("'", "''").trim();	
				name_e = name_e.replace('\0', ' ').replace("'", "''").trim();	
				remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();	
				remark_e = remark_e.replace('\0', ' ').replace("'", "''").trim();	
				keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();	
				keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").trim();	
				imburse = imburse.replace('\0', ' ').replace("'", "''").trim();	
				doi = doi.replace('\0', ' ').replace("'", "''").trim();	
				sClass = sClass.replace('\0', ' ').replace("'", "''").trim();	
				auto_class = auto_class.replace('\0', ' ').replace("'", "''").trim();	
				firstclass = firstclass.replace('\0', ' ').replace("'", "''").trim();	
				years = years.replace('\0', ' ').replace("'", "''").trim();	
				vol = vol.replace('\0', ' ').replace("'", "''").trim();	
				num = num.replace('\0', ' ').replace("'", "''").trim();	
				pageline = pageline.replace('\0', ' ').replace("'", "''").trim();	
				pagecount = pagecount.replace('\0', ' ').replace("'", "''").trim();	
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();	
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();	
				beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
				endpage = endpage.replace('\0', ' ').replace("'", "''").trim();
				jumppage = jumppage.replace('\0', ' ').replace("'", "''").trim();
				muinfo = muinfo.replace('\0', ' ').replace("'", "''").trim();
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();
			}
					   
			String sql = "INSERT INTO main([lngid], [bookid], [issn], [cnno], [rawid], [pub1st], "
					+ "[muinfo], [pageline], [pagecount], [ref_cnt], [cited_cnt], "
					+ "[beginpage], [endpage], [jumppage], [qid], [title_c], [title_e], "
					+ "[firstwriter], [showwriter], [cbmwriter], [writer], [author_e], "
					+ "[firstorgan], [organ], [name_c], [name_e], [showorgan], "
					+ "[remark_c], [remark_e], [keyword_c], [keyword_e], [imburse], "
					+ "[doi], [class], [auto_class],[firstclass], [years], [vol], [num], "
					+ "[fromtype], [down_date]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', "
					+ "'%s', '%s');";
			sql = String.format(sql, lngid, bookid, issn, cnno, rawid, pub1st, 
					muinfo, pageline, pagecount, ref_cnt, cited_cnt, 
					beginpage, endpage, jumppage, pykm, title_c, title_e, 
					firstwriter, showwriter, cbmwriter, writer, author_e, 
					firstorgan, organ, name_c, name_e, showorgan, 
					remark_c, remark_e, keyword_c, keyword_e, imburse, 
					doi, sClass, auto_class, firstclass, years, vol, num, 
					fromtype, down_date);								
			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());			
		}
	}

}