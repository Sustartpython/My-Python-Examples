package simple.jobstream.mapreduce.site.cnki_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

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

//输入应该为去重后的html
public class Std2Db3 extends InHdfsOutHdfsJobInfo {
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
		        String pathfile = "/walker/log/log_map/" + nowDate + ".txt";
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
		
		private static String getLngIDByCnkiID(String cnkiID) {
			cnkiID = cnkiID.toUpperCase();
			String lngID = "";
			for (int i = 0; i < cnkiID.length(); i++) {
				lngID += String.format("%d", cnkiID.charAt(i) + 0);
			}
			
			return lngID;
		}
		
		private String[] parsePageInfo(String line) {
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			
			int idx = line.indexOf('+');
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
				
			String rawid = "";
			String pykm = "";
			String title_c = "";
			String title_e = "";
			String author_c = "";
			String author_e = "";
			String corr_author = "";
			String author_id = "";	
			String organ = "";
			String organ_id = "";
			String remark_c = "";
			String remark_e = "";
			String keyword_c = "";
			String keyword_e = "";
			String imburse = "";	//基金
			String muinfo = "";
			String doi = "";
			String sClass = "";
			String process_date = ""; //在线出版日期（online date）
			String down_date = "";
			String name_c = "";
			String name_e = "";
			String issn = "";
			String years = "";
			String num = "";
			String pageline = "";
			String pagecount = ""; // 页数
			String pub1st = "0";		//是否优先出版
			String sentdate = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
			String fromtype = "CNKI";
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			String if_html_fulltext = "0";
			String fulltext_type = "";
			String down_cnt = "0";			// 下载量
			String ref_cnt  = "0";			// 引文量
			String cited_cnt = "0";		// 被引量
			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];  
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());  
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_c")) {
					author_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_e")) {
					author_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("corr_author")) {
					corr_author = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("author_id")) {
					author_id = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("organ_id")) {
					organ_id = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_e")) {
					remark_e = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("muinfo")) {
					muinfo = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("doi")) {
					doi = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("sClass")) {
					sClass = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("process_date")) {
					process_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_date")) {
					down_date = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_c")) {
					name_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("name_e")) {
					name_e = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("issn")) {
					issn = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pageline")) {
					pageline = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pagecount")) {
					pagecount = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pub1st")) {
					pub1st = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("if_html_fulltext")) {
					if_html_fulltext = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("fulltext_type")) {
					fulltext_type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_cnt")) {
					down_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ref_cnt")) {
					ref_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cited_cnt")) {
					cited_cnt = updateItem.getValue().trim();
				}
			}
			
			
			String showwriter = author_c.length() > 0 ? author_c : author_e;
			String showorgan = organ;
			String lngid = getLngIDByCnkiID(rawid);		
			String cnkiid = rawid;
			String bid = pykm;
			String bookid = lngid.substring(0, 20);
			if (lngid.length() < 26) {	//九几年的老规则，有点，比较短（WAVE803.010）
				bookid = lngid.substring(0, lngid.length()-6);
			}
			
			if (title_c.length() < 1) {
				title_c = title_e;
			}			
			String firstclass = "";
			String firstwriter = "";
			String firstorgan = "";
			sClass = sClass.replaceAll("\\s+", ";");
			String[] vec = sClass.split(";");
			if (vec.length > 0) {
				firstclass = vec[0].trim();
			}
			vec = showwriter.split(";");
			if (vec.length > 0) {
				firstwriter = vec[0].trim();
			}
			vec = showorgan.split(";");
			if (vec.length > 0) {
				firstorgan = vec[0].trim();
			}
			String cbmwriter = showwriter;
//			String writer = showwriter.replace(';', ' ');
			String writer = showwriter;
			
			if (remark_c.startsWith("<正>")) {
				remark_c = remark_c.substring("<正>".length());
			}

			vec = parsePageInfo(pageline);
			beginpage = vec[0];
			endpage = vec[1];
			jumppage = vec[2];
			
			
			//转义
			{
				title_c = title_c.replace('\0', ' ').replace("'", "''").trim();	
				title_e = title_e.replace('\0', ' ').replace("'", "''").trim();	
				author_c = author_c.replace('\0', ' ').replace("'", "''").trim();	
				author_e = author_e.replace('\0', ' ').replace("'", "''").trim();	
				corr_author = corr_author.replace('\0', ' ').replace("'", "''").trim();	
				author_id = author_id.replace('\0', ' ').replace("'", "''").trim();	
				firstwriter = firstwriter.replace('\0', ' ').replace("'", "''").trim();	
				showwriter  = showwriter.replace('\0', ' ').replace("'", "''").trim();	
				cbmwriter = cbmwriter.replace('\0', ' ').replace("'", "''").trim();	
				writer = writer.replace('\0', ' ').replace("'", "''").trim();	
				firstorgan = firstorgan.replace('\0', ' ').replace("'", "''").trim();	
				showorgan = showorgan.replace('\0', ' ').replace("'", "''").trim();	
				organ = organ.replace('\0', ' ').replace("'", "''").trim();	
				organ_id = organ_id.replace('\0', ' ').replace("'", "''").trim();	
				remark_c = remark_c.replace('\0', ' ').replace("'", "''").trim();	
				remark_e = remark_e.replace('\0', ' ').replace("'", "''").trim();	
				keyword_c = keyword_c.replace('\0', ' ').replace("'", "''").trim();	
				keyword_e = keyword_e.replace('\0', ' ').replace("'", "''").trim();	
				imburse = imburse.replace('\0', ' ').replace("'", "''").trim();	
				muinfo = muinfo.replace('\0', ' ').replace("'", "''").trim();	
				doi = doi.replace('\0', ' ').replace("'", "''").trim();	
				sClass = sClass.replace('\0', ' ').replace("'", "''").trim();	
				firstclass = firstclass.replace('\0', ' ').replace("'", "''").trim();	
				process_date = process_date.replace('\0', ' ').replace("'", "''").trim();	
				down_date = down_date.replace('\0', ' ').replace("'", "''").trim();	
				name_c = name_c.replace('\0', ' ').replace("'", "''").trim();	
				name_e = name_e.replace('\0', ' ').replace("'", "''").trim();	
				issn = issn.replace('\0', ' ').replace("'", "''").trim();	
				years = years.replace('\0', ' ').replace("'", "''").trim();	
				num = num.replace('\0', ' ').replace("'", "''").trim();	
				pageline = pageline.replace('\0', ' ').replace("'", "''").trim();	
				pagecount = pagecount.replace('\0', ' ').replace("'", "''").trim();	
				beginpage = beginpage.replace('\0', ' ').replace("'", "''").trim();
				endpage = endpage.replace('\0', ' ').replace("'", "''").trim();
				jumppage = jumppage.replace('\0', ' ').replace("'", "''").trim();
				if_html_fulltext = if_html_fulltext.replace('\0', ' ').replace("'", "''").trim();
				fulltext_type = fulltext_type.replace('\0', ' ').replace("'", "''").trim();
				down_cnt = down_cnt.replace('\0', ' ').replace("'", "''").trim();
				ref_cnt = ref_cnt.replace('\0', ' ').replace("'", "''").trim();
				cited_cnt = cited_cnt.replace('\0', ' ').replace("'", "''").trim();
			}
								   
			String sql = "INSERT INTO main([lngid], [rawid], [bookid], [cnkiid], [bid], "
					+ "[title_c], [title_e], [firstwriter], [showwriter], [cbmwriter], [corr_author], [author_id], "
					+ "[writer], [author_e], [firstorgan], [organ], [showorgan], [organ_id],"
					+ "[remark_c], [remark_e], [keyword_c], [keyword_e], [imburse], "
					+ "[muinfo], [doi], [class], [firstclass], [process_date], [down_date], [name_c], [name_e], "
					+ "[issn], [years], [num], [pageline], [pagecount], [pub1st], "
					+ "[sentdate], [fromtype], [beginpage], [endpage], [jumppage], "
					+ "[if_html_fulltext], [fulltext_type], [down_cnt], [ref_cnt], [cited_cnt]) ";
			sql += " VALUES ('%s', '%s', '%s', '%s', '%s', '%s','%s', '%s', '%s', '%s',  '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',  '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s',  '%s', "
					+ "'%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s', '%s');";
			
			sql = String.format(sql, lngid, rawid, bookid, cnkiid, bid, 
					title_c, title_e, firstwriter, showwriter, cbmwriter, corr_author, author_id,
					writer, author_e, firstorgan, organ, showorgan, organ_id,
					remark_c, remark_e, keyword_c, keyword_e, imburse, 
					muinfo, doi, sClass, firstclass, process_date, down_date, name_c, name_e, 
					issn, years, num, pageline, pagecount, pub1st, 
					sentdate, fromtype, beginpage, endpage, jumppage, 
					if_html_fulltext, fulltext_type, down_cnt, ref_cnt, cited_cnt);								
			
			context.getCounter("map", "count").increment(1);

			context.write(new Text(sql), NullWritable.get());
			
		}
	}

	
}