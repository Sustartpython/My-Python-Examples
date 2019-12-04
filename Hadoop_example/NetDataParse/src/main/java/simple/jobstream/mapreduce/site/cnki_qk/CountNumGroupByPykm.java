package simple.jobstream.mapreduce.site.cnki_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

/**
 * <p>Description: 统计某刊某年发行了几本 </p>  
 * @author qiuhongyang 2018年11月21日 上午9:35:09
 */
public class CountNumGroupByPykm extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		
		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
	}

	public void post(Job job) {

	}
	
	@Override
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	@Override
	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		
		job.setNumReduceTasks(reduceNum);
	}

	// ======================================处理逻辑=======================================
	public static class ProcessMapper extends
			Mapper<Text, BytesWritable, Text, Text> {
		
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
		
		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String rawid = "";
			String pykm = "";
			String title_c = "";
			String title_e = "";
			String author_c = "";
			String author_e = "";
			String organ = "";
			String remark_c = "";
			String keyword_c = "";
			String imburse = "";	//基金
			String muinfo = "";
			String doi = "";
			String sClass = "";
			String name_c = "";
			String name_e = "";
			String issn = "";
			String years = "";
			String num = "";
			String pageline = "";
			String pub1st = "0";		//是否优先出版
			String sentdate = (new SimpleDateFormat("yyyyMMdd")).format(new Date());
			String fromtype = "CNKI";
			String beginpage = "";
			String endpage = "";
			String jumppage = "";
			String if_html_fulltext = "0";
			String down_cnt = "0";			// 下载量
			String cite_cnt = "0";		// 被引量
			
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
				else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("remark_c")) {
					remark_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("keyword_c")) {
					keyword_c = updateItem.getValue().trim();
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
				else if (updateItem.getKey().equals("pub1st")) {
					pub1st = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("if_html_fulltext")) {
					if_html_fulltext = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("down_cnt")) {
					down_cnt = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("cite_cnt")) {
					cite_cnt = updateItem.getValue().trim();
				}
			}
			
			
			String lngid = getLngIDByCnkiID(rawid);		
			String cnkiid = rawid;
			String bid = pykm;
			String bookid = lngid.substring(0, 20);
			if (lngid.length() < 26) {	//九几年的老规则，有点，比较短（WAVE803.010）
				bookid = lngid.substring(0, lngid.length()-6);
			}
			
			String outKey = pykm + "\t" + years;
			String outValue = num;
			
			context.getCounter("map", "count").increment(1);
			context.write(new Text(outKey), new Text(outValue));
		}

	}

	public static class ProcessReducer extends
			Reducer<Text, Text, Text, NullWritable> {
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {			
			
			String outLine = key.toString(); 
			HashSet<String> numSet = new HashSet<String>();
			for (Text val : values) {
				String num = val.toString();
				numSet.add(num);
			}
			outLine += "\t" + numSet.size();
			
			context.getCounter("reduce", "count").increment(1);
			context.write(new Text(outLine), NullWritable.get());
		}

	}


}