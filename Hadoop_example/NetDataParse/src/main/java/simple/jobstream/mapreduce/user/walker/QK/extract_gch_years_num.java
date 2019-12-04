package simple.jobstream.mapreduce.user.walker.QK;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.mockito.internal.matchers.And;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class extract_gch_years_num extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 1;
	public static final String inputHdfsPath = "/RawData/cnki/qk/detail/latest";
	public static final String outputHdfsPath = "/user/qhy/output/QK/extract_gch_years_num";
	
	public void pre(Job job) {
		String jobName = this.getClass().getSimpleName();
		
		job.setJobName(jobName);
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
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
		
		public void map(Text key, BytesWritable values, Context context)
				throws IOException, InterruptedException {
			
			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), xxxobj);

			
			String type = "";
			String language  = "";
			String srcid = "";
			String libid = "";
			String title_c = "";
			String title_e = "";
			
			String bookid = "";
			String pykm = "";
			String years = "";
			//String vol = "";
			String num = "";
			String gch5 = "";

			for (Map.Entry<String, String> updateItem : xxxobj.data.entrySet()) {
				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("title_e")) {
					title_e = updateItem.getValue().trim();
				}				
				else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}				
				else if (updateItem.getKey().equals("srcid")) {
					srcid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("libid")) {
					libid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("bookid")) {
					bookid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("pykm")) {
					pykm = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				}				
				else if (updateItem.getKey().equals("gch5")) {
					gch5 = updateItem.getValue().trim();
				}
			}
			

//			if (!libid.equals("") && !libid.toUpperCase().equals("VIP")) {
//				return;
//			}
//			
//			boolean bVIP = false;			
//			for (String item : srcid.split(";")) {
//				item = item.toUpperCase().trim();
//				if (item.equals("VIP")) {
//					bVIP = true;
//				}
//			}
//			if (!bVIP) {
//				if (srcid.toUpperCase().indexOf("VIP") > 0) {
//					context.getCounter("map", "not whole VIP").increment(1);
//				}
//				
//				return;
//			}
//			
//			if (!type.equals("1") || !language.equals("1")) {	//中刊
//				return;
//			}
//			context.getCounter("map", "vip zk").increment(1);
//			
//			if (num.length() == 1) {
//				num = "0" + num;
//			}
//			
//			if (pykm.length() > 5) {
//				gch5 = pykm.substring(0, 5);
//			}
			
			if ((!title_c.endsWith("简")) && (!title_c.endsWith("简"))) {
				return;
			}
			
			context.getCounter("map", "简").increment(1);
			
			String outKey = pykm + "\t" + years + "\t" + num;
			String outValue = bookid;
			
			context.write(new Text(outKey), new Text(outValue));
		}

	}

	public static class ProcessReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		public static int cntLine = 0;	//记录进入reduce的次数
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {			
			
			String bookid = "xxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxxx";	//搞个大值便于后面筛选小值
			
			for (Text val : values) {
				String line = val.toString().trim();
				if (line.length() < bookid.length()) {
					bookid = line;
				}
			}
			
//			String outText = bookid + "\t" + key.toString() + "\r";
			
			context.getCounter("reduce", "count").increment(1);
			context.write(key, NullWritable.get());
		}

	}

	@Override
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	@Override
	public String getHdfsOutput() {
		return outputHdfsPath;
	}
}