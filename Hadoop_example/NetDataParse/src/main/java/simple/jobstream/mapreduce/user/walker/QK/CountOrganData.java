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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

public class CountOrganData extends InHdfsOutHdfsJobInfo {
	private static boolean bBCP = true;
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 1;
	//public static final String inputHdfsPath = "/RawData/wanfang/qk/detail/XXXXObject";
	public static final String inputHdfsPath = "/user/ganruoxun/CNKIBasicInfo/TitleInfo/TitleInfo";
	public static final String outputHdfsPath = "/vipuser/walker/output/QK/CountOrganData";
	
	public void pre(Job job) {
		String jobName = "CountOrganData";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
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

		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
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

			String lngid = key.toString().trim();
			String type = "";
			String language  = "";
			String srcid = "";
			String libid = "";
			
			String bookid = "";
			String gch = "";
			String years = "";
			//String vol = "";
			String num = "";
			String gch5 = "";
			String showorgan = "";
			String organ = "";
			String title_c = "";

			for (Map.Entry<String, String> updateItem : xxxobj.data.entrySet()) {
				if (updateItem.getKey().equals("type")) {
					type = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("language")) {
					language = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("srcid")) {
					srcid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("libid")) {
					libid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("bookid")) {
					bookid = updateItem.getValue().trim();
				}else if (updateItem.getKey().equals("gch")) {
					gch = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("years")) {
					years = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("num")) {
					num = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("gch5")) {
					gch5 = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("showorgan")) {
					showorgan = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("organ")) {
					organ = updateItem.getValue().trim();
				} else if (updateItem.getKey().equals("title_c")) {
					title_c = updateItem.getValue().trim();
				}
			}
			
			/*
			if (!libid.equals("") && !libid.toUpperCase().equals("VIP")) {
				return;
			}
			
			if (srcid.toUpperCase().indexOf("VIP") < 0) {
				return;
			}
			
			if (!type.equals("1") || !language.equals("1")) {	//中刊
				return;
			}
			
			
			context.getCounter("map", "vip zk").increment(1);
			
			if (num.length() == 1) {
				num = "0" + num;
			}
			
			if (gch.length() > 5) {
				gch5 = gch.substring(0, 5);
			}
			
			if (!years.equals("2016")) {
				return;
			}
			
			if (!gch.toUpperCase().equals("92335X")) {
				return;
			}
			*/
			
			
			
			context.getCounter("map", "in_count").increment(1);
			
			if ((showorgan.indexOf("清华大学") > -1) || (organ.indexOf("清华大学") > -1)) {
				context.getCounter("map", "清华大学").increment(1);
			}
			
			if ((showorgan.indexOf("泉州师范学院") > -1) || (organ.indexOf("泉州师范学院") > -1)) {
				context.getCounter("map", "泉州师范学院").increment(1);
			}
						
			if ((showorgan.indexOf("泉州师范学院") < 0) && (organ.indexOf("泉州师范学院") < 0)) {
				return;
			}
			
			String outKey = years;
			String outValue = lngid;
			
			context.getCounter("map", "count").increment(1);
			context.write(new Text(outKey), new Text(outValue));
		}

	}

	public static class ProcessReducer extends
			Reducer<Text, Text, Text, NullWritable> {

		public static int cntLine = 0;	//记录进入reduce的次数
		
		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException {		
			
			int cnt = 0;

			for (Text val : values) {
				++cnt;
			}
			
			String outText = key.toString() + "\t" + String.format("%d", cnt);
			
			context.getCounter("reduce", "count").increment(1);
			context.write(new Text(outText), NullWritable.get());
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