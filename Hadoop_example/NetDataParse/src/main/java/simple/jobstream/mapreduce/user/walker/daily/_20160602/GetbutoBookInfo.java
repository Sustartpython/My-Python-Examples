package simple.jobstream.mapreduce.user.walker.daily._20160602;

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
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;


public class GetbutoBookInfo extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;

	private static Map<String, String> maplngID = new HashMap<String, String>();
	private static int reduceNum = 1;
	
	public static final String inputHdfsPath = "/RawData/CQU/YD/BOOKS";
	//public static final String inputHdfsPath = "/VipProcessData/BasicObject/TitleObject";
	//public static final String inputHdfsPath = "/VipProcessData/BasicInfo/TitleInfo/TitleInfo";
	public static final String outputHdfsPath = "/vipuser/walker/output/20160602/GetbutoBookInfo";
	

	
	public void pre(Job job) {
		String jobName = "GetbutoBookInfo";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		
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
		job.setOutputFormatClass(TextOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(NullWritable.class);

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
			Mapper<Text, BytesWritable, Text, NullWritable> {
		
		public void setup(Context context) throws IOException,
				InterruptedException {
	        // 获取HDFS文件系统  
	        FileSystem fs = FileSystem.get(context.getConfiguration());
	  
	        FSDataInputStream fin = fs.open(new Path("/vipuser/walker/input/108425.txt")); 
	        BufferedReader in = null;
	        String line;
	        try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.trim();
		        	if (line.length() < 2) {
						continue;
					}
		        	maplngID.put(line, "");
		        }
	        } finally {
		        if (in!= null) {
		        	in.close();
		        }
	        }
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
		        String pathfile = "/vipuser/walker/log/log_map/" + nowDate + ".txt";
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
			/*
			XXXXObject xxxobj = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), xxxobj);
			*/
			XXXXObject bxObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(values.getBytes(), bxObj);
			
			String bookID = "";
			String print_isbn  = "";

			for (Map.Entry<String, String> updateItem : bxObj.data.entrySet()) {
				if (updateItem.getKey().equals("ID")) {
					bookID = updateItem.getValue();
					bookID = bookID.replace('\\', ' ').toUpperCase().trim();
				}
				
				if (updateItem.getKey().equals("PRINT_ISBN")) {
					print_isbn = updateItem.getValue();
					print_isbn = print_isbn.replace('\\', ' ').toUpperCase().trim();
				}
			}
			
			String[] vec = print_isbn.split(",");
			for (int i = 0; i < vec.length; i++) {
				String isbn = vec[i].trim();
				if (isbn.length() < 10) {
					context.getCounter("map", "isbn<10").increment(1);
					continue;
				}
				
				String lineOutput = bookID + "\t" + vec[i].trim();
				context.getCounter("map", "count").increment(1);
				context.write(new Text(lineOutput), NullWritable.get());
			}
		}

	}

	public static class ProcessReducer extends
			Reducer<Text, NullWritable, Text, NullWritable> {

		public void reduce(Text key, Iterable<NullWritable> values,
				Context context) throws IOException, InterruptedException {
			
			context.getCounter("reduce", "count").increment(1);
			context.write(key, NullWritable.get());
		}

	}
}