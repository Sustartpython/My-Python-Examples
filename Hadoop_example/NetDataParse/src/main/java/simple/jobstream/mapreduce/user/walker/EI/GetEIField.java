package simple.jobstream.mapreduce.user.walker.EI;

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

//统计ei的数据量
public class GetEIField extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;

	private static int reduceNum = 1;
	public static final String inputHdfsPath = "/RawData/EI/latest";
	public static final String outputHdfsPath = "/vipuser/walker/output/GetEIField";
	

	
	public void pre(Job job) {
		String jobName = "GetEIField";
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

		public void map(Text key, BytesWritable value, Context context)
				throws IOException, InterruptedException {
			
			String rawid = "";
			String AccessionNumber = "";
			String Title = "";
			String Authors = "";
			String AuthorAffiliation = "";
			String CorrAuthorAffiliation = "";
			String CorrespondingAuthor = "";
			String SourceTitle = "";
			String Volume = "";
			String Issue = "";
			String Abstract = "";
			String CODEN = "";
			String ISSN = "";
			String EISSN = "";
			String ISBN13 = "";
			String DocumentType = "";
			String DOI = "";
			String ControlledTerms = "";
			String UncontrolledTterms = "";
			String MainHeading = "";
			String Pages = "";
			String ConferenceLocation = "";
			String IssueDate = "";
			String ConferenceDate = "";
			String Sponsor = "";
			String Publisher = "";
			
			XXXXObject xObj = new XXXXObject();
			VipcloudUtil.DeserializeObject(value.getBytes(), xObj);
			for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
				if (updateItem.getKey().equals("rawid")) {
					rawid = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AccessionNumber")) {
					AccessionNumber = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Title")) {
					Title = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Authors")) {
					Authors = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("AuthorAffiliation")) {
					AuthorAffiliation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CorrAuthorAffiliation")) {
					CorrAuthorAffiliation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CorrespondingAuthor")) {
					CorrespondingAuthor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("SourceTitle")) {
					SourceTitle = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Volume")) {
					Volume = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Issue")) {
					Issue = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Abstract")) {
					Abstract = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("CODEN")) {
					CODEN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ISSN")) {
					ISSN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("EISSN")) {
					EISSN = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ISBN13")) {
					ISBN13 = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DocumentType")) {
					DocumentType = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("DOI")) {
					DOI = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ControlledTerms")) {
					ControlledTerms = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("UncontrolledTterms")) {
					UncontrolledTterms = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("MainHeading")) {
					MainHeading = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Pages")) {
					Pages = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ConferenceLocation")) {
					ConferenceLocation = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("IssueDate")) {
					IssueDate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("ConferenceDate")) {
					ConferenceDate = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Sponsor")) {
					Sponsor = updateItem.getValue().trim();
				}
				else if (updateItem.getKey().equals("Publisher")) {
					Publisher = updateItem.getValue().trim();
				}
			}
			
			String lineOutput = CODEN;
			
			context.getCounter("map", "count").increment(1);
			
			context.write(new Text(lineOutput), NullWritable.get());
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