package simple.jobstream.mapreduce.common.vip;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.net.URI;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Collections;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileStatus;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.FileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.JobInfoProcessBase;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.EmptyInputFormat;
import com.process.frame.util.EmptySplit;
import com.process.frame.util.VipcloudUtil;


/**
 * <p>Description: 将 db3 数据转成 XXXXObject 导入到 hdfs </p>  
 * @author qiuhongyang 2018年12月1日 下午4:01:33
 */
public class Sqlite2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static Logger logger = Logger.getLogger(Sqlite2XXXXObject.class);
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";
	public static int memGb = 2;
	public static int taskNum = 10;
	public static String jobName = "";

	public void pre(Job job) {
		jobName = job.getConfiguration().get("jobName");		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		
		job.setJobName(jobName);
		
		logger.info("pre job finished ......... input=" + inputHdfsPath + " ; output=" + outputHdfsPath);
	}

	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job) {
		job.setMapperClass(Sqlite2XXXXObject.ProcessMapper.class);
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);
		job.setInputFormatClass(EmptyInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);
		SequenceFileOutputFormat.setCompressOutput(job, false);
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);

		try {
			FileInputFormat.addInputPath(job, new Path("ignored"));
		} catch (IOException e1) {
			e1.printStackTrace();
		}

		// 确定文件个数据,以确定MAP数据
		try {
			if (inputHdfsPath != null) {
				Configuration conf = job.getConfiguration();
				URI uri = URI.create(conf.get("fs.default.name"));
				FileSystem hdfs = FileSystem.get(uri, conf);
				String ProcRootDir = conf.get("vipcloud.hdfs.proc.root.dir");
				System.out.println("SetMRInfo ProcRootDir:" + ProcRootDir);
				ArrayList<String> allPath = new ArrayList<String>();
				String paths[] = inputHdfsPath.split(",");
				for (String path : paths) {
					Path inPathDir = new Path(ProcRootDir + path);
					countValidPath(inPathDir, hdfs, allPath);
				}
				System.out.println("map num is :" + allPath.size());
				((JobConf) job.getConfiguration()).setNumMapTasks(allPath.size());
			}
//			logger.info("JobInfoProcessBase.SetHdfsOutputPath begin");
			JobInfoProcessBase.SetHdfsOutputPath(job, outputHdfsPath);
//			logger.info("JobInfoProcessBase.SetHdfsOutputPath end");
		} catch (Exception e) {
		}

		logger.info("SetMRInfo finished ........ ");
	}

	public void post(Job job) {
	}

	public static void countValidPath(Path path, FileSystem fs, ArrayList<String> allPath) throws IOException {

		if (fs.isDirectory(path)) {
			FileStatus[] status = fs.listStatus(path);
			for (FileStatus file : status) {
				Path temPath = file.getPath();
				countValidPath(temPath, fs, allPath);
			}
		} else {
			if (path.getName().endsWith(".db3")) {
				allPath.add(path.toString());
			}
		}

	}

	public static class ProcessMapper extends Mapper<IntWritable, IntWritable, Text, BytesWritable> {
		protected String sqlDataPath;

		private String tableName = "";
		private String keyField = "";
		private static FileSystem hdfs = null;

		@Override
		protected void setup(Context context) throws IOException, InterruptedException {
			String taskId = context.getConfiguration().get("mapred.task.id");
			String JobDir = context.getConfiguration().get("job.local.dir");
			String taskTmpDir = JobDir + File.separator + taskId;
			String tmpSqlDir = taskTmpDir + File.separator + "sqldata";
			int filePos = ((EmptySplit) context.getInputSplit()).getSplitNum();
			String inputHdfsPath = context.getConfiguration().get("inputHdfsPath");  // 不能感知到外部类的变量

			// ====================================================
			Configuration conf = context.getConfiguration();
			URI uri = URI.create(conf.get("fs.default.name"));
			hdfs = FileSystem.get(uri, conf);
			String processsqlFile = getProcessSqlDb(hdfs, context, inputHdfsPath, filePos);
			System.out.println("setup processsqlFile: " + processsqlFile);
			if (processsqlFile != null) {
				Path src = new Path(processsqlFile);
				Path dst = new Path(tmpSqlDir + File.separator + src.getName());
				System.out.println("setup src: " + src);
				System.out.println("setup dst: " + dst);
				hdfs.copyToLocalFile(false, src, dst);
				sqlDataPath = dst.toString();
			}
			keyField = context.getConfiguration().get("keyField");
			tableName = context.getConfiguration().get("tableName");
		}

		protected String getProcessSqlDb(FileSystem hdfs, Context context, String inputPath, int filePos)
				throws IOException {
			Configuration conf = context.getConfiguration();
			String ProcRootDir = conf.get("vipcloud.hdfs.proc.root.dir");

			System.out.println("getProcessSqlDb ProcRootDir: " + ProcRootDir);
			System.out.println("getProcessSqlDb inputPath: " + inputPath);
			ArrayList<String> allPath = new ArrayList<String>(100);
			if (inputPath == null)
				inputPath = "";
			String paths[] = inputPath.split(",");
			for (String path : paths) {
				Path inPathDir = null;
				if (ProcRootDir == null) {
					inPathDir = new Path(path);
				} else {
					inPathDir = new Path(ProcRootDir + path);
				}
				countValidPath(inPathDir, hdfs, allPath);
			}
			Collections.sort(allPath);
			if (allPath.size() > filePos)
				return allPath.get(filePos);
			return null;
		}

		public void map(IntWritable key, IntWritable values, Context context) throws IOException, InterruptedException {
			logger.info("======开始进行SQL数据导入======== " + sqlDataPath);
			File file = new File(sqlDataPath);
			if (!file.exists() && file.isDirectory() && !sqlDataPath.endsWith(".db3")) {
				throw new FileNotFoundException(sqlDataPath + " is not a sqlite db3 file");
			}

			try {
				Class.forName("org.sqlite.JDBC");
				Connection conn = DriverManager.getConnection("jdbc:sqlite:" + file);
				Statement st = conn.createStatement();

				ResultSet rs = st.executeQuery("SELECT * FROM " + tableName);
				ResultSetMetaData rsmd = rs.getMetaData();

				int keyId = -1;
				for (int i = 1; i <= rsmd.getColumnCount(); ++i) {
					if (rsmd.getColumnName(i).equals(keyField)) {
						keyId = i;
						break;
					}
				}
				if (keyId == -1) {
					logger.warn("no any available data, beacuse it not include primary key " + keyField);
					return;
				}
				while (rs.next()) {
					XXXXObject outObj = new XXXXObject();
					String val = rs.getString(keyId);
					outObj.data.put("_keyid", val);
					for (int i = 1; i <= rsmd.getColumnCount(); ++i) {

						Object valObj = rs.getObject(i);
						String valStr = valObj != null ? valObj.toString() : "";
						if (i != keyId) {
							outObj.data.put(rsmd.getColumnName(i), valStr);
						}
					}
					byte[] outData = VipcloudUtil.SerializeObject(outObj);
					context.write(new Text(outObj.data.get("_keyid")), new BytesWritable(outData));
					context.getCounter("map", "record count").increment(1);
				}

				if (conn != null && !conn.isClosed()) {
					conn.close(); // 关闭sqlite连接
				}
			} catch (Exception ex) {
				System.out.println("*********************: 导出异常");
				ex.printStackTrace();
				throw new InterruptedException(ex.getMessage());
			}
		}

		@Override
		protected void cleanup(Context context) throws IOException, InterruptedException {
			super.cleanup(context);
			File db3File = new File(sqlDataPath);		        
			if (db3File.exists()) {
				if(db3File.delete()) {	//删除db3文件
					logger.info("***** delete successed:" + db3File.toString());
				}
				else {
					logger.info("***** delete failed:" + db3File.toString());
				}
			}		
		}

	}

}
