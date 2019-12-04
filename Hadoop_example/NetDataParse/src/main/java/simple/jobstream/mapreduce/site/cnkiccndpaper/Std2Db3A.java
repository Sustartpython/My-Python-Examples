package simple.jobstream.mapreduce.site.cnkiccndpaper;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DatabaseMetaData;
import java.sql.DriverManager;
import java.sql.ResultSet;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.log4j.Logger;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.SqliteReducer;

// 以A层标准导出数据到db3
public class Std2Db3A extends InHdfsOutHdfsJobInfo {
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******"
				+ job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		// job.setInputFormatClass(SimpleTextInputFormat.class);
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
	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, NullWritable> {
		private static Set<String> fieldSet = new HashSet<String>(); //保存字段名
		private static String table_name = "";
		private static String sqlPart1 = "";
		private static String sqlPart2 = "";
		private static Logger logger = Logger.getLogger(Std2Db3A.class);
		private static FileSystem hdfs = null;
		private static String db3PathFile = "";

		public void setup(Context context) throws IOException, InterruptedException {
			try {
				Class.forName("org.sqlite.JDBC");
				String tempFileDb3 = context.getConfiguration().get("tempFileDb3");
				String taskId = context.getConfiguration().get("mapred.task.id");
				String JobDir = context.getConfiguration().get("job.local.dir");
				String taskTmpDir = JobDir + File.separator + taskId;
				String tmpSqlDir = taskTmpDir + File.separator + "sqldata";
				hdfs = FileSystem.get(context.getConfiguration());
				
				db3PathFile = tmpSqlDir + File.separator + "template.db3";
				Path src = new Path(tempFileDb3);	//模板文件（HDFS路径）
				Path dst = new Path(db3PathFile);	//local路径
				hdfs.copyToLocalFile(src, dst);
				
				Connection connSqlite = DriverManager.getConnection("jdbc:sqlite:"+db3PathFile);
				DatabaseMetaData metaData = connSqlite.getMetaData();
		        //获得表信息
			    ResultSet tables = metaData.getTables(null, null, null, new String[]{"TABLE"});

			    logger.info("***** start:" + tempFileDb3);	
		        while (tables.next()) {	
		            //获得表名
		            table_name = tables.getString("TABLE_NAME");
		            //通过表名获得所有字段名
		            ResultSet columns = metaData.getColumns(null, null, table_name, "%");
		            //获得所有字段名
		            while (columns.next()) {
		                //获得字段名
		                String column_name = columns.getString("COLUMN_NAME");	
		                fieldSet.add(column_name);
		            }
		        }
		        if (connSqlite != null && !connSqlite.isClosed()) {
					connSqlite.close(); 		//关闭sqlite连接
				}
		        File crcFile = new File(tmpSqlDir + File.separator + ".template.db3.crc");		        
				if (crcFile.exists()) {
					if(crcFile.delete()) {	//删除crc文件
						logger.info("***** delete successed:" + crcFile.toString());
					}
					else {
						logger.info("***** delete failed:" + crcFile.toString());
					}
				}
				File db3File = new File(db3PathFile);		        
				if (db3File.exists()) {
					if(db3File.delete()) {	//删除db3文件
						logger.info("***** delete successed:" + db3File.toString());
					}
					else {
						logger.info("***** delete failed:" + db3File.toString());
					}
				}				     
		        logger.info("***** end:" + table_name);	
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}

		}

		public void cleanup(Context context) throws IOException, InterruptedException {
			
		}

		public void map(Text key, BytesWritable value, Context context) throws IOException, InterruptedException {
			sqlPart1 = "INSERT INTO " + table_name +"(";
            sqlPart2 = " VALUES(";			
			XXXXObject xObj = new XXXXObject();
			byte[] bs = new byte[value.getLength()];
			System.arraycopy(value.getBytes(), 0, bs, 0, value.getLength());
			VipcloudUtil.DeserializeObject(bs, xObj);
			for (Map.Entry<String, String> item : xObj.data.entrySet()) {
				String ikey = item.getKey();
				String val = item.getValue();
				if (fieldSet.contains(ikey)){
					sqlPart1 += "[" + ikey + "],";
	            	sqlPart2 += "'" + val.replace('\0', ' ').replace("'", "''").trim() + "',";
				}
			}
			sqlPart1 = sqlPart1.replaceAll(",$", "") + ")";
            sqlPart2 = sqlPart2.replaceAll(",$", "") + ");";
            if(sqlPart2.contains("2018")) {context.getCounter("map", "count").increment(1);

			context.write(new Text(sqlPart1+sqlPart2), NullWritable.get());}
			

		}
	}

}