package simple.jobstream.mapreduce.common.vip;

import java.io.File;
import java.io.FileNotFoundException;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.List;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.log4j.Logger;

public class SqliteReducer extends Reducer<Text, NullWritable, Text, NullWritable> {
	public static Logger logger = Logger.getLogger(SqliteReducer.class);
	public static String postfixDb3 = "";
	public static String tempFileDb3 = "";
	public static String outputHdfsPath = "";
	
	private FileSystem hdfs = null;
	private String tempDir = null;

	private Connection connSqlite = null;
	private Statement stateSqlite = null;
	
	private List<String> sqlList = new ArrayList<String>();
	
	private Counter sqlCounter = null;

	protected void setup(Context context)
			throws IOException, InterruptedException
	{
		try {
			Class.forName("org.sqlite.JDBC");

			//创建存放db3文件的本地临时目录
			postfixDb3 = context.getConfiguration().get("postfixDb3");
			tempFileDb3 = context.getConfiguration().get("tempFileDb3");
			String taskId = context.getConfiguration().get("mapred.task.id");
			String JobDir = context.getConfiguration().get("job.local.dir");
			tempDir = JobDir + File.separator + taskId;
			File baseDir = new File(tempDir);
			if (!baseDir.exists())
			{
				baseDir.mkdirs();
			}
			logger.info("***** baseDir:" + baseDir);	
			//
			hdfs = FileSystem.get(context.getConfiguration());
			sqlCounter = context.getCounter("reduce", "sqlCounter");
			
			String db3PathFile = baseDir.getAbsolutePath() + File.separator +  taskId + "_" + postfixDb3 + ".db3";
			Path src = new Path(tempFileDb3);	//模板文件（HDFS路径）
			Path dst = new Path(db3PathFile);	//local路径
			hdfs.copyToLocalFile(src, dst);
			File crcFile = new File(baseDir.getAbsolutePath() + File.separator + "." +  taskId + "_" + postfixDb3 + ".db3.crc");
			if (crcFile.exists()) {
				if(crcFile.delete()) {	//删除crc文件
					
				}
				else {
					logger.info("***** delete failed:" + crcFile.toString());
				}
			}				
			connSqlite = DriverManager.getConnection("jdbc:sqlite:"+db3PathFile);
			stateSqlite = connSqlite.createStatement();
		} catch (Exception e) {
			logger.error("****************** setup failed. ******************", e);
		}
								
		logger.info("****************** setup finished  ******************");
	}
	
	public void insertSql(Context context)
	{
		String sql = "";
		if (sqlList.size() > 0) {
			try {						
				stateSqlite.execute("BEGIN TRANSACTION;");
				for (int i = 0; i < sqlList.size(); ++i)
				{
					sql = sqlList.get(i);
					stateSqlite.execute(sql);
					sqlCounter.increment(1);
				}
				stateSqlite.execute("COMMIT TRANSACTION;");
				
				sqlList.clear();
							
			} catch (Exception e) {
				context.getCounter("reduce", "insert error").increment(1);
				logger.error("***Error: insert failed. sql:" + sql, e);
			}
		}
		
	}
	
	public void reduce(Text key, Iterable<NullWritable> values,
			Context context) throws IOException, InterruptedException {
		
		sqlList.add(key.toString());
		
		if (sqlList.size() > 1000) {
			insertSql(context);
		}				
		
		context.getCounter("reduce", "count").increment(1);
		context.write(key, NullWritable.get());
	}
	

	protected void cleanup(Context context) throws IOException,
				InterruptedException
	{			
		logger.info("****************** Enter cleanup ******************");
		insertSql(context); 		//处理余数
		
		try {
			if (connSqlite != null && !connSqlite.isClosed()) {
				connSqlite.close(); 		//关闭sqlite连接
			}
		} catch (SQLException ex) {
			// TODO Auto-generated catch block
			ex.printStackTrace();
			throw new InterruptedException(ex.getMessage());
		}
		
		
		try
		{
			File localDir = new File(tempDir);
			if (!localDir.exists())
			{
				throw new FileNotFoundException(tempDir + " is not found.");
			}

			//再次获取，这里并不能感知到pre获取的参数
			outputHdfsPath = context.getConfiguration().get("outputHdfsPath");
			//最终存放db3的hdf目录。嵌在了MR的输出目录，便于自动清空。
			Path finalHdfsPath = new Path(outputHdfsPath + File.separator + "/db3/");	 	
			logger.info("***** finalHdfsPath:" + finalHdfsPath);	
			
			
			File[] files = localDir.listFiles();
			for (File file : files)
			{
				if (file.getName().endsWith(".db3"))
				{
					Path srcPath = new Path(file.getAbsolutePath());
					Path dstPash = new Path(finalHdfsPath.toString() + "/" + file.getName());						
					hdfs.moveFromLocalFile(srcPath, dstPash); 	//移动文件
					//hdfs.copyFromLocalFile(true, true, srcPath, dstPash);	//删除local文件，并覆盖hdfs文件
					logger.info("copy " + srcPath.toString() + " to "
							+ dstPash.toString());
				}
			}
		}
		catch (Exception e)
		{
			logger.error("****************** upload file failed. ******************", e);
		}
	}
	
}
