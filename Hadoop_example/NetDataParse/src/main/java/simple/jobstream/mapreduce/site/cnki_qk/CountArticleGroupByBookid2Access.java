package simple.jobstream.mapreduce.site.cnki_qk;

import java.io.File;
import java.io.IOException;
import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.SQLException;
import java.sql.Statement;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Date;
import java.util.List;
import java.util.Map;

import net.ucanaccess.jdbc.UcanaccessDriver;

import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Counter;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.SequenceFileInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;

import com.healthmarketscience.jackcess.Database.FileFormat;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.JobConfUtil;
import com.process.frame.util.VipcloudUtil;

public class CountArticleGroupByBookid2Access extends InHdfsOutHdfsJobInfo
{
	public static String inputTablePath = "";
	public static String outputTablePath = "";
	public static int redNum = 1;

	public void pre(Job job) 
	{
		String jobName = "cnki_qk." + this.getClass().getSimpleName();
		job.setJobName(jobName);

		inputTablePath  = job.getConfiguration().get("export2access.inputTablePath");
		outputTablePath = job.getConfiguration().get("export2access.outputTablePath");
	}

	public void SetMRInfo(Job job) 
	{
		//JobConfUtil.setTaskPerMapMemory(job, 5000);
//		JobConfUtil.setTaskPerReduceMemory(job, 10000);
		//JobConfUtil.setTaskShareMapJVM(job, 200);
		
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
		JobConfUtil.setTaskPerReduceMemory(job, 5120);
		
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setMapperClass(CountArticleGroupByBookid2Access.ProcessMapper.class);
		job.setReducerClass(CountArticleGroupByBookid2Access.ProcessReducer.class);
		
		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(Text.class);
		
		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(NullWritable.class);
		
		job.setInputFormatClass(SequenceFileInputFormat.class);
		job.setOutputFormatClass(TextOutputFormat.class);
		
		SequenceFileOutputFormat.setCompressOutput(job, false);
		redNum = Integer.valueOf(job.getConfiguration().get("export2access.rednum"));
		job.setNumReduceTasks(redNum);
	}

	public void post(Job job) 
	{
	}

	@Override
	public String getHdfsInput() {
		return inputTablePath;
	}

	@Override
	public String getHdfsOutput() {
		return outputTablePath;
	}

	public static class ProcessMapper extends Mapper<Text, BytesWritable, Text, Text> 
	{
		protected void setup(Context context) throws IOException, InterruptedException
		{
			
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
				throws IOException, InterruptedException 
		{
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
			
			
			
			
			String outKey = bookid;
			String outValue = pykm + "\t" + years + "\t" + num;
			
			context.getCounter("map", "count").increment(1);
			context.write(new Text(outKey), new Text(outValue));
		}
	}

	public static class ProcessReducer extends Reducer<Text, Text, Text, NullWritable> 
	{
		int nCount = 0;
		Connection conn = null;
		Statement stmt = null;
		PreparedStatement pstmt = null;
		String tempDir = "";
		String dbName = "ArticleCount.accdb";
		String taskDbName = "";
		private List<String> sqls = new ArrayList<String>();
		private Counter totalCount = null;
		private Counter successCount = null;
		
		protected void setup(Context context) throws IOException, InterruptedException 
		{
			
			String sSqlCreate = context.getConfiguration().get("export2access.sSqlCreate");
			
			String JobDir = context.getConfiguration().get("job.local.dir");
			String taskId = context.getConfiguration().get("mapred.task.id");
			
			taskDbName = taskId + "_" + dbName;
			tempDir = JobDir + File.separator + taskId;
			File baseDir = new File(tempDir);
			if (!baseDir.exists()) 
			{
				baseDir.mkdirs();
			}
			
			/*
			String sSqlCreate = "CREATE TABLE modify_title_info (";
			for (int i = 0; i < fields.size(); i++)
			{
				String field = fields.get(i);
				String datatype = dtypes.get(i);
				
				sSqlCreate += String.format("%s %s,", field, datatype);
			}
			sSqlCreate = sSqlCreate.substring(0, sSqlCreate.length() - 1);
			sSqlCreate += ")";
			*/
			
			//System.out.println(sSqlCreate);
			
			try 
			{
				Class.forName("net.ucanaccess.jdbc.UcanaccessDriver");
//				String url = UcanaccessDriver.URL_PREFIX + tempDir + File.separator + taskDbName
//						+ ";newdatabaseversion=" + FileFormat.V2003.name();
				String url = UcanaccessDriver.URL_PREFIX + tempDir + File.separator + taskDbName
						+ ";newdatabaseversion=" + FileFormat.V2010.name();
				conn = DriverManager.getConnection(url, "", "");
				conn.setAutoCommit(false);
				stmt = conn.createStatement();
				stmt.execute(sSqlCreate);
				
			} catch (Exception ex) {
				ex.printStackTrace();
			}

			totalCount = context.getCounter("ReducerCount", "TOTAL_COUNT");
			successCount = context.getCounter("ReducerCount", "SUCCESS_COUNT");
		}

		public void reduce(Text key, Iterable<Text> values,
				Context context) throws IOException, InterruptedException 
		{
			String bookid = key.toString();
			String pykm = "";
			String years = "";
			String num = "";
			
			String outLine = key.toString(); 
			int cnt = 0;
			for (Text val : values) {
				cnt += 1;
				if (cnt == 1) {
					String line = val.toString();
					String[] vec = line.split("\t");
					
					outLine += "\t" + line;
					pykm = vec[0].trim();
					years = vec[1].trim();
					num = vec[2].trim();
				}
			}
			outLine += "\t" + cnt;
			
			String sql = "insert into ArticleCount(pykm, years, num, bookid, cnt)";
			sql += "values('%s', '%s', '%s', '%s', %d)";
			sql = String.format(sql, pykm, years, num, bookid, cnt);
			totalCount.increment(1);
			sqls.add(sql);
			if (sqls.size() > 5000) {
				insertSql();
			}
			
			context.getCounter("reduce", "count").increment(1);
			context.write(new Text(outLine), NullWritable.get());
		}

		@Override
		protected void cleanup(Context context) throws IOException,InterruptedException 
		{
			insertSql();
			FileSystem hdfs;
			hdfs = FileSystem.get(context.getConfiguration());
			Path srcPath = new Path(tempDir + File.separator + taskDbName);
			String rootDir = context.getConfiguration().get("vipcloud.hdfs.proc.root.dir");
			Path dbPath;
			outputTablePath = context.getConfiguration().get("export2access.outputTablePath");
			if (rootDir == null) 
			{
				dbPath = new Path(outputTablePath + File.separator + taskDbName);
			} else {
				dbPath = new Path(rootDir + outputTablePath + File.separator + taskDbName);
			}
			if (stmt != null) 
			{
				try 
				{
					stmt.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			if (conn != null) 
			{
				try 
				{
					conn.close();
				} catch (SQLException e) {
					e.printStackTrace();
				}
			}
			System.out.println("srcPath:" + srcPath);
			System.out.println("dbPath:" + dbPath);
			hdfs.copyFromLocalFile(srcPath, dbPath);
		}
		
		/*
		public void insertSql() 
		{
			try 
			{
				if (stmt != null) 
				{
					for (int i = 0; i < sqls.size(); i++) 
					{
						stmt.execute(sqls.get(i));
						successCount.increment(1);
					}
					conn.commit();
					sqls.clear();
				}
			} catch (Exception e) {
				e.printStackTrace();
				//System.out.println(_keyid);
			}
		}
		*/
		public void insertSql() 
		{
			if (stmt != null) 
			{
				for (int i = 0; i < sqls.size(); i++) 
				{
					String sqlinsert = sqls.get(i);
					try 
					{
						stmt.execute(sqlinsert);
					} catch (Exception e) {
						e.printStackTrace();
						System.out.println(sqlinsert);
					}
					successCount.increment(1);
				}
				
				try 
				{
					conn.commit();
				} catch (SQLException e) {
					e.printStackTrace();
				}
				sqls.clear();
			}
		}
	}

	
}
