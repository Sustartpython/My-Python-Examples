package simple.jobstream.mapreduce.site.sxxnkd;

import java.awt.List;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hdfs.server.namenode.block_005finfo_005fxml_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import com.cloudera.org.codehaus.jackson.sym.Name;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

//将Html格式转化为XXXXObject格式，包含去重合并
public class Html2XXXXObject_sxcq extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "sxcq";
		if (testRun) {
			jobName = "test_" + jobName;
		}
		job.setJobName(jobName);
		
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
	}
	
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    
		SequenceFileOutputFormat.setCompressOutput(job, false);
		if (testRun) {
			job.setNumReduceTasks(testReduceNum);
		} else {
			job.setNumReduceTasks(reduceNum);
		}
	}

	public void post(Job job)
	{
	
	}

	public String GetHdfsInputPath()
	{
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath()
	{
		return outputHdfsPath;
	}
	
	public static class ProcessMapper extends 
			Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		
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
		        String pathfile = "/vipuser/chenyong/log/log_map/" + nowDate + ".txt";
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
		
		public static HashMap<String,String> parseHtml(String htmlText){
			HashMap<String,String> map = new HashMap<String,String>();
			String rawid = "";
			String doi="";
			String title = "";
			String creator="";
			String source = "";
			String publisher = "";
			String volume = "";
			String issue = "";
			String description = "";
			String date_created = "";
			String subject = "";
			String page ="";
			String temp = "";
			String url = "";
			String htmltext = "";
			int num = 1; 
			
			JsonObject obj = new JsonParser().parse(htmlText).getAsJsonObject();
			String html = obj.get("html").getAsString();
			String cover = obj.get("cover").getAsString();
			rawid = obj.get("rawid").getAsString();
			Document doc = Jsoup.parse(html);
			Element div = doc.select("div.dgBook_fm_dl").first(); 
			if (div != null){
				
				Element	title_elElement = div.select("h1").first();
				title = title_elElement.text();
			}
			Element disc_elElement = doc.select("div.dgBook_txt").first(); 
			if(disc_elElement != null){
				description = disc_elElement.text();
			}
			Elements lis = div.select("li");
			if (lis != null){
				for(Element li:lis){
					if(li.text().startsWith("作者：")){
						String name = "作者：";
						creator = li.text().replace("'", "''").replace(" ", "");
						creator = creator.substring(name.length(),creator.length());	
					}
					else if(li.text().startsWith("出版社：")){
						String name = "出版社：";
						publisher = li.text();
						publisher = publisher.substring(name.length(),publisher.length());	
					}
					else if(li.text().startsWith("出版日期：")){
						String name = "出版日期：";
						date_created = li.text().replace("'", "''").replace("年", "").replace("月", "").replace("/", "").replace(".", "").replace("-", "");	
						date_created = date_created.substring(name.length(),date_created.length()).replace("日", "");
					}
				}
			}
			
			//
			if (rawid.trim().length() > 0) {			
				map.put("date_created", date_created);
				map.put("creator", creator);
				map.put("title", title);
				map.put("description", description);
				map.put("publisher", publisher);
				map.put("rawid", rawid);
				map.put("cover", cover);
				
			}
			
			return map;

		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString();
	    	HashMap<String, String> map = parseHtml(text);
			if(map.size() < 1){
				context.getCounter("map", "map.size() < 1").increment(1);	
				
	    		return;				
			}
			if (map.get("title").equalsIgnoreCase("")) {
				
				context.getCounter("map", "not title").increment(1);	
	    		return;	
			}
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("rawid", map.get("rawid"));
			xObj.data.put("title", map.get("title"));
			xObj.data.put("creator", map.get("creator"));
			xObj.data.put("description", map.get("description"));
			xObj.data.put("date_created", map.get("date_created"));
			xObj.data.put("publisher", map.get("publisher"));
			xObj.data.put("cover", map.get("cover"));
			
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(map.get("rawid")), new BytesWritable(bytes));		
		}
	}
	
	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//将buffer设为实际长度
		
			context.write(key, bOut);

		}
	}
}
