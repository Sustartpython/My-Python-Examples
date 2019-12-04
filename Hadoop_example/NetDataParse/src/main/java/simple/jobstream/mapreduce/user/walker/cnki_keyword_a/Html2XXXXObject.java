package simple.jobstream.mapreduce.user.walker.cnki_keyword_a;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Map;

import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
//import org.apache.tools.ant.taskdefs.Replace;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将html格式转化为XXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
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
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);
	    
	    
		SequenceFileOutputFormat.setCompressOutput(job, false);		
		job.setNumReduceTasks(reduceNum);
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
	
		public void setup(Context context) throws IOException,
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
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	context.getCounter("map", "input").increment(1);
	    	
	    	
	    	String line = value.toString().trim();
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			String id  = mapJson.get("id");
			String keyword = mapJson.get("keyword");
			String html = mapJson.get("html");
	    	
	    	Document doc = Jsoup.parse(html);
	    	String alt_word = "";	
	    	String summary = "";
	    	
	    	for (Element pElement : doc.select("div.aboutIntro > p")) {
				if ((pElement.select("span.from").first() != null) || (pElement.select("a.more").first() != null)) {	//简介
					summary = pElement.text().trim();
					
					if (summary.endsWith("在知网百科中查看更多释义")) {
						summary = summary.substring(0, summary.length() - "在知网百科中查看更多释义".length()).trim();
					}
				}
				else {	//单词
					alt_word = pElement.text().trim();
				}
			}
	    	
	    	XXXXObject xObj = new XXXXObject();
	    	xObj.data.put("id", id);
	    	xObj.data.put("keyword", keyword);
	    	xObj.data.put("alt_word", alt_word);	 
	    	xObj.data.put("summary", summary);	 
			
			context.getCounter("map", "out").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(id), new BytesWritable(bytes));			
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
