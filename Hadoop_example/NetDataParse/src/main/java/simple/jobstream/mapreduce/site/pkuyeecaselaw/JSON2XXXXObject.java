package simple.jobstream.mapreduce.site.pkuyeecaselaw;

import java.awt.print.Printable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.swing.text.AbstractDocument.Content;

import org.apache.directory.shared.kerberos.components.TypedData.TD;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
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
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class JSON2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "pkuyeecaselaw." + this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
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
		
		//清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';');			//全角转半角
			text = text.replaceAll("\\s*;\\s*", ";");	//去掉分号前后的空白
			text = text.replaceAll("\\s*\\[\\s*", "[");	//去掉[前后的空白	
			text = text.replaceAll("\\s*\\]\\s*", "]");	//去掉]前后的空白	
			text = text.replaceAll("[\\s;]+$", "");	//去掉最后多余的空白和分号
			
			return text;
		}
		
		//清理space，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();		
			return text;
		}
		//国家class
		static String getCountrybyString(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("中国", "CN");
			hashTable.put("英国", "UK");
			hashTable.put("日本", "JP");
			hashTable.put("美国", "US");
			hashTable.put("法国", "FR");
			hashTable.put("德国", "DE");
			hashTable.put("韩国", "KR");
			hashTable.put("国际", "UN");
			
			if (null != hashTable.get(text)) {
				text = hashTable.get(text);
			}
			else {
				text = "UN";
			}	
			
			return text;
		}
		//语言class
		static String getLanguagebyCountry(String text) {
			Dictionary<String,String> hashTable=new Hashtable<String,String>();
			hashTable.put("CN", "ZH");
			hashTable.put("UK", "EN");
			hashTable.put("US", "EN");
			hashTable.put("JP", "JA");
			hashTable.put("FR", "FR");
			hashTable.put("DE", "DE");
			hashTable.put("UN", "UN");
			hashTable.put("KR", "KR");
			text = hashTable.get(text);
			
			return text;
		}
		
		public static String formatDate(String date) {
			
			if (!date.contains("/")) {
				return date;
			}		
			String new_date = "";
			SimpleDateFormat formatter = new SimpleDateFormat ("yyyy/MM/dd");
			Date fotmatDate;
			try {
				fotmatDate = formatter.parse(date);
				formatter = new SimpleDateFormat ("yyyyMMdd"); 
				new_date = formatter.format(fotmatDate);
			} catch (ParseException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			return new_date;
		}
		
		
		public XXXXObject ParseHtml(String html, Context context) {
			Document doc = Jsoup.parse(html);
		    
	    	String title = "";
			String creator_release = "";
			String identifier_standard = "";
			String date_impl = "19000000";
			String date_created = "19000000";
			String subject_dsa = "";
			//String anhao = "";
			String legal_status = "";
			String description = "";
			String contributor = "";
			String agents = "";
			String agency = "";
			
			Elements tables = doc.select("table");
			Element table = null;
			for (Element element : tables) {
				if (element.select("tr").size() > 3) {
					table = element;
					break;
				}		
			}
			
			if (table == null) {
				context.getCounter("map", "table == null").increment(1);
				return null;
			}
				
			for (Element element : table.select("td")) {
				
				if (element.text().trim().startsWith("【案例名称】")) {			
					title = element.nextElementSibling().text().trim().replace("【案例名称】", "");
					title = cleanSpace(title);
				}else if (element.text().trim().startsWith("【审理法院】")) {
					creator_release = element.nextElementSibling().text().trim().replace("【审理法院】", "");
					creator_release = cleanSpace(creator_release);
				}else if (element.text().trim().startsWith("【案　　号】")) {
					identifier_standard = element.nextElementSibling().text().trim().replace("【案　　号】", "");
					identifier_standard = cleanSpace(identifier_standard);
				}else if (element.text().trim().startsWith("【案　　由】")) {
					subject_dsa = element.nextElementSibling().text();
					subject_dsa = cleanSpace(subject_dsa).replaceAll("[\\s\\p{Zs}]+", "").replace("->", ";");
				}else if (element.text().trim().startsWith("【审理法官】")) {
					contributor = element.nextElementSibling().text().trim().replace("【审理法官】", "");
					contributor = cleanSpace(contributor).replace(" ", ";");
				}else if (element.text().trim().startsWith("【代理律师】")) {
					if (element.nextElementSibling() != null) {
						agents = element.nextElementSibling().text().trim().replace("【代理律师】", "");
						agents = cleanSpace(agents).replace(" ", ";");
					}	
				}else if (element.text().trim().startsWith("【代理律所】")) {
					agency = element.nextElementSibling().text().trim().replace("【代理律所】", "");
					agency = cleanSpace(agency).replace(" ", ";");
				}else if (element.text().trim().startsWith("【颁布时间】")) {
					date_created = element.nextElementSibling().text().trim().replace("【颁布时间】", "");
					date_created = cleanSpace(date_created);
				}else if (element.text().trim().startsWith("【判决日期】")) {
					date_impl = element.nextElementSibling().text().trim().replace("【判决日期】", "");
					date_impl = cleanSpace(date_impl);
				}else if (element.text().trim().startsWith("【效力属性】")) {
					legal_status = element.nextElementSibling().text().trim().replace("【效力属性】", "");
					legal_status = cleanSpace(legal_status);
				}
				
			}
			
			//********************************fulltext start****************************
			if (doc.select("div#content").first() != null) {
				Element ele = doc.select("div#content").first();
				
				for (Element element : ele.children()) {		
					description += element.text().trim();
					description += '\n';
				}					
				description = description.trim();
			}	
			//********************************fulltext end  ****************************
			
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("title", title);
			xObj.data.put("creator_release", creator_release);
			xObj.data.put("date_impl", date_impl);
			xObj.data.put("date_created", date_created);
			xObj.data.put("legal_status", legal_status);
			xObj.data.put("description", description);
			xObj.data.put("subject_dsa", subject_dsa);
			xObj.data.put("contributor", contributor);
			xObj.data.put("agents", agents);
			xObj.data.put("agency", agency);
			xObj.data.put("identifier_standard", identifier_standard);
					
			return xObj;
			
		}
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("**text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String,Object>>(){}.getType();
			Map<String, Object> mapField = null;
			try {
				mapField = gson.fromJson(text, type);
			} catch (Exception e) { 
				// TODO: handle exception
				System.out.println("!!:" + text);
				return;
			}

				
			
			if (mapField.get("rid") == null) {
				context.getCounter("map", "rid is null").increment(1);
				return;
			}
			
			if (mapField.get("html") == null) {
				context.getCounter("map", "html is null").increment(1);
				return;
			}
			String rid = mapField.get("rid").toString();
			String html = mapField.get("html").toString();
	    	
	    		
	    	String rawid = rid;
	    	
			
			XXXXObject xObj = ParseHtml(html, context);
			if (null == xObj) {
				return;
			}
			xObj.data.put("rawid", rawid);
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));			

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
