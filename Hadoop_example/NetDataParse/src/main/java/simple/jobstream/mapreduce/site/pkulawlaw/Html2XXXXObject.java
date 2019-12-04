package simple.jobstream.mapreduce.site.pkulawlaw;

import java.io.IOException;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "pkulaw." + this.getClass().getSimpleName();
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
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();
	    	
	    	int idx = text.indexOf('★');
	    	if (idx < 1) {
	    		context.getCounter("map", "idx < 1").increment(1);
				return;
			}
	    	
	    	String rawid = text.substring(0, idx);
	    	String html = text.substring(idx+1).trim();
	    	Document doc = Jsoup.parse(html);
	    
	    	String title = "";
			String creator_release = "";
			String identifier_standard = "";
			String description_type = "";
			String subject_dsa = "";
			String date_impl = "19000000";
			String date_created = "19000000";
			String legal_status = "";
			String description = "";
//			**************************source部分***************************
			title = doc.title().trim();
			title = cleanSpace(title);
			
			Element table = doc.select("table#tbl_content_main").first();
			if (table == null) {
//				context.getCounter("map", "null table id" + rawid).increment(1);
				context.getCounter("map", "null table id").increment(1);
				return;
			}
					
			for (Element element : table.select("td")) {
				if (element.text().trim().startsWith("【发布部门】")) {
					creator_release = element.text().trim().replace("【发布部门】", "");
					creator_release = cleanSpace(creator_release).replace("，", ";");
				}
				
				if (element.text().trim().startsWith("【发文字号】")) {
					identifier_standard = element.text().trim().replace("【发文字号】", "");
					identifier_standard = cleanSpace(identifier_standard);
				}
				
				if (element.text().trim().startsWith("【发布日期】") || element.text().trim().startsWith("【颁布日期】")) {
					date_created = element.text().trim().replace("【发布日期】", "").replace("【颁布日期】", "");
					date_created = cleanSpace(date_created);
				}
				
				if (element.text().trim().startsWith("【实施日期】") || element.text().trim().startsWith("【生效日期】")) {
					date_impl = element.text().trim().replace("【实施日期】", "").replace("【生效日期】", "");
					date_impl = cleanSpace(date_impl);
				}
				
				if (element.text().trim().startsWith("【效力级别】")) {
					description_type = element.text().trim().replace("【效力级别】", "");
					description_type = cleanSpace(description_type).replace("，", ";");
				}
				
				if (element.text().trim().startsWith("【行业类别】")) {
					subject_dsa = element.text().trim().replace("【行业类别】", "");
					subject_dsa = cleanSpace(subject_dsa).replace("，", ";");
				}
				
				if (element.text().trim().startsWith("【法规类别】") || element.text().trim().startsWith("【分类】")) {
					subject_dsa = element.text().trim().replace("【法规类别】", "").replace("【分类】", "");
					subject_dsa = cleanSpace(subject_dsa).replace("，", ";");;
				}
				
				if (element.text().trim().startsWith("【时效性】")) {
					legal_status = element.text().trim().replace("【时效性】", "");
					legal_status = cleanSpace(legal_status);
				}
				
				//********************************fulltext start****************************
				if (element.id().equals("fulltext")) {
					if (element.select("table").first() != null) {
						element.select("table").first().remove();
					}
					
					if (element.select("div#login_prompt").first() != null) {
						element.select("div#login_prompt").first().remove();
					}
					
					description = element.text().replaceAll("^\\p{Zs}+|\\p{Zs}+$", "");
				}	
				//********************************fulltext end  ****************************
				
				
			}
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("title", title);
			xObj.data.put("creator_release", creator_release);
			xObj.data.put("subject_dsa", subject_dsa);
			xObj.data.put("description_type", description_type);
			xObj.data.put("date_impl", date_impl);
			xObj.data.put("date_created", date_created);
			xObj.data.put("legal_status", legal_status);
			xObj.data.put("description", description);
			xObj.data.put("identifier_standard", identifier_standard);
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));			
			
//			**************************source部分***************************

		
			
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
