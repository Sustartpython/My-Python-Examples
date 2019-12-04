package simple.jobstream.mapreduce.site.cnki_cg;

import java.io.IOException;
import java.math.MathContext;
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
import org.apache.hadoop.mapreduce.Mapper.Context;
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
public class CnkiHtml2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
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
	   			
			XXXXObject xObj = ParseHtml(html, context);	
			if (null == xObj) {
				return;
			}	
			xObj.data.put("rawid", rawid);
			
			
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "null rawid").increment(1);
				return;
			}
			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));			
		}
	    
	    
	    public XXXXObject ParseHtml(String html, Context context) {
			Document doc = Jsoup.parse(html);

	    	String title = "";
			String description = "";
			String subject = "";
			//------------------------------cg----------------------------------------------
			String creator = "";
			String creator_institution = "";
			String subject_clc = "";
			String subject_esc = "";
			String description_type = "";
			String cglevel = "";
			String date = "";
			String cgevaluate = "";
			String cgdate = "";
			
			
		
			Element tableElement = doc.select("table#box").first();
			if (tableElement == null) {
				context.getCounter("map", "tableElement == null").increment(1);
				return null;
			} 
			
			
			/************************** begin table ************************/
			{
				for (Element rowElement : tableElement.select("> tbody > tr")) {
					//replace(' ', ' ')替换特殊空格
					String text = rowElement.text().replace(' ', ' ').trim();
					if (text.startsWith("【成果名称】")) {
						title = rowElement.select("> td.checkItem").text();
						title = cleanSpace(title);
					}else if (text.startsWith("【成果完成人】")) {
						creator = rowElement.select("> td.checkItem").text();
						creator = cleanSpace(creator);
					}else if (text.startsWith("【第一完成单位】")) {
						creator_institution = rowElement.select("> td.checkItem").text();	
						creator_institution = cleanSpace(creator_institution);
					}else if (text.startsWith("【关键词】")) {
						subject = rowElement.select("> td.checkItem").text();
						subject = cleanSpace(subject);
					}else if (text.startsWith("【中图分类号】")) {
						subject_clc = rowElement.select("> td.checkItem").text();
						subject_clc = cleanSpace(subject_clc);
					}else if (text.startsWith("【学科分类号】")) {
						subject_esc = rowElement.select("> td.checkItem").text();
						subject_esc = cleanSpace(subject_esc);
					}else if (text.startsWith("【成果类别】")) {
						description_type = rowElement.select("> td.checkItem").text();
						description_type = cleanSpace(description_type);
					}else if (text.startsWith("【成果水平】")) {
						cglevel = rowElement.select("> td.checkItem").text();	
						cglevel = cleanSpace(cglevel);
					}else if (text.startsWith("【研究起止时间】")) {
						cgdate = rowElement.select("> td.checkItem").text();
						cgdate = cleanSpace(cgdate);
					}else if (text.startsWith("【评价形式】")) {
						cgevaluate = rowElement.select("> td.checkItem").text();
						cgevaluate = cleanSpace(cgevaluate);
					}else if (text.startsWith("【成果入库时间】")) {
						date = rowElement.select("> td.checkItem").text();
						date = cleanSpace(date);
					}
	
				}
				
				if (title.isEmpty()) {
					title = doc.title().split("--")[0];
				}
			}
			/************************** end table ************************/
			
			/************************** description *************************/
			final String regex = "cgjj_source = '([\\s\\S]+)';";
			final Pattern pattern = Pattern.compile(regex);
			final Matcher matcher = pattern.matcher(doc.html().trim());
			if (matcher.find()) {
				description = matcher.group(1).trim();
			}
			
			/************************** description *************************/
					
			XXXXObject xObj = new XXXXObject();	
			xObj.data.put("title", title);
			xObj.data.put("description", description);
			xObj.data.put("subject", subject);
			xObj.data.put("creator", creator);
			xObj.data.put("creator_institution", creator_institution);
			xObj.data.put("subject_clc", subject_clc);
			xObj.data.put("subject_esc", subject_esc);
			xObj.data.put("description_type", description_type);
			xObj.data.put("cglevel", cglevel);
			xObj.data.put("date", date);
			xObj.data.put("cgevaluate", cgevaluate);
			xObj.data.put("cgdate", cgdate);
			
			return xObj;
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
