package simple.jobstream.mapreduce.site.cnki_bz;

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
	    	Document doc = Jsoup.parse(html);
	    	
	    	String pykm = "";
	    	String title_c = "";
			String title_e = "";
			String remark_c = "";
			String remark_e = "";
			String doi = "";
			String author_c = "";
			String author_e = "";
			String organ = "";
			String name_c = "";
			String name_e = "";
			String years = "";
			String vol = "";
			String num = "";
			String keyword_c = "";
			String keyword_e = "";
			String imburse = "";
			//------------------------------bz----------------------------------------------
			String drafter = "";
			String bzcommittee = "";
			String bzmaintype = "行业标准";
			//标准编号
			String identifier_standard = "";
			//版次
			String title_edition = "";
			//提出(发布)单位
			String creator_institution = "";
			//起草单位
			String creator_drafting = "";
			//发布单位
			String creator_release = "";
			//摘要
			String description = "";
			//行业标准
			String description_type = "";
			//主题词
			String subject = "";
			//本开页数
			String page = "";
			//中图分类号
			String subject_clc = "";
			//中国标准分类号
			String subject_csc = "";
			//国际标准分类号
			String subject_isc = "";
			//实施日期
			String date_impl = "";
			//发布日期
			String date_created = "";
			//国别
			String country = "";
			//语言
			String language = "";
			//bznum2
			String bznum2 = "";
			//状态
			String bzstatus = "";
			//替代标准
			String bzsubsbz = "";
			//完整url 
			String netfulltextaddr = "";
		
			 Element tableElement = doc.select("table#box").first();
			if (tableElement == null) {
				tableElement = doc.select("table#Table1").first();
				if (tableElement == null) {
					context.getCounter("map", "tableElement == null").increment(1);
					return;
				}
			} 
			
			
			/************************** begin table ************************/
			{
				for (Element rowElement : tableElement.select("> tbody > tr")) {
					//replace(' ', ' ')替换特殊空格
					text = rowElement.text().replace(' ', ' ').trim();
					if (text.startsWith("【英文标准名称】")) {
						title_e = rowElement.select("> td.checkItem").text();
						title_e = cleanSpace(title_e);
					}
					else if (text.startsWith("【中文标准名称】") || text.startsWith("【原文标准名称】")) {
						title_c = rowElement.select("> td.checkItem").text();
						title_c = cleanSpace(title_c);
					}
					else if (text.startsWith("【标准号】")) {
						identifier_standard = rowElement.select("> td.checkItem").text();
						identifier_standard = cleanSpace(identifier_standard);
						if (identifier_standard.toUpperCase().startsWith("GB")) {
							bzmaintype = "国家标准";
						}
					}
					else if (text.startsWith("【标准状态】")) {
						bzstatus = rowElement.select("> td.checkItem").text();	
						bzstatus = cleanSpace(bzstatus);
					}
					else if (text.startsWith("【发布日期】")) {
						date_created = rowElement.select("> td.checkItem").text();
						date_created = cleanSpace(date_created);
					}
					
					else if (text.startsWith("【实施日期】") || text.startsWith("【实施或试行日期】")) {
						date_impl = rowElement.select("> td.checkItem").text();
						date_impl = cleanSpace(date_impl);
					}
					else if (text.startsWith("【发布部门】")) {
						creator_release = rowElement.select("> td.checkItem").text();
						creator_release = cleanSpace(netfulltextaddr);
					}
					else if (text.startsWith("【起草单位】")) {
						creator_drafting = rowElement.select("> td.checkItem").text();
						creator_drafting = cleanSpace(creator_drafting);
					}
					else if (text.startsWith("【起草人】")) {
						drafter = rowElement.select("> td.checkItem").text();	
						drafter = cleanSpace(drafter);
					}
					else if (text.startsWith("【标准技术委员会】")) {
						bzcommittee = rowElement.select("> td.checkItem").text();
						bzcommittee = cleanSpace(bzcommittee);
					}
					else if (text.startsWith("【中国标准分类号】")) {
						subject_csc = rowElement.select("> td.checkItem").text();
						subject_csc = cleanSpace(subject_csc);
					}
					else if (text.startsWith("【国际标准分类号】")) {
						subject_isc = rowElement.select("> td.checkItem").text();
						subject_isc = cleanSpace(subject_isc);
					}
					else if (text.startsWith("【中图分类号】")) {
						subject_clc = rowElement.select("> td.checkItem").text();
						subject_clc = cleanSpace(subject_isc);
					}
					else if (text.startsWith("【摘要】")) {
						remark_c = rowElement.select("> td.checkItem").text();
						remark_c = cleanSpace(remark_c);
					}
					else if (text.startsWith("【总页数】") || text.startsWith("【页数】")) {
						page = rowElement.select("> td.checkItem").text();
						page = cleanSpace(page);
					}
					else if (text.startsWith("【中文主题词】")) {
						keyword_c = rowElement.select("> td.checkItem").text();
						keyword_c = cleanSpace(keyword_c);
					}
					else if (text.startsWith("【英文主题词】")) {
						keyword_e = rowElement.select("> td.checkItem").text();
						keyword_e = cleanSpace(keyword_e);
					}
					else if (text.startsWith("【国别】")) {
						country = rowElement.select("> td.checkItem").text();
						country = cleanSpace(country);
					}
					else if (text.startsWith("【正文语种】") || text.startsWith("【语种】")) {
						language = rowElement.select("> td.checkItem").text();
						language = cleanSpace(language);
					}
				
				}
				
				if (title_c.isEmpty()) {
					title_c = doc.title().split("--")[0];
				}
			}
			/************************** ene table ************************/
			

			
			//获取bznum2
			bznum2 = identifier_standard.replaceAll("[^a-z^A-Z^0-9]", "");
			
		
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("title_c", title_c);
			xObj.data.put("title_e", title_e);
			xObj.data.put("bzmaintype", bzmaintype);
			xObj.data.put("media_c", identifier_standard);
			xObj.data.put("bznum2", bznum2);
			xObj.data.put("bzpubdate", date_created);
			xObj.data.put("bzimpdate", date_impl);
			xObj.data.put("bzstatus", bzstatus);
			xObj.data.put("bzcountry", country);
			xObj.data.put("bzissued", creator_release);
			xObj.data.put("showorgan", creator_drafting);
			//xObj.data.put("showwriter", author_c);
			//xObj.data.put("bzcommittee", "");
			xObj.data.put("sClass", subject_csc);
			xObj.data.put("keyword_c", keyword_c);
			xObj.data.put("keyword_e", keyword_c);
			xObj.data.put("bzintclassnum", subject_isc);
			xObj.data.put("remark_c", remark_c);
			xObj.data.put("bzpagenum", page);
			xObj.data.put("bzsubsbz", bzsubsbz);
			xObj.data.put("netfulltextaddr", netfulltextaddr);
			xObj.data.put("bzcnclassnum", subject_clc);
			xObj.data.put("language", language);
			xObj.data.put("rawid", rawid);
			
			
			if (rawid.trim().length() < 1) {
				context.getCounter("map", "rawid").increment(1);
				return;
			}
			
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
