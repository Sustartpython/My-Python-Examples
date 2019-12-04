package simple.jobstream.mapreduce.site.wanfang_cg.wanfang_cg_back;

import java.awt.print.Printable;
import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;
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
		String jobName = "Html2XXXXObject";
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
		
		//清理class，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanClass(String text) {
			text = text.replace("{", "").replace("}", "").trim();
			
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
		
		public XXXXObject ParseDoc(String html, Context context) {
			
			Document doc = Jsoup.parse(html);
	    	
	    	String title_c = "";
	    	String title_e = "";
	    	//项目编号：
	    	String cgitemnumber = "";
	    	String years = "";
	    	String cglimituse = "";
	    	String cgprovinces = "";
	    	String _class = "";
	    	String cgcgtypes = "";
	    	String keyword_c = "";
			String remark_c = "";
			String cgappunit = "";
			String cgappdate = "";
			String cgregdate = "";
			String Cgtransrange = "";
			String cgrecomunit = "";
			String media_c = "";
			String cgprocesstimes = "";
			String Showorgan = "";
			String Showwriter = "";
			String cgindustry = "";
			String cginduscode = "";
			String cgcontactunit = "";
			String cgcontactaddr = "";
			String cgcontactfax = "";
			String cgzipcode = "";
			String cgcontactemail = "";

			
			
			Element baseinfoElement = doc.select("div.section-baseinfo").first();
			if (baseinfoElement == null) {
				context.getCounter("map", "baseinfoElement == null").increment(1);
				return null;
			}
			
			/************************** begin title ************************/
			{
				Element h1Element = baseinfoElement.getElementsByTag("h1").first();
				Element h2Element = baseinfoElement.getElementsByTag("h2").first();
				if (h1Element != null) {	//标题
					title_c = h1Element.text().trim();
				}
				if (h2Element != null) {
					title_e = h2Element.text().trim();
				}
				if ((title_c.length() < 1) && (title_e.length() < 1)) {
					context.getCounter("map", "no title").increment(1);
					return null;
				}
			}
			/************************** end title ************************/
			
			Element baseinfoFieldElement = doc.select("div.fixed-width.baseinfo-feild.cstad-baseinfo-feild").first();
			if (baseinfoFieldElement != null) {
				for (Element rowElement : baseinfoFieldElement.select("> table.md tr")) {
					String line = rowElement.text().trim();
					if (line.startsWith("项目年度编号：")) {
						cgitemnumber = rowElement.select("> td.text").text().trim();
						//成果公布年份 这个是之前网页的 现在改了  20181015发现并纠正
					} else if (line.startsWith("公布年份：")) {
						years = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("限制使用：")) {
						cglimituse = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("省市：")) {
						cgprovinces = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("中图分类号：")) {
						_class = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("成果类别：")) {
						cgcgtypes = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("关键词：")) {
						keyword_c = rowElement.select("> td.text").text().trim();
						keyword_c = keyword_c.replaceAll("\\p{Zs}+", " ").trim().replace(" ", ";");	//去掉分号前后的空白
					} else if (line.startsWith("成果简介：")) {
						remark_c = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("鉴定部门：")) {
						cgappunit = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("鉴定日期：")) {
						cgappdate = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("推荐部门：")) {
						cgrecomunit = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("登记号：")) {
						media_c = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("工作起止时间：")) {
						cgprocesstimes = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("完成单位：")) {
						Showorgan = rowElement.select("> td.text").text().trim();
						Showorgan = Showorgan.replaceAll("\\p{Zs}+", " ").trim().replace(" ", ";");	//去掉分号前后的空白
					} else if (line.startsWith("完成人：")) {
						Showwriter = rowElement.select("> td.text").text().trim();
						Showwriter = Showwriter.replaceAll("\\p{Zs}+", " ").trim().replace(" ", ";");	//去掉分号前后的空白
					} else if (line.startsWith("应用行业名称：")) {
						cgindustry = rowElement.select("> td.text").text().trim();
						//cgindustry = cgindustry.replaceAll("\\s*", " ").trim();	//去掉分号前后的空白
					} else if (line.startsWith("应用行业码：")) {
						cginduscode = rowElement.select("> td.text").text().trim();
						//cginduscode = cginduscode.replaceAll("\\s*", " ").trim();	//去掉分号前后的空白
					} else if (line.startsWith("联系单位名称：")) {
						cgcontactunit = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("邮政编码：")) {
						cgzipcode = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("传真：")) {
						cgcontactfax = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("电子邮件：")) {
						cgcontactemail = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("转让范围：")) {
						Cgtransrange = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("登记日期：")) {
						cgregdate = rowElement.select("> td.text").text().trim();
					} else if (line.startsWith("联系单位地址：")) {
						cgcontactaddr = rowElement.select("> td.text").text().trim();
					}					
				}
			}
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("title_c", title_c);
			xObj.data.put("title_e", title_e);
			xObj.data.put("cgitemnumber", cgitemnumber);
			xObj.data.put("years", years);
			xObj.data.put("cglimituse", cglimituse);
			xObj.data.put("cgprovinces", cgprovinces);
			xObj.data.put("_class", _class);
			xObj.data.put("cgcgtypes", cgcgtypes);
			xObj.data.put("keyword_c", keyword_c);
			xObj.data.put("remark_c", remark_c);
			xObj.data.put("cgappunit", cgappunit);
			xObj.data.put("cgappdate", cgappdate);
			xObj.data.put("cgrecomunit", cgrecomunit);
			xObj.data.put("media_c", media_c);
			xObj.data.put("cgprocesstimes", cgprocesstimes);
			xObj.data.put("Showorgan", Showorgan);
			xObj.data.put("Showwriter", Showwriter);
			xObj.data.put("cgindustry", cgindustry);
			xObj.data.put("cginduscode", cginduscode);
			xObj.data.put("cgcontactunit", cgcontactunit);
			xObj.data.put("cgzipcode", cgzipcode);
			xObj.data.put("cgcontactfax", cgcontactfax);
			xObj.data.put("cgcontactemail", cgcontactemail);
			xObj.data.put("Cgtransrange", Cgtransrange);
			xObj.data.put("cgregdate", cgregdate);
			xObj.data.put("cgcontactaddr", cgcontactaddr);
			return  xObj;
		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String,Object>>(){}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);			
			
			if (mapField.get("wanID") == null) {
				context.getCounter("map", "wanID is null").increment(1);
				return;
			}
			
			if (mapField.get("html") == null) {
				context.getCounter("map", "html is null").increment(1);
				return;
			}
			String wanID = mapField.get("wanID").toString();
			String html = mapField.get("html").toString();
	    	
	    		
	    	String rawid = wanID;
	    	XXXXObject xObj = ParseDoc(html, context);
	    	if (null == xObj) {
	    		context.getCounter("map", "xObj is null").increment(1);
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
