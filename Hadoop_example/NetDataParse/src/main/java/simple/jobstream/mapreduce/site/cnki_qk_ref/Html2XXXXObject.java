package simple.jobstream.mapreduce.site.cnki_qk_ref;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.print.attribute.standard.JobName;

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

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 40;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "cnki_qk_ref." + this.getClass().getSimpleName();
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
		
		static int cnt = 0;	
		
		private static String rawsourceid = "";
		private static String refertext = "";
		private static String refertext_tag = "";		
		private static String cnki_dbcode = "";		
		private static String cnki_filename = "";		
		private static Pattern patDbcode = Pattern.compile("dbcode=(.*?)(?:$|&)");
		private static Pattern patFilename = Pattern.compile("filename=(.*?)(?:$|&)");
		
		private static Map<String, String> mapDB = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException,
		InterruptedException {
			initMapDB();	//初始化map
		}
		
		private static void initMapDB() {
			mapDB.put("中国学术期刊网络出版总库","CJFQ");		//[J]
			mapDB.put("中国学术期刊（网络版）》","CJFQ");		//[J]
			mapDB.put("中国期刊全文数据库","CJFD");			//[J]
			mapDB.put("中国博士学位论文全文数据库","CDFD");		//[D]
			mapDB.put("中国优秀硕士学位论文全文数据库","CMFD");	//[D]
			mapDB.put("中国重要会议论文全文数据库","CPFD");		//[A][C]
			mapDB.put("国际会议论文全文数据库","IPFD");			//[A][C]
			mapDB.put("中国重要报纸全文数据库","CCND");			//[N]
			mapDB.put("中国专利数据库","SCPD");				//[P]
			mapDB.put("中国标准数据库","SCSD");				//[S]
			mapDB.put("中国图书全文数据库","CBBD");			//[M]
			mapDB.put("中国年鉴网络出版总库","CYFD");			//[Z]
			mapDB.put("国际期刊数据库","SSJD");				//[J]
			mapDB.put("外文题录数据库","CRLDENG");			//[]极少有type
		}
		
		//去除文本收尾的点、逗号、空白
		static String cleanDotCommaBlank(String text) {
			text = text.replaceAll("^[.,\\s]+", "");
			text = text.replaceAll("[.,\\s]+?$", "");
			
			return text;
		}
		
		//获取strtitle
		private static String getStrTitle(String dbcode, Element liElement) {
			String strtitle = "";
			String liText = liElement.text().trim();
			
			if (dbcode.equals("CBBD")) {	//中国图书全文数据库（图书没有title，只有书名）
				strtitle = "";
			}
			else {
				Element aElement = liElement.select("a").first();
				if (aElement != null) {
					strtitle = aElement.text().trim();
				}
				
				if (strtitle.length() < 1) {		//前面还没找到
					if (dbcode.equals("CRLDENG")) {		//外文题录数据库
						if (liText.indexOf("http://") > -1) {
							int idx = liText.indexOf("http://");
							strtitle = liText.substring(0, idx);	//去掉后面的http文字
							strtitle = strtitle.replaceAll("^\\[\\d+?\\]", "").trim();	//去掉前面的标号
						}
					}
					else if (dbcode.equals("CBBD")) {	//中国图书全文数据库（图书没有title，只有书名）
						strtitle = "";
					}
					else {
						String[] vec = liText.split(".");
						if (vec.length > 1) {
							strtitle = vec[1].replaceAll("\\[.*?\\]$", "").trim();	//正则是为了去掉最后面的中括号类型
						}
					}
				}
			}
			
			return strtitle;
		}
		
		//获取strtype
		private static String getStrType(String dbcode, Element liElement) {
			String strtype = "";
			String liText = liElement.text().trim();
			
			if (dbcode.equals("CRLDENG")) {		//外文题录数据库(CRLDENG)极少有type
				Element aElement = liElement.select("a").first();
				if (aElement != null) {
					String aText = aElement.text().trim();
					Pattern pattern = Pattern.compile("\\[(.{1,5})\\]$");
			        Matcher matcher = pattern.matcher(aText);
			        if (matcher.find()) {
			        	strtype = matcher.group(1);
			        }
				}
			}
			//中国博士学位论文全文数据库,中国优秀硕士学位论文全文数据库
			else if (dbcode.equals("CDFD") || dbcode.equals("CMFD")) {
				strtype = "D";
			}
			//中国学术期刊网络出版总库,中国学术期刊（网络版）》
			//中国期刊全文数据库
			//国际期刊数据库(SSJD)
			else if (dbcode.equals("CJFQ") || dbcode.equals("CJFD") || dbcode.equals("SSJD")) {
				strtype = "J";
			}
			//中国图书全文数据库
			else if (dbcode.equals("CBBD")) {
				strtype = "M";
			}
			//中国年鉴网络出版总库
			else if (dbcode.equals("CYFD")) {
				strtype = "Z";
			}
			//中国重要报纸全文数据库
			else if (dbcode.equals("CCND")) {
				strtype = "N";
			}
			//中国专利数据库
			else if (dbcode.equals("SCPD")) {
				strtype = "P";
			}
			//中国标准数据库
			else if (dbcode.equals("SCSD")) {
				strtype = "S";
			}
			//中国重要会议论文全文数据库,国际会议论文全文数据库
			else if (dbcode.equals("CPFD") || dbcode.equals("IPFD")) {
				strtype = "C";
				if (liText.indexOf("[C]") > 0) {
					strtype = "C";
				}
				else if (liText.indexOf("[A]") > 0) {
					strtype = "A";
				} 
			}			
			
			if (strtype.length() < 1) {		//还没找到
				Pattern pattern = Pattern.compile("\\[(.{1,5})\\]$");
		        Matcher matcher = pattern.matcher(liText);
		        int idx = 0;
		        if (matcher.find()) {
		        	strtype = matcher.group(1);
		        }
			}
			
			return strtype;
		}
		
		//获取strname(刊名)
		private static String getStrName(String dbcode, Element liElement) {
			String strname = "";
			String liText = liElement.text().trim();
			
			//中国学术期刊网络出版总库,中国学术期刊（网络版）》
			//中国期刊全文数据库
			if (dbcode.equals("CJFQ") || dbcode.equals("CJFD")) {
				int idx1 = liText.indexOf("[J].");
				int idx2 = liText.lastIndexOf('.');
				if ((idx1 > 0) && (idx1+4 < idx2)) {
					strname = liText.substring(idx1+4, idx2);
				}
			}
			//外文题录数据库(CRLDENG)
			//国际期刊数据库(SSJD)
			else if (dbcode.equals("CRLDENG") || dbcode.equals("SSJD")) {	//链接为title，链接后为(刊名.年)
				Element aElement = liElement.select("a").first();
				if (aElement != null) {
					Node node = aElement.nextSibling();
					while (node != null) {
						strname += Jsoup.parse(node.outerHtml()).text();
						node = node.nextSibling();
					}
				}
				else if (liText.indexOf("http://") > 0) {
					int idx = liText.indexOf("http://");
					strname = liText.substring(0, idx);
				}
				
				strname = strname.replaceAll("\\d{4} ?(\\(.{1,2}\\))?$", "");	//去掉最后的年期信息
				strname = strname.replaceAll("\\d{4}\\s*?$", "");	//去掉最后的年信息
				strname = strname.replaceAll("^\\[.+?\\]", "");	//去掉最前面的中括号内容
				strname = strname.replaceAll("^[\\.\\s]+?", "");	//去掉首部的点和空白	
				strname = strname.replaceAll("[\\.\\s]+?$", "");	//去掉尾部的点和空白	
			}
			//中国重要报纸全文数据库
			else if (dbcode.equals("CCND")) {
				int idx = liText.indexOf("[N].");
				if (idx > 0) {
					strname = liText.substring(idx);
					strname = strname.replaceAll("\\d{4} ?(\\(.{1,2}\\))?$", "");	//去掉最后的年期信息
					strname = strname.replaceAll("\\d{4}\\s*?$", "");	//去掉最后的年信息
					strname = strname.replaceAll("[\\.\\s]+?$", "");	//去掉尾部的点和空白	
				}
			}
			//中国图书全文数据库
			else if (dbcode.equals("CBBD")) {
				int idx2 = liText.indexOf("[M].");
				if (idx2 > 0) {
					int idx1 = liText.substring(0, idx2).lastIndexOf('.');
					if ((idx1 > 0) && (idx1 < idx2)) {
						strname = liText.substring(idx1+1, idx2);
					}
				}
				
			}
			//中国标准数据库
			else if (dbcode.equals("SCSD")) {
				strname = "";
			}
			//中国博士学位论文全文数据库,中国优秀硕士学位论文全文数据库
			else if (dbcode.equals("CDFD") || dbcode.equals("CMFD")) {
				strname = "";
			}
			//中国重要会议论文全文数据库,国际会议论文全文数据库
			else if (dbcode.equals("CPFD") || dbcode.equals("IPFD")) {
				int idxA = liText.indexOf("[A]");
				int idxC = liText.indexOf("[C]");
				if ((idxA > 0) && (idxA < idxC)) {
					strname = liText.substring(idxA+3, idxC).trim();
				}
				strname = strname.replaceAll("^[\\s,.]+", "");		//去掉首部的空格、逗号、点
			}		
			//中国年鉴网络出版总库
			else if (dbcode.equals("CYFD")) {
				strname = "";
			}
			//中国专利数据库
			else if (dbcode.equals("SCPD")) {
				int idx = liText.lastIndexOf("[P]");
				if (idx > 0) {
					strname = liText.substring(idx+3);
					strname = cleanDotCommaBlank(strname);
				}
			}
			else {
				String text = liText.replaceAll("\\s+", " ");	//去掉多余空白
				text = text.replaceAll("\\d{4} ?(\\(.{1,2}\\))?$", "");	//去掉最后的年期信息
				text = text.replaceAll("^[\\s,.]+?", "");		//去掉首部的空格、逗号、点
				text = text.replaceAll("[\\s,.]+?$", "");		//去掉尾部的空格、逗号、点
				String[] vec = text.split(".");
				if (vec.length > 0) {
					strname = vec[vec.length-1].trim();
				}
			}
			
			return strname;
		}
		
		//作者
		private static String getWriter(String dbcode, Element liElement) {
			String writer = "";
			String liText = liElement.text().trim();
			
			//外文题录数据库(CRLDENG)
			//国际期刊数据库(SSJD)
			if (dbcode.equals("CRLDENG") || dbcode.equals("SSJD")) {
				Element aElement = liElement.select("a").first();
				if (aElement == null) {
					writer = "";
				}
				else {					
					Node writerNode = aElement.previousSibling();
					if (writerNode != null) {
						writer = Jsoup.parse(writerNode.toString()).text().trim();			
					}
				}
			}
			//中国图书全文数据库
			else if (dbcode.equals("CBBD")) {
				int idx2 = liText.indexOf("[M].");
				if (idx2 > 0) {
					int idx1 = liText.substring(0, idx2).lastIndexOf('.');
					if (idx1 > 0) {
						writer = liText.substring(0, idx1).trim();
					}
				}
			}
			else {
				List<TextNode> lst = liElement.textNodes();
				if (lst.size() > 0) {
					writer = lst.get(0).text().trim();		
				}
			}
			
			writer = writer.replaceAll("^\\s*?\\[\\d+?\\]", "").trim();	//去掉前面的空白及标号
			writer = writer.replaceAll("[\\s ,.]+?$", "").trim();		//去掉尾部的空白、逗号、点。注意有个未知非空格字符。
			writer = writer.replace(',', ';');
			
			return writer;
		}
		
		//年卷期
		private static String getYearNum(String dbcode, Element liElement) {
			String stryearvolnum = "";
			String liText = liElement.text().trim();
			
			//中国博士学位论文全文数据库,中国优秀硕士学位论文全文数据库
			if (dbcode.equals("CDFD") || dbcode.equals("CMFD")) {
				Pattern pattern = Pattern.compile("\\d{4}$");
		        Matcher matcher = pattern.matcher(liText);
		        if (matcher.find()) {
		        	stryearvolnum = matcher.group(0);
		        }
			}
			else {
				String text = liText.replaceAll("\\s+", " ");	//去掉多余空白
				text = text.replace(',', '.');
				String[] vec = text.split("\\.");
				if (vec.length > 1) {
					stryearvolnum = vec[vec.length-1].trim();
				}
				if (!Pattern.compile("^[\\(\\)\\d]{4,11}").matcher(stryearvolnum).find()) {
					stryearvolnum = "";
				}
			}
			
			return stryearvolnum;
		}
		
		//出版社
		private static String getStrPubWriter(String dbcode, Element liElement) {
			String strpubwriter = "";
			String liText = liElement.text().trim();
			
			//中国博士学位论文全文数据库,中国优秀硕士学位论文全文数据库
			if (dbcode.equals("CDFD") || dbcode.equals("CMFD")) {
				int idx = liText.indexOf("[D].");
				if (idx > 0) {
					strpubwriter = liText.substring(idx+4);
					strpubwriter = strpubwriter.replaceAll("\\d{4}\\s*?$", "");	//去掉最后的年信息	
				}
			}
			//中国图书全文数据库
			else if (dbcode.equals("CBBD")) {
				int idx = liText.indexOf("[M].");
				if (idx > 0) {
					strpubwriter = liText.substring(idx+4);
					strpubwriter = strpubwriter.replaceAll("\\d{4}\\s*?$", "");	//去掉最后的年信息	
				}
			}
			//中国年鉴网络出版总库
			else if (dbcode.equals("CYFD")) {
				int idx1 = liText.indexOf("[Z].");
				int idx2 = liText.lastIndexOf('.');
				if (idx1+4 < idx2) {
					strpubwriter = liText.substring(idx1+4, idx2);
				}
			}
			
			strpubwriter = strpubwriter.replaceAll("^[\\s,.]+?", "");		//去掉首部的空白、逗号、点
			strpubwriter = strpubwriter.replaceAll("[\\s,.]+?$", "");		//去掉尾部的空白、逗号、点
			
			return strpubwriter;
		}
		
		//处理一个li，即一条引文
		public static boolean procOneLi(Element liElement) 
				throws IOException, InterruptedException  {
			refertext = "";
			refertext_tag = "";	
			cnki_dbcode = "";		
			cnki_filename = "";	
			
			Element emElement = liElement.select("em").first();
			if (emElement != null) {
				emElement.remove();
			}
			
			refertext = liElement.text().trim();
			refertext = refertext.replaceAll("^\\[\\d+?\\]", "").trim();	//去掉最前面的中括号编号
			
			if (refertext.equals(".")) {
				return false;
			}
			if (refertext.length() < 1) {
				return false;
			}
			
			for (Node node : liElement.childNodes()) {
				//System.out.println("***" + node.nodeName());
				if (node.nodeName().equals("#text")) {
					//refertext_tag += node.toString().replaceAll("&nbsp;", "").trim();
					refertext_tag += node.toString().trim();
				}
				else if (node.nodeName().equals("a")) {
					refertext_tag += "☆" + ((Element)node).text() + "★";
					if (node.hasAttr("href")) {
						String url = node.attr("href");
						/*
						 * /kcms/detail/detail.aspx?filename=JJXU201603010&dbcode=CJFQ&dbname=CJFDTEMP&v=
						 * /kcms/detail/detail.aspx?filename=SJES15122400022567&dbcode=SJES
						 */
						Matcher matDbcode = patDbcode.matcher(url);
						Matcher matFilename = patFilename.matcher(url);
						if (matDbcode.find() && matFilename.find()) {
							cnki_dbcode = matDbcode.group(1);
							cnki_filename = matFilename.group(1);
						}
					}
				}
				else {
					refertext_tag += ((Element)node).text();
				}
			}
			
			// 去掉最后的星号
			//refertext_tag = refertext_tag.replaceAll("★+$", "");
			

//			System.out.println("refertext:" + refertext);
//			System.out.println("refertext_tag:" + refertext_tag);
			
			return true;
		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();	    	
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			Map<String, String> mapJson = gson.fromJson(text, type);
			rawsourceid = mapJson.get("filename");
			String htmlText = mapJson.get("refhtm");
	    	
	    	
	    	Document doc = Jsoup.parse(htmlText);
	    	for (Element liEle : doc.select("li")) {
				procOneLi(liEle);
				
				if (refertext.length() < 1) {
					context.getCounter("map", "refertext blank").increment(1);
					continue;
				}
	    		
	    		XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawsourceid", rawsourceid);
				xObj.data.put("refertext", refertext);
				xObj.data.put("refertext_tag", refertext_tag);
				xObj.data.put("cnki_dbcode", cnki_dbcode);
				xObj.data.put("cnki_filename", cnki_filename);
				
				context.getCounter("map", "count").increment(1);
				
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(rawsourceid), new BytesWritable(bytes));		
			}
		}
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		
		private static String getLngIDByRawSourceID(String rawsourceid, int idx) {
			rawsourceid = rawsourceid.toUpperCase();
			String lngID = "";
			for (int i = 0; i < rawsourceid.length(); i++) {
				lngID += String.format("%d", rawsourceid.charAt(i) + 0);
			}
			lngID = lngID + String.format("%04d", idx);
			
			return lngID;
		}		
		
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			Set<String> refertextSet = new HashSet<String>();				
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("refertext")) {
						String refertext = updateItem.getValue().trim();
						
						if (refertextSet.contains(refertext)) {	//排除重复引文
							context.getCounter("reduce", "repeat ref").increment(1);
							continue;
						}

						refertextSet.add(refertext);
						
						context.getCounter("reduce", "count").increment(1);

						context.write(key, item);
					}
				}
			}
		}
	}
}
