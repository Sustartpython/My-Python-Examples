package simple.jobstream.mapreduce.user.walker.cnki_qk_ref;

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

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.util.URLHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer4Ref;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为XXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 0;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
		job.setJobName(job.getConfiguration().get("jobName"));
	}
	
	public String getHdfsInput() {
		return inputHdfsPath;
	}

	public String getHdfsOutput() {
		return outputHdfsPath;
	}

	public void SetMRInfo(Job job)
	{
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.9f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer4Ref.class);
		
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
		
		private static String lngid = "";
		private static String sub_db_id = "";
		private static String product = "";
		private static String sub_db = "";
		private static String provider = "";
		private static String down_date = "";
		private static String batch = "";
		private static String doi = "";
		private static String title = "";
		private static String title_alt = "";
		private static String page_info = "";
		private static String begin_page = "";
		private static String end_page = "";
		private static String jump_page = "";
		private static String raw_type = "";
		private static String author_1st = "";
		private static String author = "";
		private static String author_alt = "";
		private static String pub_year = "";
		private static String vol = "";
		private static String num = "";
		private static String publisher = "";
		private static String cited_id = "";
		private static String linked_id = "";
		private static String refer_text_raw = "";
		private static String refer_text_raw_alt = "";
		private static String refer_text_site = "";
		private static String refer_text_site_alt = "";
		private static String refer_text = "";
		private static String refer_text_alt = "";
		private static String source_name = "";
		private static String source_name_alt = "";
		private static String strtype = "";
		
		// 以下字段不属于A层标准字段
		private static String cnki_dbcode = "";
		
		
		private static Map<String, String> mapDB = new HashMap<String, String>();
		
		public void setup(Context context) throws IOException,
		InterruptedException {
			batch = context.getConfiguration().get("batch");
			
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
			{
				lngid = "";
				sub_db_id = "00002";
				product = "CNKI";
				sub_db = "CJFD";
				provider = "CNKI";
//				down_date = "";		// 无需重置
//				batch = "";			// 无需重置
				doi = "";
				title = "";
				title_alt = "";
				page_info = "";
				begin_page = "";
				end_page = "";
				jump_page = "";
				raw_type = "";
				author_1st = "";
				author = "";
				author_alt = "";
				pub_year = "";
				vol = "";
				num = "";
				publisher = "";
//				cited_id = "";	// 无需重置
				linked_id = "";
				refer_text_raw = "";
				refer_text_raw_alt = "";
				refer_text_site = "";
				refer_text_site_alt = "";
				refer_text = "";
				refer_text_alt = "";
				source_name = "";
				source_name_alt = "";
				strtype = "";
			}
			
			Element emElement = liElement.select("em").first();
			if (emElement != null) {
				emElement.remove();
			}
			
			refer_text_raw = liElement.toString();
			refer_text_site = liElement.text().trim();
			refer_text_site = 	refer_text_site.replaceAll("^\\[\\d+?\\]", "").trim();	//去掉最前面的中括号编号
			
			if (refer_text_site.equals(".")) {
				return false;
			}
			if (refer_text_site.length() < 1) {
				return false;
			}
			
			Element aElement = liElement.select("a[href*=filename=]").first();
			if (aElement != null) {
				String url = aElement.attr("href");
				cnki_dbcode = URLHelper.getParamValueFirst(url, "dbcode");
				linked_id = URLHelper.getParamValueFirst(url, "filename");
			}
			
//			for (Node node : liElement.childNodes()) {
//				//System.out.println("***" + node.nodeName());
//				if (node.nodeName().equals("#text")) {
//					//refertext_tag += node.toString().replaceAll("&nbsp;", "").trim();
//					refertext_tag += node.toString().trim();
//				}
//				else if (node.nodeName().equals("a")) {
//					refertext_tag += "☆" + ((Element)node).text() + "★";
//					if (node.hasAttr("href")) {
//						String url = node.attr("href");
//						/*
//						 * /kcms/detail/detail.aspx?filename=JJXU201603010&dbcode=CJFQ&dbname=CJFDTEMP&v=
//						 * /kcms/detail/detail.aspx?filename=SJES15122400022567&dbcode=SJES
//						 */
//						Matcher matDbcode = patDbcode.matcher(url);
//						Matcher matFilename = patFilename.matcher(url);
//						if (matDbcode.find() && matFilename.find()) {
//							cnki_dbcode = matDbcode.group(1);
//							cnki_filename = matFilename.group(1);
//						}
//					}
//				}
//				else {
//					refertext_tag += ((Element)node).text();
//				}
//			}
			
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
			if (mapJson.containsKey("down_date")) {
				down_date = mapJson.get("down_date");
			} 
			else {
				down_date = "20190101";
			}
			String cited_raw_id = mapJson.get("filename");
			cited_id = VipIdEncode.getLngid(sub_db_id, cited_raw_id, false) + "@" + cited_raw_id;
			String htmlText = mapJson.get("refhtm");
	    	
	    	
	    	Document doc = Jsoup.parse(htmlText);
	    	for (Element liEle : doc.select("li")) {
				procOneLi(liEle);
				
				if (refer_text_site.length() < 1) {
					context.getCounter("map", "refertext blank").increment(1);
					continue;
				}
	    		
				XXXXObject xObj = new XXXXObject();
				{
//					xObj.data.put("lngid", lngid);		// lngid 在 reduce 中生成
					xObj.data.put("sub_db_id", sub_db_id);
					xObj.data.put("product", product);
					xObj.data.put("sub_db", sub_db);
					xObj.data.put("provider", provider);
					xObj.data.put("down_date", down_date);
					xObj.data.put("batch", batch);
					xObj.data.put("doi", doi);
					xObj.data.put("title", title);
					xObj.data.put("title_alt", title_alt);
					xObj.data.put("page_info", page_info);
					xObj.data.put("begin_page", begin_page);
					xObj.data.put("end_page", end_page);
					xObj.data.put("jump_page", jump_page);
					xObj.data.put("raw_type", raw_type);
					xObj.data.put("author_1st", author_1st);
					xObj.data.put("author", author);
					xObj.data.put("author_alt", author_alt);
					xObj.data.put("pub_year", pub_year);
					xObj.data.put("vol", vol);
					xObj.data.put("num", num);
					xObj.data.put("publisher", publisher);
					xObj.data.put("cited_id", cited_id);
					xObj.data.put("linked_id", linked_id);
					xObj.data.put("refer_text_raw", refer_text_raw);
					xObj.data.put("refer_text_raw_alt", refer_text_raw_alt);
					xObj.data.put("refer_text_site", refer_text_site);
					xObj.data.put("refer_text_site_alt", refer_text_site_alt);
					xObj.data.put("refer_text", refer_text);
					xObj.data.put("refer_text_alt", refer_text_alt);
					xObj.data.put("source_name", source_name);
					xObj.data.put("source_name_alt", source_name_alt);
					xObj.data.put("strtype", strtype);
				}
				
				context.getCounter("map", "count").increment(1);
				context.getCounter("map", "down_date:" + down_date).increment(1);
				context.getCounter("map", "batch:" + batch).increment(1);
				
				byte[] bytes = VipcloudUtil.SerializeObject(xObj);
				context.write(new Text(cited_raw_id), new BytesWritable(bytes));		
			}
		}
	}
}
