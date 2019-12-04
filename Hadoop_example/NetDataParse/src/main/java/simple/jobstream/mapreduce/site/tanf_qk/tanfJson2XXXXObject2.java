package simple.jobstream.mapreduce.site.tanf_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.security.acl.Group;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.time.Year;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.management.MalformedObjectNameException;
import javax.print.DocFlavor.STRING;
import javax.print.attribute.standard.DateTimeAtCompleted;

import org.apache.avro.JsonProperties.Null;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hbase.generated.thrift.thrift_jsp;
import org.apache.hadoop.hbase.thrift.generated.Hbase.atomicIncrement_args;
import org.apache.hadoop.hdfs.server.datanode.dataNodeHome_jsp;
import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.log4j.Logger;
import org.apache.xerces.impl.dv.xs.DayDV;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.jsoup.select.Elements;
import org.jsoup.select.Evaluator.AllElements;
import org.mockito.internal.matchers.And;

import com.cloudera.io.netty.handler.codec.http.HttpContentEncoder.Result;
import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.AuthorOrgan;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class tanfJson2XXXXObject2 extends InHdfsOutHdfsJobInfo{
	private static Logger logger = Logger.getLogger(tanfJson2XXXXObject2.class);
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
		job.setJobName("tandfj."+jobName);
		
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
		
		Map<String, String> monthMap = new HashMap<String, String>(){{
		    put("january", "01");
		    put("february", "02");
		    put("februaryy", "02");
		    put("march", "03");
		    put("april", "04");
		    put("may", "05");
		    put("june", "06");
		    put("july", "07");
		    put("august", "08");
		    put("september", "09");
		    put("october", "10");
		    put("november", "11");
		    put("december", "12");
		    put("jan", "01");
		    put("feb", "02");
		    put("mar", "03");
		    put("apr", "04");
		    put("jun", "06");
		    put("jul", "07");
		    put("aug", "08");
		    put("sept", "09");
		    put("sep", "09");
		    put("oct", "10");
		    put("nov", "11");
		    put("dec", "12");
		    }};
		
		
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
		          String pathfile = "/user/xujiang/logs/logs_map_jstor/" + nowDate + ".txt";
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
	    	
	    	context.getCounter("map", "inputcount").increment(1);
	    	
	    	String text = value.toString().trim();
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String,Object>>(){}.getType();
			try {
				Map<String, Object> mapField = gson.fromJson(text, type);	
			}catch (Exception e) {
				// TODO: handle exception
				context.getCounter("map", "errtext").increment(1);
				return;
			}
			
			Map<String, Object> mapField = gson.fromJson(text, type);	
			if (mapField.get("doi") == null) {
				context.getCounter("map", "doinull").increment(1);
				return;
			}
			
			if (mapField.get("jsonmsg") == null) {
				context.getCounter("map", "jsonmsgnull").increment(1);
				return;
			}
			
			if (mapField.get("absHtml") == null) {
				context.getCounter("map", "abshtmlnull").increment(1);
				return;
			}
			String refhtml = "";
			if (mapField.get("refHtml") != null) {
				refhtml = mapField.get("refHtml").toString();
				refhtml = refhtml.replace("\0", " ").replace("\r", " ").replace("\n", " ") + "\n";
			}

			String abshtml = mapField.get("absHtml").toString();
			abshtml = abshtml.replace("\0", " ").replace("\r", " ").replace("\n", " ") + "\n";
		
			
			//doi
			String doi="";
			doi = mapField.get("doi").toString().trim();

			
			
	    	if (abshtml.length() < 10) {
	    		 log2HDFSForMapper(context,"null html:"+doi);
	    		 return;
	    	}
	    	


	    	
			String id = "";
			String rawid = "";
			//关键字
			String keywords = "";
			//摘要
			String abstracts = "";
			//出版社
			String contentPublisher="";
			//标题
		 	String title = "";
		 	//issn
		 	String issn = "";
		 	//页面
			String page = "";
			String startpage="";
			String endpage="";
			//期刊名
			String source = "";
			//issue
			String issuenumber = "";
			//vol
			String vol = "";
			//year
			String date="1900";
			//
			String date_created = "1900000";
			//国家
			String country="";
			//语言
			String language="";
			//月
			String month = "00";
			//日
			String day = "00";
			
			String pageCount="";
			//作者
		    String author="";
		    //机构
		    String insitutionall = "";
		    String gch="";
		    String cited_cnt="0";
		    String ref_cnt = "0";
		    String url = "https://www.tandfonline.com/doi/abs/";
			
			country = getCountrybyString("英国");
			language = getLanguagebyCountry(country);
			
			String jsonmsg="";
			jsonmsg = mapField.get("jsonmsg").toString();
			try {
				Map<String, Object> jsonmsgobj = gson.fromJson(jsonmsg, type);
		    	//gch
		    	gch = jsonmsgobj.get("jid").toString().trim();
		    	//source
		    	source = jsonmsgobj.get("jname").toString().trim();
		    	issuenumber = jsonmsgobj.get("issue").toString().trim();
		    	vol = jsonmsgobj.get("vol").toString().trim();
			}catch (Exception e) {
				// TODO: handle exception
				//不知道为什么 里面会有 "jsonmsg": ["10.1080/01448765.2017.1333454", "{\"issue\": \"4\", \"date\": \"2017\", \"jid\": \"tbah20\"]
				//这种格式的数据  先抛弃不管 找了一段时间没找到原因
				log2HDFSForMapper(context,text+"\n");
				context.getCounter("map", "errjsonmsg").increment(1);
//				throw new InterruptedException("**********"+jsonmsg);
				return;
			}
			
			id = doi;
			rawid =doi;
			url = url+id;
//			vol = mapField.get("volumeNum").toString().trim();
//			issuenumber = mapField.get("issueNum").toString().trim();
			
//			//author可能不存在
//			if (mapField.containsKey("author")) {
//				author = mapField.get("author").toString().trim();
//				author = author.replaceAll("\\s*,\\s*",";");
//			}else {
//				author="";
//			}

			//有可能出现以作者列表为title的数据 例如 10.1080/00048623.2004.10755256
			if (mapField.containsKey("title")) {
				title = mapField.get("title").toString().trim();
			}else {
				if (mapField.containsKey("author")) {
					title = mapField.get("author").toString().trim();
				}
			}
			issn = mapField.get("issn").toString().trim();
			startpage = mapField.get("url").toString().trim();
			startpage = startpage.replace("startPage=", "").trim();
			endpage = mapField.get("endPage").toString().trim();
			page = startpage+"-"+endpage;
			
			pageCount = mapField.get("pageCount").toString().trim();
			contentPublisher = "Informa UK Limited";
			
			
			
			
			Document doc = Jsoup.parse(abshtml);
			if (title.equals("")) {
				Elements title_tag = doc.select("title");
				title = title_tag.first().text();
			}
			if (title.equals("")) {
				context.getCounter("map", "titlenull").increment(1);
				return;
			}
			//作者和机构
			Elements divauthors = doc.select("span.NLM_contrib-group > span.contribDegrees");
			LinkedHashMap author_insitution = new LinkedHashMap();
			for (Element author1_tag:divauthors) {
				Elements author_tag = author1_tag.select("a.entryAuthor");
				//可能没有
				if (!author_tag.isEmpty()) {
					author = author_tag.first().ownText().trim();
					if("".equals(author)) {
						continue;
					}
				}else {
					continue;
				}
				Elements insitution_tag = author1_tag.select("a > span.overlay");
				String insitution="";
				if (!insitution_tag.isEmpty()) {
					insitution = insitution_tag.first().text().trim();
				}
				author = author.replace("[","(");
				author_insitution.put(author, insitution);
			}
			if (!author_insitution.isEmpty()) {
//				throw new InterruptedException(doi+author_insitution.toString());
				try {
					String[] Result = AuthorOrgan.numberByMap(author_insitution);
					author = Result[0];
					insitutionall = Result[1];
				}catch (Exception e) {
					// TODO: handle exception
					throw new InterruptedException("author_insitution is"+author_insitution.toString()+":"+url);
				}
			}
			//摘要
			Elements divabsele = doc.getElementsByClass("abstractInFull");
			if (divabsele.isEmpty()) {
				abstracts = "";
			}else {
				abstracts = divabsele.first().text().trim();
			}
			//关键字
			Elements divkeyeles = doc.getElementsByClass("hlFld-KeywordText");
			if (!divkeyeles.isEmpty()) {
				Element keylist = divkeyeles.first();
				Elements akeyElements = keylist.getElementsByTag("a");
				if (!akeyElements.isEmpty()) {
					for (Element aElement:akeyElements) {
						keywords = keywords+aElement.text().trim()+";";
					}
					if (!keywords.equals("")) {
						keywords = keywords.substring(0,keywords.length()-1);
					}
					
				}
			}
			//时间
//			try {
				Element dateEle = doc.select("meta[name=dc.Date]").first();
				//我认为这个必须有 如果能找出确实没有这个的请对这里进行逻辑更改
				if (dateEle == null) {
					return;
				} 
				String datesstring = dateEle.attr("content");
				String [] arr = datesstring.split("\\s+");
				if (arr.length != 3) {
					// 1977-2-1 这种形式
					arr = datesstring.split("-");
					if (arr.length != 3) {
						throw new InterruptedException("doi is"+doi+"datesstring is:"+datesstring);
					}
					date = arr[0];
					month = arr[1];
					if (month.length() == 1) {
						month = "0"+month;
					} 
					day = arr[2];
					if (day.length() == 1) {
						day = "0"+day;
					} 
					
				}else {
					//05 Jan 1987 这种形式
					day = arr[0];
					if (day.length() == 1) {
						day = "0"+day;
					} 
					month = monthMap.get(arr[1].toLowerCase().trim()).toString();
					date = arr[2];	
				}

			date_created = date+month+day;
			
			//被引用量
			Element divEle = doc.select(".compactView").first();
			for (Element div:divEle.children()) {
				if (div.text().trim().contains("CrossRef citations")) {
					cited_cnt = div.select(".value").first().text().trim();
				}
			}
			
			//引用量
	    	if (refhtml.length() > 10) {
				Document refdoc = Jsoup.parse(refhtml);
				Element ulEle = refdoc.select("div > ul.numeric-ordered-list").first();
				if (ulEle != null) {
					ref_cnt = String.valueOf(ulEle.getElementsByTag("li").size());
				}
	    	}

	    	
	    	
            XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("id", id);
			xObj.data.put("subject", keywords);
			xObj.data.put("description", abstracts);
			xObj.data.put("publisher", contentPublisher);
			xObj.data.put("title", title);
			xObj.data.put("identifier_pissn", issn);
			xObj.data.put("page", page);
			xObj.data.put("identifier_doi", doi);
			xObj.data.put("source", source);
			xObj.data.put("issue", issuenumber);
			xObj.data.put("volume", vol);
			xObj.data.put("date", date);
			xObj.data.put("date_created", date_created);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("url", url);
			xObj.data.put("pageCount", pageCount);
			xObj.data.put("author", author);
			xObj.data.put("gch", gch);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("startpage", startpage);
			xObj.data.put("endpage", endpage);
			xObj.data.put("creator_institution", insitutionall);


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
