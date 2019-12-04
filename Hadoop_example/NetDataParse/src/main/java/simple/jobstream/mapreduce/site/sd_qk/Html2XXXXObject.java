package simple.jobstream.mapreduce.site.sd_qk;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import com.google.gson.JsonArray;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将html格式转化为XXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "sd_qk." + this.getClass().getSimpleName();
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
		static int sourceNullCnt = 0;
		static int issnNullCnt = 0;
		private static Map<String, String> mapMonth = new HashMap<String, String>();
		
		private static String title = "";
		private static String title_alternative = "";
		private static String creator = "";
		private static String creator_institution = "";
		private static String subject = "";
		private static String description = "";
		private static String description_en = "";
		private static String identifier_doi = "";
		private static String identifier_pissn = "";
		private static String volume = "";
		private static String issue = "";
		private static String source = "";
		//private static String publisher = "Elsevier Science";
		private static String date = "";
		private static String page = "";
		private static String date_created = "";
		
		private static int curYear = Calendar.getInstance().get(Calendar.YEAR);
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			initMapMonth();	//初始化月份map
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
		        String pathfile = "/user/qhy/log/log_map/" + nowDate + ".txt";
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
		
		public static void initMapMonth() {
			mapMonth.put("january","01");
			mapMonth.put("february","02");
			mapMonth.put("march","03");
			mapMonth.put("april","04");
			mapMonth.put("may","05");
			mapMonth.put("june","06");
			mapMonth.put("july","07");
			mapMonth.put("august","08");
			mapMonth.put("september","09");
			mapMonth.put("october","10");
			mapMonth.put("november","11");
			mapMonth.put("december","12");
			
			mapMonth.put("spring","03");
			mapMonth.put("summer","06");
			mapMonth.put("autumn","09");
			mapMonth.put("winter","12"); 	//http://www.sciencedirect.com/science/article/pii/S0251108883800370
			
			mapMonth.put("1st quarter","03");	//http://www.sciencedirect.com/science/article/pii/S1084856801000451
			mapMonth.put("2nd quarter","06");
			mapMonth.put("3rd quarter","09");
			mapMonth.put("4th quarter","12"); 	//http://www.sciencedirect.com/science/article/pii/S0251108883800370
		}
		
		private static boolean isNumeric(String str){ 
		   Pattern pattern = Pattern.compile("[0-9]*"); 
		   Matcher isNum = pattern.matcher(str);
		   if( !isNum.matches() ){
		       return false; 
		   } 
		   return true; 
		}
	
		// 获取date，date_created
		public void getDateCreated(String line) {
			//System.out.println("getDateCreated line:" + line);
			for (String item : line.split(",")) {
				item = item.trim();
				Pattern pattern = Pattern.compile("^(19|20)\\d{2}$");
				Matcher matcher = pattern.matcher(item);
				if (matcher.find()) {	//只有年
					date = item;	
					date_created = date + "0000";
					break;
				}
				
				boolean flag = false;				
				for (String month : mapMonth.keySet()) {	//找月
					if (item.toLowerCase().indexOf(month) > -1) {
						flag = true;
						break;
					}
				}
				
				if (!flag) {
					continue;
				}
				
				String year = "1900";
				String month = "00";
				String day = "00";
				for (Map.Entry<String, String> entry : mapMonth.entrySet()) {  
					line = item.toLowerCase().trim();		//29 April 1961
					int idx = line.indexOf(entry.getKey());
					if (idx > -1) {
						month = entry.getValue();
						if (idx > 0) {
							String tmp = line.substring(0, idx).trim();
							if (isNumeric(tmp)) {
								if (Integer.parseInt(tmp) <= 31) {		//是数字并且小等于31
								    // 0 代表前面补充0     
								    // 4 代表长度为4     
								    // d 代表参数为正数型     
								    day = String.format("%02d", Integer.parseInt(tmp));    
								}
							}
						}
						break;
					}
				}  
				pattern = Pattern.compile("^.*(\\d{4})$");
				matcher = pattern.matcher(item);
				if (matcher.find()) {
					year = matcher.group(1).trim();								
				}
				
				date = year;
				date_created = year + month + day;
				break;
			}
			
//			System.out.println("date:" + date);
//			System.out.println("date_created:" + date_created);
		}
		
		public boolean parseArticleTitle(String rawid, Document doc) {	
			
			// json 字符串
			String jsonString = "";
			JsonObject jsonObj = null;
			Element eleJson = doc.select("script[type=application/json]").first();
			if (eleJson != null) {
				jsonString = eleJson.html().trim();
				jsonObj = new JsonParser().parse(jsonString).getAsJsonObject();				
			}

			Element eleArticleContent = doc.select("div.article-wrapper").first();
			if (eleArticleContent == null) {
				//System.out.println("eleArticleContent == null");
				return false;
			}

			/**************************** begin title ****************************/
			{
				title = title_alternative = "";
				
				Element eleTitle = eleArticleContent.select("h1.article-title").first();
				Element eleDochead = eleArticleContent.select("div.Head > div.article-dochead").first();
				if (eleTitle != null) {
					title = eleTitle.text().trim();
				}
				else if (eleDochead != null) {
					title = eleDochead.text().trim();
				}

				if (title.length() < 1) {
					title = doc.title().trim();
					if (title.endsWith("- ScienceDirect")) {
						title = title.substring(0, title.length()-"- ScienceDirect".length()).trim();
					}
				}

				title = title.replaceAll("[ ☆]*$", "");		//删除末尾的☆
				if (title.length() < 1) {
					return false;
				}
				
				
				Element eleTitle_alt = eleArticleContent.select("div.article-title-alt").first();
				if (eleTitle_alt != null) {
					title_alternative = eleTitle_alt.text().trim();
				}

				System.out.println("title:" + title);	
//				System.out.println("title_alternative:" + title_alternative);	
			}		
			/**************************** end title ****************************/
			
			/***************************** begin creator creator_institution ********************************/
			{
				creator = "";	
				creator_institution = "";
				Element eleAuthorGroup = eleArticleContent.select("div.AuthorGroups > div.author-group").first();
				if (eleAuthorGroup != null) {

					for (Element eleAuthor : eleAuthorGroup.select("a > span.content")) {
						Element spanGivenName = eleAuthor.select("span.text.given-name").first();
						Element spanSurName = eleAuthor.select("span.text.surname").first();
						
						String author = "";
						if (spanGivenName != null) {
							author = spanGivenName.text().trim();
						}
						if (spanSurName != null) {
							author += " " + spanSurName.text().trim();
						}
						
						String sup = "";
						for (Element eleSup : eleAuthor.select("span.author-ref > sup")) {
							sup += eleSup.text() + ",";
						}
						sup = sup.replaceAll(",+?$", "");	//去掉尾部多余逗号
						
						if (sup.length() > 0) {
							author += "[" + sup + "]";
						}

						creator += author + ";";
					}
					creator = creator.replaceAll("[ ;]+?$", "");	//去掉尾部多余的分号
					creator = creator.replaceAll("\\[\\]$", "");		//去掉尾部空的中括号

					if (jsonObj != null) {
						try {
							JsonObject organObj = jsonObj.get("authors").getAsJsonObject().get("affiliations").getAsJsonObject();
							
							Type type = new TypeToken<Map<String,  JsonObject>>(){}.getType();
							Gson gson = new Gson();				
							Map<String,  JsonObject> mapJson = gson.fromJson(organObj, type);
							for (JsonObject obj : mapJson.values()) {  	 
							    JsonArray array = obj.get("$$").getAsJsonArray();	
							    //System.out.println(rawid + " array:" + array);
							    if (array.get(0).getAsJsonObject().get("#name").getAsString().equals("label")) {	//多个机构
							    	String organ = "[" + array.get(0).getAsJsonObject().get("_").getAsString() + "]" + array.get(1).getAsJsonObject().get("_").getAsString();
								    creator_institution += organ + ";";
								}
							    else if (array.get(0).getAsJsonObject().get("#name").getAsString().equals("textfn")) {	//单个机构
							    	creator_institution = array.get(0).getAsJsonObject().get("_").getAsString().trim();
								}
							    
							}  
						} catch (Exception e) {
							String text = "Errror:" + rawid + "\t" + e.toString();
							System.out.println(text);
						}
					}
					creator_institution = creator_institution.replaceAll(";+?$", "");	//去掉尾部多余分号
				}				
				
//				System.out.println("creator:" + creator);
//				System.out.println("creator_institution:" + creator_institution);
			}
			/*************************** end  creator creator_institution **************************/
			
			/*************************** begin subject **************************/
			{
				subject = "";
				Element eleKeywords = eleArticleContent.select("div.Keywords").first();
				if (eleKeywords != null) {
					for (Element divKeyord : eleKeywords.select("div.keyword")) {
						subject += divKeyord.text().trim() + ";";
					}
				}	
				subject = subject.replaceAll(";+?$", "");	//去掉尾部多余分号
//				System.out.println("subject:" + subject);
			}
			/*************************** end subject **************************/
			
			/*************************** begin description description_en **************************/
			{
				//多摘要如：0001616053900030
				description = description_en = "";				
				Element eleAbs = eleArticleContent.select("div.Abstracts").first();
				//System.out.println("div.Abstracts:" + eleAbs.text());
				if (eleAbs != null) {
					int cnt = 0;
					for (Element ele : eleAbs.select("div > div[id^=aep-abstract-sec]")) {
						++cnt;
						if (1 == cnt) {
							description = ele.text().trim();
						}
						else if (2 == cnt) {
							description_en = ele.text().trim();
							break;
						}
					}
					
					if (description.length() < 1) {
						Element eleH2 = eleAbs.select("h2:contains(Abstract)").first();
						if (eleH2 != null) {
							eleH2.remove();
						}						
						description = eleAbs.text().trim();
					}
				}
//				System.out.println("description:" + description);
//				System.out.println("description_en:" + description_en);
			}
			/*************************** end description description_en **************************/
			
			/******************* begin identifier_doi ************************/		
			{
				identifier_doi = "";
				Element eleDoi = eleArticleContent.select("div.DoiLink > a.doi").first();
				if (eleDoi != null) {
					String url = eleDoi.attr("href").trim();
					
					final String regex = "doi\\.org/(.*)$";

					final Pattern pattern = Pattern.compile(regex);
					final Matcher matcher = pattern.matcher(url);
					
					if (matcher.find()) {
						identifier_doi = matcher.group(1);
					}
				}
				//System.out.println("identifier_doi:" + identifier_doi);
			}
			/******************* end identifier_doi ************************/	
			
			/**************************** begin identifier_pissn ******************************/
			{

				identifier_pissn = "";
				Element eleAJournal = eleArticleContent.select("div.Publication > div.publication-brand > a").first();
				if (eleAJournal == null) {
					eleAJournal = eleArticleContent.select("h2#publication-title > a").first();
				}
				if (eleAJournal != null) {
					String url = eleAJournal.attr("href").trim();
					Pattern pattern = Pattern.compile("science/journal/([0-9a-zA-z]{8})$");	//issn的字符不全是数字
					Matcher matcher = pattern.matcher(url);
					if (matcher.find()) {
						identifier_pissn = matcher.group(1).trim();			
					}
				}
			}
//			System.out.println("identifier_pissn:" + identifier_pissn);
			/**************************** end identifier_pissn ******************************/			
			
			/**************************** begin identifier_pissn 年卷期页 ******************************/
			//S0892059189706212: #react-root > div > div > div > div > section > div > div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right > div.Publication > div.publication-volume > h2 > span > a
	    	//S0140673600025459: #react-root > div > div > div > div > section > div > div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right > div.Publication > div.publication-brand > a
			{//卷期处无链接：S2314728816300034
				date = "1900"; 
				date_created = "19000000";
				volume = issue = page = "";
				
				//#publication > div.publication-brand > div
				//#publication > div.publication-volume.u-text-center > div.text-xs
				//Element eleVolIssue = eleArticleContent.select("div#publication > div > span.size-m").first();
				Element eleVolIssue = eleArticleContent.select("div#publication > div > div.text-xs").first();
				if (eleVolIssue != null) {
					String line = eleVolIssue.text();
					
//					System.out.println(eleArticleContent.select("div#publication > div").first().html());
//					System.out.println("********************");
//					System.out.println(eleVolIssue.html());
//					System.out.println("line:" + line);
					getDateCreated(line);
					
					if (identifier_pissn.length() < 1) {		//如果前面没获取到issn
						Element aElement = eleVolIssue.select("a[href*=science/journal/]").first();
						if (aElement != null) {
							String url = aElement.attr("href").trim();
							Pattern pattern = Pattern.compile("science/journal/([0-9a-zA-z]{8})/");	//issn的字符不全是数字
							Matcher matcher = pattern.matcher(url);
							if (matcher.find()) {
								identifier_pissn = matcher.group(1).trim();			
							}
						}
					}
					for (String item : line.split(",")) {
						item = item.trim();
						if (item.startsWith("Volume")) {
							volume = item.substring("Volume".length()).trim();
						}
						else if (item.startsWith("Issue")) {
							if (item.startsWith("Issues")) {
								issue = item.substring("Issues".length()).trim();	//多期
							}
							else {
								issue = item.substring("Issue".length()).trim();	//单期
							}
							issue = issue.replace('–', '-');	//替换特殊的短横
						}
						else if (item.startsWith("Page")) {
							if (item.startsWith("Pages")) {
								page = item.substring("Pages".length()).trim();	//多页
							}
							else {
								page = item.substring("Page".length()).trim();	//单页:S0008418216310754
							}
						}
						else if (item.toLowerCase().startsWith("article")) {
							//排除对年的干扰（eg.S2405844017302797）
						}
						else {	//年月日
//							String year = "1900";
//							String month = "00";
//							String day = "00";
//							for (Map.Entry<String, String> entry : mapMonth.entrySet()) {  
//								line = item.toLowerCase().trim();		//29 April 1961
//								int idx = line.indexOf(entry.getKey());
//								if (idx > -1) {
//									month = entry.getValue();
//									if (idx > 0) {
//										String tmp = line.substring(0, idx).trim();
//										if (isNumeric(tmp)) {
//											if (Integer.parseInt(tmp) <= 31) {		//是数字并且小等于31
//											    // 0 代表前面补充0     
//											    // 4 代表长度为4     
//											    // d 代表参数为正数型     
//											    day = String.format("%02d", Integer.parseInt(tmp));    
//											}
//										}
//									}
//									break;
//								}
//							}  
//							Pattern pattern = Pattern.compile("^.*(\\d{4})$");
//							Matcher matcher = pattern.matcher(item);
//							if (matcher.find()) {
//								year = matcher.group(1).trim();								
//							}
//							
//							date = year;
//							date_created = year + month + day;
						}
					}
				}
				
				
				
				System.out.println("identifier_pissn:" + identifier_pissn);
				System.out.println("date:" + date);
				System.out.println("date_created:" + date_created);
				System.out.println("volume:" + volume);
				System.out.println("issue:" + issue);
				System.out.println("page:" + page);
			}
			/**************************** end identifier_pissn 年卷期页 ******************************/
			
			/******************* begin source(刊名) ************************/
			//图片刊名：014067369093348S
			source = "";
			Element eleJournal = eleArticleContent.select("a.publication-title-link").first();
			if (eleJournal != null) {
				source = eleJournal.text().trim();
			}	
			System.out.println("source:" + source);
			/******************* end source(刊名) ************************/		
			
			return true;
		}
				
		
		//<h1 class="svTitle" id="tit0005">Institutional shareholder </h1>
		public boolean parseSvTitle(String rawid, Document doc) {				
			//Document doc = Jsoup.parse(html);

			Element eleCenterInner = doc.getElementById("centerInner");
			if (eleCenterInner == null) {
				return false;
			}
			
			/************************** begin title ************************/
			{
				title = title_alternative = "";
				Element eleTitle = eleCenterInner.select("h1.svTitle").first();
				Element elePublicationType = eleCenterInner.select("div.publicationType > span").first();	
				if (eleTitle != null) {
					title = eleTitle.text().trim();
				}
				else if (elePublicationType != null) {
					title = elePublicationType.text().trim();
				}
				
				if (title.length() < 1) {
					title = doc.title().trim();
					if (title.endsWith("- ScienceDirect")) {
						title = title.substring(0, title.length()-"- ScienceDirect".length()).trim();
					}
				}
				
				title = title.replaceAll("[ ☆]*$", "");		//删除末尾的☆
				
				//System.out.println("title:" + title);
				//System.out.println("title_alternative:" + title_alternative);
			}
			/************************** end title ************************/
			
			/***************************** begin creator ********************************/
			{
				creator = "";
				Element eleUlAuthor = eleCenterInner.select("ul.authorGroup.noCollab.svAuthor").first();				
				if (eleUlAuthor != null) {
					for (Element eleLi : eleUlAuthor.getElementsByTag("li")) {
						for (Element eleA : eleLi.getElementsByTag("a")) {
							if (eleA.attr("title").toLowerCase().startsWith("footnote")) {
								eleA.remove(); 		//去掉脚注
							}
						}
						
						String liHtml = eleLi.html();
						int idx1 = liHtml.indexOf("<sup>");
						int idx2 = liHtml.lastIndexOf("</sup>");
						
						String newLiHtml = liHtml;
						if ((idx1>0) && (idx2>idx1)) {					
							newLiHtml = liHtml.substring(0, idx1+5) + "[";
							newLiHtml += liHtml.substring(idx1+5, idx2) + "]";
							newLiHtml += liHtml.substring(idx2);
						}
	
						eleLi = eleLi.html(newLiHtml);	
						
						//去掉尾部中括号前后的空格和逗号
						String author = eleLi.text().replaceAll("[, ]*?\\][, ]*?$", "]");
						//去掉尾部的逗号
						author = author.replaceAll(",+?$", "");
						//去掉中间的[, ]
						author = author.replaceAll("\\[[, ]+?\\]", "");
						// 去掉空的中括号
						author = author.replace("[]", "");
						
						//System.out.println(eleLi.text());
						//System.out.println(author);
						creator += author + ";";
					}
				}
				creator = creator.replaceAll("[ ;]+?$", "");	//去掉尾部多余的分号
				creator = creator.replaceAll("\\[\\]$", "");		//去掉尾部空的中括号
				//System.out.println("creator:" + creator);
			}
			/***************************** end creator ********************************/
			
			/*************************** begin creator_institution **************************/
			Element eleUlAffil = eleCenterInner.select("ul.affiliation.authAffil").first();
			creator_institution = "";
			if (eleUlAffil != null) {
				for (Element eleLi : eleUlAffil.getElementsByTag("li")) {
					eleLi = eleLi.html(eleLi.html().replace("<sup>", "[").replace("</sup>", "]"));	//替换sup为中括号
					String affiliation = eleLi.text();
					creator_institution += affiliation + ";";
				}
			}
			creator_institution = creator_institution.replaceAll("[ ;]+?$", "");	//去掉尾部多余的分号
			//System.out.println("creator_institution:" + creator_institution);
			/*************************** end creator_institution **************************/
			
			/*************************** begin subject **************************/
			Element eleUlKeyword = eleCenterInner.select("ul.keyword").first();
			subject = "";
			if (eleUlKeyword != null) {
				subject = eleUlKeyword.text();
			}
			//System.out.println("subject:" + subject);
			/*************************** end subject **************************/
			
			/*************************** begin description **************************/
			{
				description = description_en = "";
				Element eleUlAbstract = eleCenterInner.select("div[class=abstract svAbstract]").first();			
				if (eleUlAbstract != null) {
					Element eleH2 = eleUlAbstract.select("h2.secHeading").first();
					if (eleH2 != null) {
						eleH2.remove();
					}
					description = eleUlAbstract.text();
				}
				//System.out.println("description:" + description);
			}
			/*************************** end description **************************/
			
			/******************* begin identifier_doi ************************/		
			{
				identifier_doi = "";
				String line = "";
				Pattern pattern = Pattern.compile("(SDM\\.pm\\.doi.*);");
				Matcher matcher = pattern.matcher(doc.html());
				if (matcher.find()) {
					line = matcher.group(1).trim();				
				}
				else {
					pattern = Pattern.compile("(SDM\\.doi.*);");
					matcher = pattern.matcher(doc.html());
					if (matcher.find()) {
						line = matcher.group(1).trim();					
					}
				}
				if (line.length() > 6) {
					pattern = Pattern.compile("'(.*?)'");
					matcher = pattern.matcher(line);
					if (matcher.find()) {
						identifier_doi = matcher.group(1).trim();					
					}
				}
				
				//System.out.println("identifier_doi:" + identifier_doi);
			}
			/******************* end identifier_doi ************************/	
			
			/******************* begin identifier_pissn ************************/		
			{
				identifier_pissn = "";
				Pattern pattern = Pattern.compile("(SDM\\.pm\\.issnisbn.*);");
				Matcher matcher = pattern.matcher(doc.html());
				if (matcher.find()) {
					String line = matcher.group(1).trim();	
					pattern = Pattern.compile("\"(.*?)\"");
					matcher = pattern.matcher(line);
					if (matcher.find()) {
						identifier_pissn = matcher.group(1).trim();					
					}
				}
				
				//System.out.println("identifier_pissn:" + identifier_pissn);
			}
			/******************* end identifier_pissn ************************/	
			
			/**************************** begin 年卷期页 ******************************/
			{
				date = "1900"; 
				date_created = "19000000";
				volume = issue = page = "";
				
				Element eleVolIssue = eleCenterInner.select("p.volIssue").first();
				if (eleVolIssue != null) {
					String line = eleVolIssue.text();
					//System.out.println("line:" + line);
					getDateCreated(line);
					if (identifier_pissn.length() < 1) {		//如果前面没获取到issn
						Element aElement = eleVolIssue.select("a[href*=science/journal/]").first();
						if (aElement != null) {
							String url = aElement.attr("href").trim();
							Pattern pattern = Pattern.compile("science/journal/([0-9a-zA-z]{8})/");	//issn的字符不全是数字
							Matcher matcher = pattern.matcher(url);
							if (matcher.find()) {
								identifier_pissn = matcher.group(1).trim();			
							}
						}
					}
					for (String item : line.split(",")) {
						item = item.trim();
						if (item.startsWith("Volume")) {
							volume = item.substring("Volume".length());
						}
						else if (item.startsWith("Issue")) {
							if (item.startsWith("Issues")) {
								issue = item.substring("Issues".length());	//多期
							}
							else {
								issue = item.substring("Issue".length());	//单期
							}
							issue = issue.replace('–', '-');	//替换特殊的短横
						}
						else if (item.startsWith("Page")) {
							if (item.startsWith("Pages")) {
								page = item.substring("Pages".length()).trim();	//多页
							}
							else {
								page = item.substring("Page".length()).trim();	//单页:S0008418216310754
							}
						}
						else if (item.toLowerCase().startsWith("article")) {
							//排除对年的干扰（eg.S2405844017302797）
						}
						else {	//年月
//							String year = "1900";
//							String month = "00";
//							for (Map.Entry<String, String> entry : mapMonth.entrySet()) {  
//								
//								
//								if (item.toLowerCase().startsWith(entry.getKey())) {
//									month = entry.getValue();
//									break;
//								}
//							}  
//							Pattern pattern = Pattern.compile("^.*(\\d{4})$");
//							Matcher matcher = pattern.matcher(item);
//							if (matcher.find()) {
//								year = matcher.group(1).trim();								
//							}
//							
//							date = year;
//							date_created = year + month + "00";
						}
					}
				}
//				System.out.println("date:" + date);
//				System.out.println("date_created:" + date_created);
//				System.out.println("volume:" + volume);
//				System.out.println("issue:" + issue);
//				System.out.println("page:" + page);
			}
			/**************************** end 年卷期页 ******************************/
			
			/******************* begin source(刊名) ************************/		
			source = "";
			Element eleJournal = eleCenterInner.select("div.title").first();
			if (eleJournal != null) {
				source = eleJournal.text().trim();
			}			
			if (source.length() < 1) {
				eleJournal = eleCenterInner.select("div.title.title1 > a > img").first();
				if (eleJournal != null) {
					source = eleJournal.attr("alt").trim();
				}
			}
			//System.out.println("source:" + source);
			/******************* end source(刊名) ************************/		
			
			
			return true;
		}
		
		private String clearPage(String page) {
			
			page = page.trim().replaceAll("[^0-9a-zA-Z]+", "-");
			
			return page;
		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	context.getCounter("map", "input").increment(1);
	    	//防止混乱与错位
	    	{
	    		title = "";
	    		title_alternative = "";
	    		creator = "";
	    		creator_institution = "";
	    		subject = "";
	    		description = "";
	    		description_en = "";
	    		identifier_doi = "";
	    		identifier_pissn = "";
	    		volume = "";
	    		issue = "";
	    		source = "";
	    		date = "";
	    		page = "";
	    		date_created = "";
	    	}
	    	
	    	String line = value.toString().trim();
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>(){}.getType();
			Map<String, String> mapJson = gson.fromJson(line, type);
			String rawid = mapJson.get("rawid");
			String down_date = mapJson.get("DownDate");
			String html = mapJson.get("detail");
	    	
	    	Document doc = Jsoup.parse(html);
	    	if (doc.select("#centerInner > div.head.headTemplate > div.publicationHead").first() != null) {
	    	//0027510772900279: #centerInner > div.head.headTemplate.page_fragment_ind > div.publicationHead > div > a
	    		parseSvTitle(rawid, doc);
	    		context.getCounter("map", "svTitle").increment(1);	    		
			}	    	
	    	else if (doc.select("div.article-wrapper > article > div.Publication").first() != null) {
	    	//S0892059189706212: #react-root > div > div > div > div > section > div > div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right > div.Publication > div.publication-volume > h2 > span > a
	    	//S0140673600025459: #react-root > div > div > div > div > section > div > div.article-wrapper > div.cl-m-3-3.cl-t-6-9.cl-l-6-12.pad-left.pad-right > div.Publication > div.publication-brand > a
	    		parseArticleTitle(rawid, doc);
	    		context.getCounter("map", "ArticleTitle").increment(1);
			}
	    	else {
	    		context.getCounter("map", "error feature").increment(1);
	    		log2HDFSForMapper(context, "error feature:" + rawid);
	    		return;
			}
	    	
	    	
	    	//没有title的数据不能要
	    	if (title.length() < 1) {
	    		context.getCounter("map", "no title").increment(1);
	    		log2HDFSForMapper(context, "no title: " + rawid);
	    		return;
			}
	    	
	    	//identifier_pissn不应该不存在
	    	if (identifier_pissn.length() < 1) {
	    		log2HDFSForMapper(context, "issnNull text:" + value.toString());
	    		context.getCounter("map", "identifier_pissn.length() < 1").increment(1);	    		
	    		log2HDFSForMapper(context, "no identifier_pissn: " + rawid);
	    		return;
			}
	    	
	    	{// 20180910 临时特殊处理
	    		if (rawid.startsWith("S17654629183")) {
					date = "2018";
					date_created = "2018" + date_created.substring(4);
				}
	    	}
	    	
	    	if ((Integer.parseInt(date) < 1000) || (Integer.parseInt(date) > curYear+1)) {
	    		context.getCounter("map", "err_date").increment(1);
	    		log2HDFSForMapper(context, "err_date:" + date + ";" + rawid);
	    		return;
			}
	    	
	    	if (date.equals("1900")) {
	    		context.getCounter("map", "warning:date_1900").increment(1);
	    		log2HDFSForMapper(context, "date_1900:" + rawid);
			}
	    	
	    	//一些统计数据
	    	{	
		    	if (source.length() < 1) {
		    		sourceNullCnt += 1;
		    		if (sourceNullCnt < 3) {
		    			log2HDFSForMapper(context, "sourceNull text:" + value.toString());
					}
		    		context.getCounter("map", "source.length() < 1").increment(1);
				}
		    	
		    	
	    	}
	    	
	    	XXXXObject xObj = new XXXXObject();
	    	xObj.data.put("parse_time", (new SimpleDateFormat("yyyy-MM-dd_kk:mm:ss")).format(new Date()));
	    	xObj.data.put("down_date", down_date);
			xObj.data.put("rawid", rawid);
	    	xObj.data.put("title", title.trim());
	    	xObj.data.put("title_alternative", title_alternative.trim());
	    	xObj.data.put("creator", creator.trim());
	    	xObj.data.put("creator_institution", creator_institution.trim());
	    	xObj.data.put("subject", subject.trim());
	    	xObj.data.put("description", description.trim());
	    	xObj.data.put("description_en", description_en.trim());
	    	xObj.data.put("identifier_doi", identifier_doi.trim());
	    	xObj.data.put("identifier_pissn", identifier_pissn.trim());
	    	xObj.data.put("volume", volume.trim());
	    	xObj.data.put("issue", issue.trim());
	    	xObj.data.put("source", source.trim());
	    	xObj.data.put("date", date.trim());
	    	xObj.data.put("page", clearPage(page).trim());
	    	xObj.data.put("date_created", date_created.trim());	
	    	
			
			context.getCounter("map", "out").increment(1);
			
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
