package simple.jobstream.mapreduce.site.intlpressjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.LinkedHashMap;
import java.util.Map;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		reduceNum = Integer.parseInt(job.getConfiguration().get("reduceNum"));
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
		job.setReducerClass(UniqXXXXObjectReducer.class); // ProcessReducer.class
		
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
    	public static String batch = "";
		static int cnt = 0;	
		
		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}

		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().trim();
	    	 Gson gson = new Gson();
	    	 //JsonElement
	         Type type = new TypeToken<Map<String, Object>>() {}.getType();
	         Map<String, Object> mapField = gson.fromJson(line, type);
	         
	         String articlehtml = mapField.get("articlehtml").toString();
	         String journalhtml = mapField.get("journalhtml").toString();
	         
	         String lngid = "";
	         String doi = "";
	         //作者机构原始信息
	         String author_raw = "";
	         String author ="";
	         String organ = "";
	         String author_1st = "";
    		 String organ_1st = "";
	         String abstract_ = "";
	         String recv_date = "";
	         String pub_date = "";
	         String revision_date = "";
	         String accept_date = "";
	         String fund = "";
	         String title = "";
	         String pub_year = "";
	         //期
	         String num = "";
	         //卷
	         String vol = "";
	         
	         String page_info  = "";
	         
	         String begin_page = "";
	         
	         String end_page = "";
	         
	         String journal_raw_id = "";
	         
	         String provider_url = "";
	         
	         //期刊名
	         String journal_name = "";
	         
	         //eissn 
	         String eissn = "";
	         //issn
	         String issn = "";
	         
	         //总引用次数
	         String cited_cnt= "";
	         
	         // 期刊影响因子(a表暂时没有)
	         String factor = "";
	         
	         String rawid = "";
	         String product = "INTLPRESS";
	         String down_date = "";
	         
	         String sub_db_id = "00125";
	         String sub_db = "QK";
	         
	        String provider = "INTLPRESS";
	        String provider_zt = "intlpressjournal";
	         
	         
	        String raw_type = "Article";
	        
	        String country = "US";
			String language= "EN";
			
			String source_type = "3";
			
			String publisher = "International Press of Boston, Inc.";
			
			String fulltext_type = "pdf";
			//主编
			String Chief = "";

	         
	         
	         pub_year = mapField.get("year").toString();
	         vol = mapField.get("vol").toString();
	         rawid = mapField.get("rawid").toString();
	         rawid = rawid.replaceAll("\"", "");
	         num = mapField.get("issue").toString();
	         page_info = mapField.get("pages").toString().trim();
	         page_info = page_info.replaceAll("pp.", "").trim();
	         if (page_info.equals("None")) {
	        	 page_info = "";
	         }
	         journal_raw_id = mapField.get("jid").toString();
	         down_date = mapField.get("downdate").toString();
	         down_date = down_date.replaceAll("\"", "");
	         provider_url = "https://www.intlpress.com/"+mapField.get("url").toString().replaceAll("\"", "");;
	         
	         if (page_info.indexOf("-") > -1) {
	        	 String[] page_list = page_info.split("-");
	 	         begin_page = page_list[0].trim();
	 	         end_page = page_list[1].trim();
	         }
	         //articlehtml = articlehtml.replace("\0", " ").replace("\r", " ").replace("\n", " ");
	         articlehtml = articlehtml.replace("\\0", "").replace("\\r", "").replace("\\n", "").replace("\\\"","\"");
	         journalhtml = journalhtml.replace("\\0", "").replace("\\r", "").replace("\\n", "").replace("\\\"","\"");
		     	
	    	 Document articledoc = Jsoup.parse(articlehtml);
	    	 Document journaldoc = Jsoup.parse(journalhtml);
	    	 
	    	 if (articledoc.select("span.doi_directurl").isEmpty()) {
	    		context.getCounter("map", "doinull").increment(1);
//	    		return;
	    	 }else {
	    		doi = articledoc.select("span.doi_directurl").first().text();
		    	doi = doi.replaceAll("http://dx.doi.org/", "").trim();	
	    	 }
	    	 
		        
			 lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
	    	 
	    	 //title
			 title = articledoc.select("h3.contentitem_title").first().text().trim();
			 
			 if (!articledoc.select("p.contentitem_authors").isEmpty()) {
				//作者机构
		    	 LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
		    	 
		    	 String oneauthor= articledoc.select("p.contentitem_authors").first().text().trim();
		    	 if (oneauthor.indexOf("(") > -1) {
	    			 String[] temp = oneauthor.split("\\(");
		    		 String mapauthor = temp[0].trim();
		    		 String maporg = temp[1].replaceAll("\\)", "").trim();
		    		 author_1st = mapauthor;
		    		 organ_1st = maporg;	 
	    		 }else{
	    			 author_1st = oneauthor;
		    		 organ_1st = "";	
	    			 
	    		 }	
		    	 
		    	 
		    	 Elements authororgan = articledoc.select("p.contentitem_authors");
		    	 
		    	 for (Element authtag:authororgan) {
		    		 
		    		 String authstring = authtag.text().trim();
		    		 author_raw = author_raw +authstring+";";
		    		 if (authstring.indexOf("(") > -1) {
		    			 String[] temp = authstring.split("\\(");
			    		 String mapauthor = temp[0].trim();
			    		 String maporg = temp[1].replaceAll("\\)", "").trim();
			    		 mapcre_ins.put(mapauthor, maporg); 
		    		 }else{
		    			 mapcre_ins.put(authstring, ""); 
		    		 }	 
		    		
		    	 } 
	            String[] result = AuthorOrgan.numberByMap(mapcre_ins);
	            author = result[0];
	            organ = result[1];
	            author_raw = StringHelper.cleanSemicolon(author_raw);
	            
			 }
	    	 
            //摘要 可能有多个p标签
            Elements abstracttags = articledoc.select("p.contentitem_abstract");
            for (Element oneabstag:abstracttags) {
            	abstract_ = abstract_+oneabstag.text().trim();
            }
            
            //Received 时间
            Elements date_list = articledoc.select("p.contentitem_paperreceiveddate");
            int i = 0;
            for (Element date_tag:date_list) {
            	i=0;
            	String dates = date_tag.text().trim();
            	if (dates.indexOf("Received revised") > -1) {
            		i=1;
            		revision_date = dates.replaceAll("Received revised", "").trim();
            	}else if (dates.indexOf("Received") > -1) {
            		i=1;
            		recv_date = dates.replaceAll("Received", "").trim();
            	}else if (dates.indexOf("Paper received on") > -1) {
            		i=1;
            		recv_date = dates.replaceAll("Paper received on", "").trim();
            	}else if (dates.indexOf("Published") > -1) {
            		i=1;
            		pub_date = dates.replaceAll("Published", "").trim();
				}else if (dates.indexOf("Accepted") > -1) {
            		i=1;
            		accept_date = dates.replaceAll("Accepted", "").trim();
				}else if (dates.indexOf("Paper accepted on") > -1) {
            		i=1;
            		accept_date = dates.replaceAll("Paper accepted on", "").trim();
				}
            	
            	// 时间格式转换
            	revision_date = DateTimeHelper.stdDate(revision_date);
            	recv_date = DateTimeHelper.stdDate(recv_date);
            	pub_date = DateTimeHelper.stdDate(pub_date);
            	accept_date = DateTimeHelper.stdDate(accept_date);
            	
            	if (i==0) {
            		throw new InterruptedException("还有其他时间没有被我发现，请检查html;"+dates);
            	}
            }
            
            //基金
            if (!articledoc.select("div.contentitem_notes").isEmpty()) {
            	fund = articledoc.select("div.contentitem_notes").first().text().trim();
            }
            
            
            /////////////////////////////////////////////////////////////////
            
            journal_name = journaldoc.select("h1.journalnamefull").first().text().trim();
            
            
            String issnstrings = journaldoc.select("p.issn").first().text();
            String[] stringslist = issnstrings.split("Online");
            for (String issns:stringslist) {
            	if (issns.indexOf("Print") > -1) {
            		issn = issns.replaceAll("Print", "").replaceAll("ISSN", "").trim();
            		issn = issn.replace("  ", "").trim();
            	}else {
            		eissn = issns.replaceAll("Online", "").replaceAll("ISSN", "").trim();
				}
            }
            
            
            //主编
            Chief = journaldoc.select("p.editor").first().text();
            
            //影响因子和总引用次数
            Elements p_tags = journaldoc.select("div.metrics > p");
            for (Element p_tag:p_tags) {
            	String datas = p_tag.text();
            	if (datas.indexOf("Total Citations") > -1) {
            		cited_cnt = datas.replaceAll("Total Citations:", "").trim();
            		cited_cnt = cited_cnt+"@"+down_date;
            	}else if (datas.indexOf("Journal Impact Factor:") > -1) {
            		factor = datas.replaceAll("Journal Impact Factor:","").trim();
				}
            }
            if (pub_year == "") {
            	pub_year = "1900";
            }
            if (pub_date == "") {
            	pub_date = pub_year + "0000";
            }
            
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("lngid", lngid);
			xObj.data.put("rawid", rawid);
			xObj.data.put("product", product);
			xObj.data.put("down_date", down_date);
			xObj.data.put("batch", batch);
			xObj.data.put("doi", doi);
			xObj.data.put("title", title);
			xObj.data.put("author", author);
			xObj.data.put("pub_year", pub_year);
			xObj.data.put("begin_page", begin_page);
			xObj.data.put("end_page", end_page);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("eissn", eissn);
			xObj.data.put("issn", issn);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("vol", vol);
			xObj.data.put("source_type", source_type);
			xObj.data.put("sub_db_id", sub_db_id);
			xObj.data.put("sub_db", sub_db);
			xObj.data.put("provider", provider);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("raw_type", raw_type);
			//不作为期刊的引用次数 加屏蔽
			xObj.data.put("cited_cnts", cited_cnt);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("author_1st", author_1st);
			xObj.data.put("author_raw", author_raw);
			xObj.data.put("organ", organ);
			xObj.data.put("organ_1st", organ_1st);
			xObj.data.put("abstract", abstract_);
			xObj.data.put("revision_date", revision_date);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("fund", fund);
			xObj.data.put("num", num);
			xObj.data.put("page_info", page_info);
			xObj.data.put("journal_raw_id", journal_raw_id);
			xObj.data.put("factor", factor);
			xObj.data.put("provider_zt", provider_zt);
			xObj.data.put("publisher", publisher);
			xObj.data.put("fulltext_type", fulltext_type);
			xObj.data.put("Chief", Chief);
			
			context.getCounter("map", "count").increment(1);
				
				
				
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));	
				
		}		
		
		
	}
}
