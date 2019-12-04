package simple.jobstream.mapreduce.site.siamjournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.net.URI;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.collections.set.AbstractSerializableSetDecorator;
import org.apache.commons.lang.StringEscapeUtils;
import org.apache.hadoop.hbase.generated.thrift.thrift_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.FileSplit;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import com.sun.jersey.core.util.StringKeyIgnoreCaseMultivaluedMap;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
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
		public String inputPath = "";
		
		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
			inputPath = VipcloudUtil.GetInputPath((FileSplit)context.getInputSplit());	//两级路径
		}

		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	
	    	String line = value.toString().trim();
	    	 Gson gson = new Gson();
	         Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
	         Map<String, JsonElement> mapField = gson.fromJson(line, type);
	         
	         JsonObject jsonmsg = mapField.get("jsonmsg").getAsJsonObject();
	         String html = mapField.get("html").toString();
	         
	         String rawid = "";
	         String product = "";
	         String down_date = "";
	         String doi = "";
	         String title = "";
	         String author ="";
	         String pub_year = "";
	         String begin_page_sort = "";
	         String begin_page = "";
	         String end_page = "";
	         String page_cnt  = "";
	         String abstracts  = "";
	         //提交时间
	         String recv_date = "";
	         // 接受时间
	         String accept_date ="";
	         //出版时间
	         String pub_date = "";
	         String keyword = "";
	         //主题词
	         String subject_word = "";
	         //eissn 
	         String eissn = "";
	         //issn
	         String issn = "";
	         //publisher
	         String publisher = "";
	         //期刊名简写
	         String journal_id = "";
	         //期刊名
	         String journal_name = "";
	         //期
	         String issue = "";
	         //卷
	         String vol = "";
	         
	         String source_type="";
	         
	         String sub_db_id = "00023";
	         String sub_db = "QK";
	         
	        String provider = "SIAM";
	        String provider_url = "";
	        String raw_type = "Article";
	        String column_info= "";
	        String cited_cnt="0";
			String country = "US";
			String language= "EN";
			String author_1st = "";
	         
	         
	         
	         rawid = mapField.get("url").toString().replace("/doi/abs/", "").trim();
	         rawid = rawid.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         product = "SIAM";
	         down_date = mapField.get("date").toString().trim();
	         down_date = down_date.replaceAll("^\"", "").replaceAll("\"$", "").trim();
		     title = jsonmsg.get("title").toString().trim();
		     title = title.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         author = jsonmsg.get("authors").toString().trim();
	         author = author.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         author_1st = author.split(";")[0];
	         pub_year = jsonmsg.get("issueyear").toString();
	         String doijson = jsonmsg.get("doi").toString();
	         
	         String pattern = ".*\\((\\d{4})\\)";
	         // 创建 Pattern 对象
	         Pattern r = Pattern.compile(pattern);
	         // 现在创建 matcher 对象
	         Matcher m = r.matcher(pub_year);
	         if (m.find()) {
	              pub_year = m.group(1);
	         }
	         if (!DateTimeHelper.checkYearByRange(pub_year,1000,3000)) {
	        	 pub_year = "";
	         }
	         
	         begin_page_sort = jsonmsg.get("page").toString().replace("p", "").replace(".","").replace("P","").trim();
	         begin_page_sort = begin_page_sort.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         begin_page = begin_page_sort.split("-")[0];
	         end_page =begin_page_sort.split("-")[1];
	       
	         issue = jsonmsg.get("issue").toString().split(",")[0].replace("Issue", "").trim();
	         issue = issue.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         vol = jsonmsg.get("vol").toString().replace("Volume", "").trim();
	         vol = vol.replaceAll("^\"", "").replaceAll("\"$", "").trim();
	         
	         html = html.replace("\\0", "").replace("\\r", "").replace("\\n", "").replace("\\\"","\"");
			 Document doc = Jsoup.parse(html);
			 

			 String pagecount = "";
			 Element pagecountele = null;
			 pagecountele = doc.select("div.articleMeta").first();
			 pagecount =pagecountele.text();
			 // 按指定模式在字符串查找
			 pattern = ".*\\((\\d+) pages\\)";
	        // 创建 Pattern 对象
	        r = Pattern.compile(pattern);
	        // 现在创建 matcher 对象
	        m = r.matcher(pagecount);
	        if (m.find()) {
	        	page_cnt = m.group(1);
	        }
	        //doi
	        doi = doc.select("div.permaLinkSectionDoi > a").first().attr("href");
	        doi = doi.replace("https://doi.org/", "").trim();
	        //对比两个doi防止html不是该doi的html
	        doijson = doijson.replace("https://doi.org/", "").replace("\"","").trim();
	       
	        if (!doi.equals(doijson)) {
	        	context.getCounter("map", "countdoierr").increment(1);
	        	//throw new InterruptedException("doi is:"+doi+";"+doijson+";");
			
	        	return;
	        }
	        //abs
	        Elements abstractsEles = doc.select("div.abstractSection >div.abstractSection");
	        if (!abstractsEles.isEmpty()) {
	        	abstracts = abstractsEles.first().select("p").first().text();
	        }
				// TODO: handle exception
//	        
	        Element pubHsData = doc.select("div#pubHisDataDiv").first();
	        Element edatesdiv = pubHsData.select("div#historyPanel > div.box > div.box-inner > div").first();
	        Elements edates = edatesdiv.select("div");
	        //三个日期
	        for (Element edate : edates) {
	        	String edatestring = edate.text();
	        	if (edatestring.indexOf("Submitted:") > -1) {
	        		edatestring = edatestring.replace("Submitted:", "").trim();
	        		recv_date = DateTimeHelper.stdDate(edatestring);
	        	}else if (edatestring.indexOf("Accepted:") > -1) {
	        		edatestring = edatestring.replace("Accepted:", "").trim();
	        		accept_date = DateTimeHelper.stdDate(edatestring);
	        	}else if (edatestring.indexOf("Published online:") > -1) {
	        		edatestring = edatestring.replace("Published online:", "").trim();
	        		pub_date = DateTimeHelper.stdDate(edatestring);
	        	}
	        }
	        
	        Elements keyandsubeles = doc.select("div.hlFld-KeywordText > div");
	        //关键词
	        Elements Keywordseles = null;
	        Elements Subjectseles = null;
	        if (keyandsubeles.size() == 2) {
	        	Keywordseles = keyandsubeles.get(0).select("span");
	        	for (Element keywords: Keywordseles) {
	        		keyword += keywords.text().replace(",","").trim()+";";
	        	}
	        	keyword = StringHelper.cleanSemicolon(keyword);
	        //主题词
				 Subjectseles = keyandsubeles.get(1).select("span");

				 for (Element Subjects: Subjectseles) {
					 subject_word += Subjects.text().replace(",","").trim()+";";
				 }
				 subject_word = StringHelper.cleanSemicolon(subject_word);
	        }else if(keyandsubeles.size() == 1) {
	        	String keys = keyandsubeles.get(0).select("h3.keywords").first().text();
        		Keywordseles = keyandsubeles.get(0).select("span");
	        	for (Element keywords: Keywordseles) {
	        		keyword += keywords.text().replace(",","").trim()+";";
	        	}
	        	if (keys.equals("Keywords")) {
		        	keyword = StringHelper.cleanSemicolon(keyword);
	        	}else {
					 subject_word = StringHelper.cleanSemicolon(keyword);
	        	}	
	        }
	        
			 
	         //issn
	        Elements issneles = doc.select("div#publicationDataPanel >div.box >div.box-inner>div");
	        String issnstring = issneles.get(0).text();
	        if (issnstring.indexOf("ISSN (print):") > -1) {
	        	issn = issnstring.replace("ISSN (print):", "").trim();
	        }
	        else if (issnstring.indexOf("ISSN (online):") > -1) {
	        	eissn = issnstring.replace("ISSN (online):", "").trim();
	        }
	        String eissnstring = issneles.get(1).text();
	        if (eissnstring.indexOf("ISSN (online):") > -1) {
	        	eissn = eissnstring.replace("ISSN (online):", "").trim();
	        }
	        Element publishersele = doc.select("div#publicationDataPanel >div.box >div.box-inner>div.publishers>a").first();
	        publisher = publishersele.text().trim();     
	        journal_id = doc.select("ul.breadcrumbs > li").get(1).select("a").attr("href").replace("/loi/", "");
	        try {
	        	journal_name = doc.select("meta[name=citation_journal_title]").first().attr("content");
	        }catch (Exception e) {
	        	journal_name = doc.select("ul.breadcrumbs > li").get(1).select("a").text().trim();
			}
	        
	        source_type = "3";
	        provider_url = "https://epubs.siam.org"+mapField.get("url").getAsString();
	        column_info= doc.select("div.landSubjectHeading").first().text().trim();
	        cited_cnt="0";
	        Elements cites = doc.select("div.citedBySection");
			if (!cites.isEmpty()) {
				cited_cnt = String.valueOf(cites.first().select("div.citedByEntry").size());
			}
			
			String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);

			 
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
			xObj.data.put("begin_page_sort", begin_page_sort);
			xObj.data.put("begin_page", begin_page);
			xObj.data.put("end_page", end_page);
			xObj.data.put("page_cnt", page_cnt);
			xObj.data.put("abstracts", abstracts);
			xObj.data.put("recv_date", recv_date);
			xObj.data.put("accept_date", accept_date);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("keyword", keyword);
			xObj.data.put("subject_word", subject_word);
			xObj.data.put("eissn", eissn);
			xObj.data.put("issn", issn);
			xObj.data.put("publisher", publisher);
			xObj.data.put("journal_id", journal_id);
			xObj.data.put("journal_name", journal_name);
			xObj.data.put("issue", issue);
			xObj.data.put("vol", vol);
			xObj.data.put("source_type", source_type);
			xObj.data.put("sub_db_id", sub_db_id);
			xObj.data.put("sub_db", sub_db);
			xObj.data.put("provider", provider);
			xObj.data.put("provider_url", provider_url);
			xObj.data.put("raw_type", raw_type);
			xObj.data.put("column_info", column_info);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("author_1st", author_1st);
			
			context.getCounter("map", "count").increment(1);
				
				
				
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));	
				
		}		
		
		
	}
}
