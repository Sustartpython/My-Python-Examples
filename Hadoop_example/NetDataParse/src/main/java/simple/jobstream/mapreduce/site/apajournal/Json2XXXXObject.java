package simple.jobstream.mapreduce.site.apajournal;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashMap;
import java.util.LinkedHashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.commons.lang.StringUtils;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.tools.SimpleCopyListing;
import org.json.JSONObject;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.JsonObject;
import com.google.gson.JsonParser;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;
import simple.jobstream.mapreduce.common.vip.AuthorOrgan;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;
import simple.jobstream.mapreduce.site.cssci.Json2XXXXObject.ProcessMapper;
import simple.jobstream.mapreduce.common.util.DateTimeHelper;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 8;

	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";

	public void pre(Job job) {
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

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs",
				"org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));

		job.setInputFormatClass(TextInputFormat.class);
		job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(UniqXXXXObjectReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

		job.setNumReduceTasks(reduceNum);
	}

	public void post(Job job) {

	}

	public String GetHdfsInputPath() {
		return inputHdfsPath;
	}

	public String GetHdfsOutputPath() {
		return outputHdfsPath;
	}

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		public String batch = "";				
		
		public void setup(Context context) throws IOException,
			InterruptedException {
			batch = context.getConfiguration().get("batch");
		}
		
		
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	
	    	Gson gson = new Gson();
	    	Type type = new TypeToken<Map<String, String>>(){}.getType();
	    	Map<String, String> obj =new HashMap<String,String>();
            try {
            	obj = gson.fromJson(value.toString(), type);	
			} catch (Exception e) {
				context.getCounter("map", "er").increment(1);
				return;
				// TODO: handle exception
			}
			LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
			String author = "";
			String author_1st = "";
			String organ = "";
			String organ_1st = "";
			String title = "";
			String keyword = "";
			String pub_year = "";
			String vol = "";
			String num = "";
			String journal_name = "";
			String raw_type = "Article";
			String doi = "";
			String page_info = "";
			String begin_page = "";
			String end_page = "";
			String eissn  = "";
			String pissn = "";
			String recv_date = "";
			String accept_date = "";
			String pub_date = "";
			String publisher = "American Psychological Association";
			String abstract_ = "";
			String is_oa ="0";
			String lngid = "";
			String rawid = "";
			String product = "APA";
			String sub_db = "QK";
			String provider = "APA";
			String sub_db_id = "00067";
			String source_type = "3";
			String provider_url = "";
			String country = "US";
			String language = "EN";
			String journal_raw_id="";
			String SourcePI="";
			
			String down_date="20190419";

				author=obj.get("AuthorName").trim();
				  if (!author.equals("")) {
		            	
				 int intIndex = author.indexOf("No authorship");
			      if(intIndex == -1){
			    	 author=	author.replace(",",";").replace("; ",",").replace("'","''");
			       String [] author_1sts=author.split(";");
			    	 if (author_1sts.length>0) {
			    		 author_1st=author_1sts[0];
					}
			    	 
			      }	
			      else {
			    	  author=""; 
				}
					}
				  				
			
				 journal_raw_id=obj.get("PAJournalCode").trim();
				 
				 pub_year=obj.get("PublicationYear").trim();
				 title=obj.get("GivenDocumentTitle").trim();
				 
				 SourcePI=obj.get("SourcePI").trim(); 
					  if (!SourcePI.equals("")) {
						  
						  SourcePI=SourcePI.split("Vol")[1];
							 pub_date=SourcePI.split(",")[1];
							 pub_date=StringUtils.trimToNull( pub_date );
							 String[] pub_dates=pub_date.split(" ");
							 String month;
							 if (pub_dates.length>1) {
								 month=pub_dates[1];
							}
							 else {
								  month=pub_dates[0];
							}
//							  String months= months(month);
							 pub_date=pub_year+months(month)+"00";
							 
							 
							 page_info=SourcePI.split(",")[2];
							 page_info=StringUtils.trimToNull( page_info );
							 if (page_info.indexOf("-")==-1) {
								 begin_page=page_info;
								 end_page=page_info;
							 } else {
								begin_page=page_info.split("-")[0];
								end_page=page_info.split("-")[1];
							}
										  
									  }
				 
				
				 doi=obj.get("DOI").trim();
				 rawid=obj.get("UID").trim();
				 abstract_=obj.get("Abstract").trim();
				  if (!abstract_.equals("")) {
					  		
	             if (abstract_.indexOf("Objective")==-1)
	             {
	            	 String[] Abstracts=abstract_.split(":");
				 if (Abstracts.length>1) {
					 abstract_=Abstracts[1].replace("\"", "");
				 }
				 else {
					 abstract_=Abstracts[0].replace("\"", "");		
				}
				  Abstracts=abstract_.split("\\(PsycINFO");
				  abstract_=Abstracts[0].replace("\"", "");
	             }
	             else {
	            	 String[] Abstracts=abstract_.split("tive: ");
	    			 if (Abstracts.length>1) {
	    				 abstract_=Abstracts[1].replace("\"", "");
	    			 }
	    			 else {
	    				 abstract_=Abstracts[0].replace("\"", "");		
	    			}
	    			  Abstracts=abstract_.split("\\(PsycINFO");
	    			  abstract_=Abstracts[0].replace("\"", "");            				
				}
				  }
				  pissn=obj.get("ISSN").trim();
				  if (!pissn.equals("")) {
					 int intIndex = pissn.indexOf("Electronic");
					  if(intIndex ==-1){
						  eissn="";
						  int intIndexs = pissn.indexOf("Pri");
						  if(intIndexs ==-1){
							 	
							 	 pissn="";		
						  }
						  else {
							 pissn=pissn.split("\\(Pri")[0];
						}
						  
				      }	
				      else {
				    	 eissn=pissn.split("\\(Electronic")[0];
				    
				    	
				    	 if (pissn.indexOf("Pri")==-1) {
				    		 pissn="";
						}
				    	 else {
				    		 	 pissn=pissn.split("\\(Electronic\\),")[1];
				    		 	 pissn=pissn.split("\\(Pri")[0];
						}
				    	  
					}
				  }
				  
				   vol=obj.get("PAVolume").trim();
				  num=obj.get("PAIssueCode").trim();
				  journal_name=obj.get("PublicationName").trim();
				 
				 String[] Publication=journal_name.split(":");
				 if (Publication.length>1) {
					 journal_name=Publication[1];					
				 }
				 else {
					 journal_name=Publication[0];		
				}
				 provider_url = "https://psycnet.apa.org/record/" + rawid;
				 

 
            if (!pub_date.equals("")) {
            	pub_year = pub_date.substring(0, 4).trim();
			}
            
//           rawid = doi;
            if (rawid.equals("")) {
            	context.getCounter("map", "error rawid").increment(1);
            	return;
			}
           lngid = VipIdEncode.getLngid(sub_db_id,rawid,false);
        
			XXXXObject xObj = new XXXXObject();

			xObj.data.put("author",author);
			xObj.data.put("author_1st",author_1st);
			xObj.data.put("organ",organ);
			xObj.data.put("organ_1st",organ_1st);
			xObj.data.put("title",title);
			xObj.data.put("keyword",keyword);
			xObj.data.put("pub_year",pub_year);
			xObj.data.put("vol",vol);
			xObj.data.put("num",num);
			xObj.data.put("journal_name",journal_name);
			xObj.data.put("raw_type",raw_type);
			xObj.data.put("doi",doi);
			xObj.data.put("page_info",page_info);
			xObj.data.put("begin_page",begin_page);
			xObj.data.put("end_page",end_page);
			xObj.data.put("eissn",eissn);
			xObj.data.put("pissn",pissn);
			xObj.data.put("recv_date",recv_date);
			xObj.data.put("accept_date",accept_date);
			xObj.data.put("pub_date",pub_date);
			xObj.data.put("publisher",publisher);
			xObj.data.put("abstract",abstract_);

			xObj.data.put("lngid",lngid);
			xObj.data.put("rawid",rawid);
			xObj.data.put("product",product);
			xObj.data.put("sub_db",sub_db);
			xObj.data.put("provider",provider);
			xObj.data.put("sub_db_id",sub_db_id);
			xObj.data.put("source_type",source_type);
			xObj.data.put("provider_url",provider_url);
			xObj.data.put("country",country);
			xObj.data.put("language",language);
			xObj.data.put("journal_raw_id",journal_raw_id);
			xObj.data.put("down_date",down_date);
			xObj.data.put("batch",batch);
			
			

			context.getCounter("map", "count").increment(1);

			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
            
	    }

	}
public static  String months(String data){//创建一个月份解释函数，增加去定义它的功能
	Map<String,String> months = new HashMap<String,String>();
	String sum = "";
	String dats = data;
	months.put("Jan","01");
	months.put("Feb","02");
	months.put("Mar","03");
	months.put("Apr","04");
	months.put("May","05");
	months.put("Jun","06");
	months.put("Jul","07");
	months.put("Aug","08");
	months.put("Sep","09");
	months.put("Oct","10");
	months.put("Nov","11");
	months.put("Dec","12");
	sum=months.get(dats);
//	System.out.println(months);
//	System.out.println(sum);
 return sum;

}

}