package simple.jobstream.mapreduce.site.cniprpatent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.List;
import java.util.Map;
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
import org.jsoup.Jsoup;
import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//这个是解析下载的网页的  另一个解析是解析江苏所过来的数据
public class sipoJson2XXXXObjectPss extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
	public static String batch ="";
  
	public void pre(Job job)
	{
//		String jobName = this.getClass().getSimpleName();
//		if (testRun) {
//			jobName = "test_" + jobName;
//		}
//		job.setJobName("cnipr."+jobName);
		
		job.setJobName(job.getConfiguration().get("jobName"));
		inputHdfsPath = job.getConfiguration().get("inputHdfsPath");
		outputHdfsPath = job.getConfiguration().get("outputHdfsPath");
		batch = job.getConfiguration().get("batch");
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
		job.setReducerClass(UniqXXXXObjectReducer.class);
		
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
		static List<String> CountryList = Arrays.asList("AD", "AE", "AF", "AG", "AI", "AL", "AM", "AN", "AO", "AR", 
				"AT", "AU", "AW", "AZ", "BB", "BD", "BE", "BF", "BG", "BH", "BI", "BJ", "BM", "BN", "BO", "BR", 
				"BS", "BT", "BU", "BW", "BY", "BZ", "CA", "CF", "CG", "CH", "CI", "CL", "CM", "CN", "CO", "CR", 
				"CS", "CU", "CV", "CY", "DE", "DJ", "DK", "DM", "DO", "DZ", "EC", "EE", "EG", "EP", "ES", "ET",
				"FI", "FJ", "FK", "FR", "GA", "GB", "GD", "GE", "GH", "GI", "GM", "GN", "GQ", "GR", "GT", "GW",
				"GY", "HK", "HN", "HR", "HT", "HU", "HV", "ID", "IE", "IL", "IN", "IQ", "IR", "IS", "IT", "JE",
				"JM", "JO", "JP", "KE", "KG", "KH", "KI", "KM", "KN", "KP", "KR", "KW", "KY", "KZ", "LA", "LB",
				"LC", "LI", "LK", "LR", "LS", "LT", "LU", "LV", "LY", "MA", "MC", "MD", "MG", "ML", "MN", "MO",
				"MR", "MS", "MT", "MU", "MV", "MW", "MX", "MY", "MZ", "NA", "NE", "NG", "NH", "NI", "NL", "NO",
				"NP", "NR", "NZ", "OA", "OM", "PA", "PC", "PE", "PG", "PH", "PK", "PL", "PT", "PY", "QA", "RO",
				"RU", "RW", "SA", "SB", "SC", "SD", "SE", "SG", "SH", "SI", "SL", "SM", "SN", "SO", "SR", "ST",
				"SU", "SV", "SY", "SZ", "TD", "TG", "TH", "TJ", "TM", "TN", "TO", "TR", "TT", "TV", "TZ", "UA",
				"UG", "US", "UY", "UZ", "VA", "VC", "VE", "VG", "VN", "VU", "WO", "WS", "YD", "YE", "YU", "ZA",
				"ZM", "ZR", "ZW");
		
		
		
		//清理space，比如带大括号的情况（xiandaijj201204178:{G445}）
		static String cleanSpace(String text) {
			text = text.replaceAll("[\\s\\p{Zs}]+", " ").trim();		
			return text;
		}
		
		//处理申请号验证位问题
		static String get_check_bit(String typedeal) {
			String bb;
			if (typedeal.length()==8){
				bb = "23456789";
			}else if (typedeal.length()==12) {
				bb = "234567892345";
			} else {
				return null;
			}
			char[] ar = typedeal.toCharArray(); //char数组
			char[] br = bb.toCharArray(); //char数组
			int allnum = 0;
			for (int i=0; i<ar.length;i++){
				int mi = Integer.parseInt(String.valueOf(ar[i]));
				int mj = Integer.parseInt(String.valueOf(br[i]));
				allnum += mi*mj;
			}
			int mode = allnum % 11;
			String modes="";
			if (mode == 10) {
				modes = "X";
			}else{
				modes = String.valueOf(mode);
			}
			return modes;
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
	    	
	    	String text = value.toString().trim();
	    	
	    	 Gson gson = new Gson();
	         Type type = new TypeToken<Map<String, JsonElement>>() {}.getType();
	         Map<String, JsonElement> mapField = gson.fromJson(text, type);
			

            //申请号
            String 	app_no = "";
            //申请日
            String app_date  = "";
            //申请公布号
            String pub_no = "";
            //申请公布日
            String pub_date = "";
            //主分类号
            String ipc_no_1st = "";
            //分类号
            String ipc_no = "";
            //名称
            String title = "";
            //申请人
            String applicant  = "";
            //发明人
            String author = "";
            //地址
            String applicant_addr = "";
            //国省代码
            String organ_area = "";
            //专利代理机构
            String agency = "";
            //代理人
            String agent = "";
            //优先权
            String priority = "";
            //优先权号
            String priority_no = "";
            //优先权日
            String priority_date = "";
            //PCT进入国家阶段日
            String pct_enter_nation_date = "";
            //PCT申请数据
            String pct_app_data = "";
            //PCT公布数据
            String pct_pub_data = "";
            //分案原申请号(我们的表中没有)
            String old_app_no = "";
            //法律状态
            String legal_status = "";
            //引证文献(没有 但是保留在hadoop平台)
            String Citing_literature = "";
            //摘要
            String abstracts = "";
            //关键词
            String keyword = "";
            //专利类型
            String raw_type = "";
            //主权项
            String claim = "";
			//国家
			String country="CN";
			//语言
			String language="ZH";
            //图片地址
            String cover_path = "";
            //邮编
            String postcode = "";
            //cpc_no
            String cpc_no = "";
            //cpc_no_1st
            String cpc_no_1st = "";
            
            
            
            //最上层json
            JsonObject absDTO = mapField.get("detail").getAsJsonObject().get("abstractInfoDTO").getAsJsonObject();
            // 这是搜索时列表页显示的数据 代理人在其中而没有在详情页
            JsonObject jsonmsg = mapField.get("jsonmsg").getAsJsonObject();
            JsonArray abIdex = absDTO.get("abIndexList").getAsJsonArray();

            JsonObject idexobj = abIdex.get(0).getAsJsonObject();
            // 获取摘要
            String html = idexobj.get("value").toString();
            abstracts = Jsoup.parse(html).text();
            abstracts = abstracts.replace("\\n","").replace("  ","").replace("\"", "");
            //获取一个json数组
            JsonArray arrayObj = absDTO.get("abstractItemList").getAsJsonArray();
            for (int i=0;i<arrayObj.size();i++){
                JsonObject jsonObj = arrayObj.get(i).getAsJsonObject();
                if (jsonObj.get("indexEnName").toString().replace("\"","").equals("Application No. ")){
                	//这里的app_no无校验位
                    app_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Application Date")){
                    app_date = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Publication No. ")){
                    pub_no = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Publication Date")){
                    pub_date = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("IPC Classification No.")){
                    ipc_no = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Applicant\\/ Assignee")){
                    applicant = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Inventor")){
                    author = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Priority No.")){
                    priority_no = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Priority Date")){
                    priority_date = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Address of the Applicant")){
                    applicant_addr = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("Zip Code of the Applicant")){
                    postcode = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }else if(jsonObj.get("indexEnName").toString().replace("\"","").equals("CPC Classification No.")){
                    cpc_no = jsonObj.get("value").toString().replace("\"","").replaceAll(";$", ""); ;
                }

            }
            ipc_no.replaceAll("\\s?;\\s+", "");
            cpc_no_1st = cpc_no.split(";")[0];
            ipc_no_1st = ipc_no.split(";")[0];
            if (!priority_date.equals("") || !priority_no.equals("")) {
                priority = priority_date +" CN "+ priority_no;
            }
            
            
            String[] postcodearray=postcode.split(";");
            if (postcodearray.length > 1){
            	String[] applicantarray = applicant_addr.split(";");
            	applicant_addr = "";
            	for (int i=0;i<postcodearray.length;i++) {
            		applicant_addr += postcodearray[i].trim()+" "+ applicantarray[i].trim()+";";
            	}
            }else {
                applicant_addr = postcode.trim()+" "+applicant_addr.trim();
            }
        	applicant_addr = StringHelper.cleanSemicolon(applicant_addr);
 
            
            String figureRid = "";
            //下载图片需要该id

            figureRid = absDTO.get("figureRid").toString();

            //申请号与日期的组合 请求该数据的重要参数
            String id = absDTO.get("id").getAsString();
            //无验证为的app_no
            String nrdAn = absDTO.get("nrdAn").getAsString();
            //pub_no
            String pn = absDTO.get("pn").getAsString();
            //名称
            title = absDTO.get("tioIndex").getAsJsonObject().get("value").getAsString();
            String get_one_char = app_no.replace("CN", "");
            if (get_check_bit(get_one_char) == null) {
            	throw new InterruptedException("申请号长度不对  请检查"+get_one_char+":"+get_one_char.length());
            }
            app_no = app_no+"."+get_check_bit(get_one_char);
            
          //处理数据
			//对 专利申请号 去除CN 去除点 方便获取类型
			String typedeal = app_no.replace("CN", "").replace(".","").trim();
			int length = typedeal.length();
			if (length == 9 || length == 8) {
				raw_type = String.valueOf(typedeal.charAt(2));
			}else if (length == 13 || length == 12) {
				raw_type = String.valueOf(typedeal.charAt(4));
			}else {
				throw new InterruptedException("申请号长度不对  请检查"+typedeal+":"+app_no+":"+length);
			}
			
			//代理人
			agent = jsonmsg.get("AGT").getAsString().replaceAll(";$", "");
			//代理机构
			agency = jsonmsg.get("AGY").getAsString().replaceAll(";$", "");
			
			if (pub_no.equals("CNC")) {
				return;
			}
           
		    String sub_db_id = "00029";
			String rawid = pub_no;
		    String product = "CNIPR";
		    String sub_db = "ZL";
		    String provider = "CNIPR";
		    String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
		    
            XXXXObject xObj = new XXXXObject();	
            xObj.data.put("sub_db_id", sub_db_id);
            xObj.data.put("rawid", rawid);
            xObj.data.put("product", product);
            xObj.data.put("sub_db", sub_db);
            xObj.data.put("provider", provider);
            xObj.data.put("lngid", lngid);
            
			xObj.data.put("app_no", app_no);
			xObj.data.put("app_date", app_date);
			xObj.data.put("pub_no", pub_no);
			xObj.data.put("pub_date", pub_date);
			xObj.data.put("ipc_no_1st", ipc_no_1st);
			xObj.data.put("ipc_no", ipc_no);
			xObj.data.put("cpc_no_1st", cpc_no_1st);
			xObj.data.put("cpc_no", cpc_no);
			xObj.data.put("title", title);
			xObj.data.put("applicant", applicant);
			xObj.data.put("author", author);
			xObj.data.put("applicant_addr", applicant_addr);
			xObj.data.put("organ_area", organ_area);
			xObj.data.put("agency", agency);
			xObj.data.put("agent", agent);
			xObj.data.put("priority", priority);
			xObj.data.put("priority_no", priority_no);
			xObj.data.put("priority_date", priority_date);
			xObj.data.put("pct_enter_nation_date", pct_enter_nation_date);
			xObj.data.put("pct_app_data", pct_app_data);
			xObj.data.put("pct_pub_data", pct_pub_data);
			xObj.data.put("old_app_no", old_app_no);
			xObj.data.put("legal_status", legal_status);
			xObj.data.put("Citing_literature", Citing_literature);
			xObj.data.put("abstract", abstracts);
			xObj.data.put("keyword", keyword);
			xObj.data.put("raw_type", raw_type);
			xObj.data.put("claim", claim);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("cover_path", cover_path);
			xObj.data.put("coverurl", figureRid);
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", "20181204");
			
			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(pub_no), new BytesWritable(bytes));		
	                				
					
	    }				
	}
}
