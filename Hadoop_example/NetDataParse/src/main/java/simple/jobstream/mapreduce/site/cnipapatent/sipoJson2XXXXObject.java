package simple.jobstream.mapreduce.site.cnipapatent;

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
import java.util.Set;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
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


public class sipoJson2XXXXObject extends InHdfsOutHdfsJobInfo {
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
    	public static String batch = "";
		
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
		
		// 获取 json 值，可能为一个或多个或没有此键
		  String getJsonValue(JsonObject articleJsonObject, String jsonKey) {
		   String line = "";

		   JsonElement jsonValueElement = articleJsonObject.get(jsonKey);
		   if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
		    line = "";
		   } else if (jsonValueElement.isJsonArray()) {
		    for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
		     line += jEle.getAsString().trim() + ";";
		    }
		   } else {
		    line = jsonValueElement.getAsString().trim();
		   }
		   line = line.replaceAll(";$", ""); // 去掉末尾的分号
		   line = line.trim();

		   return line;
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
		
		protected void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
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
	        //摘要
	        String abstracts = "";
	        //关键词
	        String keyword = "";
	        //专利类型
	        String raw_type = "";
	        String raw_type1= "";
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
	        //rawid
	        String rawid = "";

	        String down_date = "";
	        //同族
	        String family_pub_no = "";
	        
	        String ref_id = "";
	        String ref_cnt = "0";
	        String cited_id = "";
	        String cited_cnt = "0";
	        

	        String sub_db_id = "00027";
	        String product = "CNIPA";
	        String sub_db = "ZL";
	        String provider = "CNIPA";
	        
	        String source_type = "7";




	        rawid = mapField.get("requestsid").getAsString();
	        down_date = mapField.get("downdate").getAsString().substring(0,8);
	        String abs = mapField.get("abs").getAsString();
	        String msgstring = mapField.get("msg").getAsString();
	        Map<String, JsonElement> jsonmsg = gson.fromJson(msgstring, type);
	        //


//	        System.out.println(abs);
	        // 摘要请求下来的json
	        Map<String, JsonElement> absmapField = gson.fromJson(abs, type);
//	        System.out.println(absmapField);
	        JsonObject abstractinfodto = absmapField.get("abstractInfoDTO").getAsJsonObject();
	        // 获取摘要
	        String abshtml = abstractinfodto.get("abIndexList").getAsJsonArray().get(0).getAsJsonObject().get("value").getAsString();
	        abstracts = Jsoup.parse(abshtml).text();
	        abstracts = abstracts.replace("\\n","").replace("  ","").replace("\"", "");
	        //获取一个json数组
	        JsonArray arrayObj = abstractinfodto.get("abstractItemList").getAsJsonArray();
	        for (int i=0;i<arrayObj.size();i++) {
	            JsonObject jsonObj = arrayObj.get(i).getAsJsonObject();
	            if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Application No. ")) {
	                //这里的app_no无校验位
	                app_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Application Date")) {
	                app_date = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Publication No. ")) {
	                pub_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Publication Date")) {
	                pub_date = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("IPC Classification No.")) {
	                ipc_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Applicant/ Assignee")) {
	                applicant = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Inventor")) {
	                author = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Priority No.")) {
	                priority_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Priority Date")) {
	                priority_date = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Address of the Applicant")) {
	                applicant_addr = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("Zip Code of the Applicant")) {
	                postcode = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            } else if (jsonObj.get("indexEnName").toString().replace("\"", "").equals("CPC Classification No.")) {
	                cpc_no = jsonObj.get("value").toString().replace("\"", "").replaceAll(";$", "");
	                ;
	            }
	        }
	        ipc_no.replaceAll("\\s?;\\s+", "");
	        cpc_no_1st = cpc_no.split(";")[0];
	        ipc_no_1st = ipc_no.split(";")[0];
	        if (!priority_date.equals("") || !priority_no.equals("")) {
	            priority = priority_date +" "+ priority_no;
	        }

	        String[] postcodearray=postcode.split(";");
	        if (postcodearray.length > 1){
	            String[] applicantarray = applicant_addr.split(";");
	            applicant_addr = "";
	            for (int ii=0;ii<postcodearray.length;ii++) {
	                applicant_addr += postcodearray[ii].trim()+" "+ applicantarray[ii].trim()+";";
	            }
	        }else {
	            applicant_addr = postcode.trim()+" "+applicant_addr.trim();
	        }
	        applicant_addr = StringHelper.cleanSemicolon(applicant_addr);

	        //无验证为的app_no
	        String nrdAn = abstractinfodto.get("nrdAn").getAsString();
	        //pub_no
	        String pn = abstractinfodto.get("pn").getAsString();
	        //名称
	        title = abstractinfodto.get("tioIndex").getAsJsonObject().get("value").getAsString();
	        
//	        String get_one_char = app_no.replace(app_no.substring(0,2), "");
//	        if (get_one_char.length()==14 || get_one_char.length()==10){
//	            if (get_one_char.indexOf(".") > -1){
//
//	            }else{
//	                throw new InterruptedException("申请号长度不对  请检查"+get_one_char+":"+get_one_char.length());
//	            }
//	        }else{
//	            if (get_check_bit(get_one_char) == null) {
//	                throw new InterruptedException("申请号长度不对  请检查"+get_one_char+":"+get_one_char.length());
//	            }
//	            app_no = app_no+"."+get_check_bit(get_one_char);
//	        }

	        //处理数据
	        //对 专利申请号 去除CN 去除点 方便获取类型
	        if (app_no.substring(0,2).equals("CN")) {
		        String typedeal = app_no.replace(app_no.substring(0,2), "").replace(".","").trim();
		        int length = typedeal.length();
		        if (length == 9 || length == 8) {
		            raw_type = String.valueOf(typedeal.charAt(2));
		        }else if (length == 13 || length == 12) {
		            raw_type = String.valueOf(typedeal.charAt(4));
		        }else {
		            throw new InterruptedException("申请号长度不对  请检查"+typedeal+":"+app_no+":"+length);
		        }
	        }else {
	        	raw_type="";
	        }
	        //专利大类型
	        raw_type1 = jsonmsg.get("patentType").getAsString();
	        //代理人
	        organ_area = jsonmsg.get("fieldMap").getAsJsonObject().get("AC").getAsString().replaceAll(";$", "");
	        //代理人
	        agent = jsonmsg.get("fieldMap").getAsJsonObject().get("AGT").getAsString().replaceAll(";$", "");
	        //代理机构
	        agency = jsonmsg.get("fieldMap").getAsJsonObject().get("AGY").getAsString().replaceAll(";$", "");

	        if (pub_no.equals("CNC")) {
	            return;
	        }

	        //全文
	        String fulltext = "";
	        try {
	        fulltext = mapField.get("fulltxt").getAsString();
	        if (fulltext.equals("-1") || fulltext.equals("") || fulltext.indexOf("IsAjaxAndJsonData")> -1){
	        	fulltext = "";
	        }else {
	        	Map<String, JsonElement> fullmapField = gson.fromJson(fulltext, type);
		        JsonElement fulltexts = fullmapField.get("fullTextDTO").getAsJsonObject().get("literaInfohtml");
		        if (fulltexts == null || fulltexts.isJsonNull()){
		            fulltext = "";
		        }else{
		            fulltext = fulltexts.getAsString();
		        }
		        fulltext = Jsoup.parse(fulltext).text();
	        }
	        }catch (Exception e) {
				// TODO: handle exception
	        	throw new InterruptedException("全文 请检查"+rawid+";"+fulltext);
			}
	        

	        //法律状态等其他信息
	        ref_cnt = mapField.get("pcount").getAsString();
	        cited_cnt = mapField.get("cpnum").getAsString();
	        
	        String patentinfo = mapField.get("patentinfo").getAsString();
	        Map<String, JsonElement> infomapField = gson.fromJson(patentinfo, type);
	        JsonArray legal_status_array = infomapField.get("lawStateList").getAsJsonArray();
	        //法律状态
	        for (int i=0;i<legal_status_array.size();i++) {
	            String legaldate = legal_status_array.get(i).getAsJsonObject().get("prsDate").getAsString();
	            JsonElement legalstatuss = legal_status_array.get(i).getAsJsonObject().get("lawStateCNMeaning");
	            String legalstatus="";
				if (legalstatuss.isJsonNull() || legalstatuss.getAsString().equals("null")) {
					legalstatus = "";
	            }else {
	            	legalstatus = legalstatuss.getAsString();
	            }
				legal_status += legaldate+":"+legalstatus+";";
	        }
	        //判断是不是有翻页的数据
	        if (mapField.get("lawmsg").getAsString().equals("")) {
	            legal_status = StringHelper.cleanSemicolon(legal_status);
	        } else {
	            String lawmsg = mapField.get("lawmsg").getAsString();
	            Map<String, JsonElement> lawmsgmapField = gson.fromJson(lawmsg, type);
	            Set<String> strings = lawmsgmapField.keySet();
	            for (String keys : strings) {
	                JsonElement legal_status_Ele = lawmsgmapField.get(keys).getAsJsonObject().get("lawStateList");
	                if (!legal_status_Ele.isJsonNull())
	                {
		                JsonArray legal_status_array1 = legal_status_Ele.getAsJsonArray();
		                for (int i = 0; i < legal_status_array1.size(); i++) {
	//	                    System.out.println(legal_status_array1.get(i).getAsJsonObject());
		                    String legaldate = legal_status_array1.get(i).getAsJsonObject().get("prsDate").getAsString();
		                    JsonElement legalstatuss = legal_status_array1.get(i).getAsJsonObject().get("lawStateCNMeaning");
		                    String legalstatus = "";
		                    if (legalstatuss.isJsonNull() || legalstatuss.getAsString().equals("null")) {
		    					legalstatus = "";
		    	            }else {
		    	            	legalstatus = legalstatuss.getAsString();
		    	            }
		                    legal_status += legaldate + ":" + legalstatus + ";";
		                }
	                }
	            }
	        }
	        legal_status = StringHelper.cleanSemicolon(legal_status);
	        
	        //同族
	        JsonElement cognationarraye = infomapField.get("cognationList");
	        if (cognationarraye == null || cognationarraye.isJsonNull()) {
	            family_pub_no = "";
	        }else {
	            JsonArray cognationarray = cognationarraye.getAsJsonArray();
	            for (int i=0;i<cognationarray.size();i++) {
	                String cognation = cognationarray.get(i).getAsJsonObject().get("pn").getAsString();
	                family_pub_no += cognation +";";
	            }
	            family_pub_no = StringHelper.cleanSemicolon(family_pub_no);
	        }
	        
	      //cite
            if (mapField.get("cpmsg").getAsString().equals("")) {
            	cited_id = StringHelper.cleanSemicolon(cited_id);
            } else {
                String cpmsg = mapField.get("cpmsg").getAsString();
                Map<String, JsonElement> pmsgmapField = gson.fromJson(cpmsg, type);
//                System.out.println(pmsgmapField);
                Set<String> strings = pmsgmapField.keySet();
                for (String keys : strings) {
                	JsonElement legal_status_Ele =pmsgmapField.get(keys).getAsJsonObject().get("citingpatList");
                	if (!legal_status_Ele.isJsonNull())
	                {
		                JsonArray legal_status_array1 = legal_status_Ele.getAsJsonArray();
                	//JsonArray legal_status_array1 = pmsgmapField.get(keys).getAsJsonObject().get("patcitList").getAsJsonArray();
                    for (int i = 0; i < legal_status_array1.size(); i++) {
                        String patcit = legal_status_array1.get(i).getAsJsonObject().get("pn").getAsString();
                        cited_id += patcit + ";";
                    }
	                }
                }
            }
            cited_id = StringHelper.cleanSemicolon(cited_id);
	        
	        //ref
	        JsonElement patcitarraye = infomapField.get("patcitList");
	        if (patcitarraye == null || patcitarraye.isJsonNull()) {
	        	ref_id = "";
	        }else {
	            JsonArray patcitarray = patcitarraye.getAsJsonArray();
	            for (int i=0;i<patcitarray.size();i++) {
		            String patcit = patcitarray.get(i).getAsJsonObject().get("pn").getAsString();
		            ref_id += patcit + ";";
		        }
	          //判断是不是有翻页的数据
	            if (mapField.get("pmsg").getAsString().equals("")) {
	            	ref_id = StringHelper.cleanSemicolon(ref_id);
	            } else {
	                String pmsg = mapField.get("pmsg").getAsString();
	                Map<String, JsonElement> pmsgmapField = gson.fromJson(pmsg, type);
//	                System.out.println(pmsgmapField);
	                Set<String> strings = pmsgmapField.keySet();
	                for (String keys : strings) {
	                	JsonElement legal_status_Ele =pmsgmapField.get(keys).getAsJsonObject().get("patcitList");
	                	if (!legal_status_Ele.isJsonNull())
		                {
			                JsonArray legal_status_array1 = legal_status_Ele.getAsJsonArray();
	                	//JsonArray legal_status_array1 = pmsgmapField.get(keys).getAsJsonObject().get("patcitList").getAsJsonArray();
	                    for (int i = 0; i < legal_status_array1.size(); i++) {
	                        String patcit = legal_status_array1.get(i).getAsJsonObject().get("pn").getAsString();
	                        ref_id += patcit + ";";
	                    }
		                }
	                }
	            }
	            ref_id = StringHelper.cleanSemicolon(ref_id);
	        }
	        
	        //获取语言
	        JsonArray langarray = abstractinfodto.get("simpleLiteraList").getAsJsonArray();
	        for (int i=0;i<langarray.size();i++) {
	        	String pn1 = langarray.get(i).getAsJsonObject().get("pn").getAsString();
	        	if (pn1.equals(pub_no)) {
	        		language = langarray.get(i).getAsJsonObject().get("lang").getAsString();
	        	}
	        }
	        
	        country = app_no.substring(0,2);
	        String lngid = VipIdEncode.getLngid(sub_db_id, rawid, false);
           
            
            XXXXObject xObj = new XXXXObject();
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
			xObj.data.put("ref_id", ref_id);
			xObj.data.put("ref_cnt", ref_cnt);
			xObj.data.put("cited_id", cited_id);
			xObj.data.put("cited_cnt", cited_cnt);
			xObj.data.put("abstracts", abstracts);
			xObj.data.put("keyword", keyword);
			xObj.data.put("raw_type", raw_type);
			xObj.data.put("raw_type1", raw_type1);
			xObj.data.put("claim", claim);
			xObj.data.put("country", country);
			xObj.data.put("language", language);
			xObj.data.put("cover_path", cover_path);
			xObj.data.put("batch", batch);
			xObj.data.put("down_date", down_date);
			xObj.data.put("family_pub_no", family_pub_no);
			xObj.data.put("fulltext", fulltext);
			xObj.data.put("sub_db_id", sub_db_id);
			xObj.data.put("product", product);
			xObj.data.put("sub_db", sub_db);
			xObj.data.put("provider", provider);
			xObj.data.put("source_type", source_type);
			xObj.data.put("rawid", rawid);
			xObj.data.put("postcode", postcode);
			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));		
	                				
					
	    }				
	}
	
}
