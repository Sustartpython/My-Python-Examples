package simple.jobstream.mapreduce.site.cniprpatent;

import java.io.BufferedWriter;
import java.io.IOException;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Arrays;
import java.util.Date;
import java.util.Dictionary;
import java.util.HashMap;
import java.util.Hashtable;
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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;
import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.util.StringHelper;
import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//这个是解析下载的网页的  另一个解析是解析江苏所过来的数据
public class sipoJson2XXXXObjectPub extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 20;
	private static int reduceNum = 20;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
	public static String batch = "";
  
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
			Type type = new TypeToken<Map<String,Object>>(){}.getType();

			Map<String, Object> mapField = gson.fromJson(text, type);			
			
			if (mapField.get("html") == null) {
				context.getCounter("map", "html is null").increment(1);
				return;
			}
			String html = mapField.get("html").toString();
			
	    	if (html.length() < 10) {
	    		 return;
	    	}
	    	
			String teString = html.replace("\0", " ").replace("\r", " ").replace("\n", " ") + "\n";
			html = teString;
			
	    	Document doc = Jsoup.parse(html);
		
			//国家
			String country="";
			//语言
			String language="";
			country = getCountrybyString("中国");
			language = getLanguagebyCountry(country);
	    	
	        // 每个页面包含多条数据
	        Elements boxElements = doc.select("div.cp_box");
	        for (Element box:boxElements) {
	            String url = "http://epub.sipo.gov.cn/tdcdesc.action?strWhere=";
	            //标题
	            String title = "";
	            //申请公布号
	            String identifier_standard = "";
	            //申请公布日
	            String date_impl = "";
	            //申请号
	            String identifier_pissn = "";
	            //申请日
	            String date_created = "";
	            //申请人
	            String creator_cluster = "";
	            //发明人
	            String creator = "";
	            //地址
                String creator_institution = "";
                //主分类号
                String subject_csc = "";
                //分类号
                String subject_isc = "";
                //专利代理机构
                String agency = "";
                //代理人
                String agents = "";
                //PCT进入国家阶段日
                String pct_date = "";
                //PCT申请数据
                String pct_data = "";
                //PCT公布数据
                String pct_data_impl = "";
                //图片地址
                String imgurl = "";
                //摘要
                String abstracts = "";
                //专利优先权
                String priority_number="";
                //图片地址
                String cover_path = "";
                //专利类型
				String raw_type = "";
				//优先权日
				String priority_date = "";
				//优先权号
				String priority_no = "";
                
	            Elements linElements = box.select("div.cp_linr");
	            if (!linElements.isEmpty()) {
	                //title
	                Elements h1 = linElements.first().select("> h1");
	                //标题
	                title = h1.first().text().trim();
	                if ("".equals(title)) {
	                    throw new InterruptedException("title is null");
	                }
	                title = title.replace("[发明公布]","").trim();
	                

	                Elements ulElements = linElements.first().select("> ul");
	                if (!ulElements.isEmpty()){
	                    Elements liElements = ulElements.first().select("li.wl228");
	                    for (Element liElement:liElements){
	                        if(liElement.text().trim().contains("申请公布号：")){
	                        	//申请公布号
	                            identifier_standard = liElement.text().trim().replace("申请公布号：","").trim();
	                        }
	                        if(liElement.text().trim().contains("申请公布日：")){
	                        	//申请公布日
	                            date_impl = liElement.text().trim().replace("申请公布日：","").trim();
	                        }
	                        if(liElement.text().trim().contains("申请号：")){
	                        	//申请号
	                            identifier_pissn = liElement.text().trim().replace("申请号：","").trim();
	                        }
	                        if(liElement.text().trim().contains("申请日：")){
	                        	//申请日
	                            date_created = liElement.text().trim().replace("申请日：","").trim();
	                        }
	                        if(liElement.text().trim().contains("申请人：")){
	                        	//申请人
	                            creator_cluster = liElement.text().trim().replace("申请人：","").trim();
	                            creator_cluster =creator_cluster.replaceAll(";[\\s\\p{Zs}]+", ";");
	                        }
	                        if(liElement.text().trim().contains("发明人：")){
	                        	//发明人
	                            creator = liElement.text().trim().replace("发明人：","").trim();
	                            creator =creator.replaceAll(";[\\s\\p{Zs}]+", ";");
	                        }

	                    }
	                    Elements li8Elements = ulElements.first().select("> li:nth-child(8)");
	                    if(li8Elements.first().text().trim().contains("地址：")){
	                    	//地址
	                        creator_institution = li8Elements.first().text().trim().replace("地址：","").trim();
	                    }else{
	                        throw new InterruptedException("dizhi is null");
	                    }
	                    Elements li9Elements = ulElements.first().select("> li:nth-child(9)");
	                    if(li9Elements.first().text().trim().contains("分类号：")){
	                        if (li9Elements.first().text().trim().contains("全部")){
	                            String subject_sc = li9Elements.first().text().trim().split("全部")[0].replace("分类号：","").trim();
	                            //主分类号
	                            subject_csc = subject_sc.split(";")[0].trim();
	                            //分类号
	                            subject_isc = subject_sc;
	                            subject_isc =subject_isc.replaceAll(";[\\s\\p{Zs}]+", ";");
	                       
	                            Elements ul2Elements = li9Elements.first().select("> div > ul");
	                            Elements li2Elements = ul2Elements.first().select("li");
	                            if(!li2Elements.first().text().trim().contains("专利代理机构")){
	                            	String subject_isc2 = li2Elements.first().text().trim();
	                                //分类号
	                                subject_isc = subject_isc + subject_isc2;
	                                subject_isc =subject_isc.replaceAll(";[\\s\\p{Zs}]+", ";");
	                            }
                                for (Element li2Element:li2Elements){
                                    if(li2Element.text().trim().contains("专利代理机构：")){
                                    	//专利代理机构
                                        agency = li2Element.text().trim().replace("专利代理机构：","").trim();
                                    }
                                    if(li2Element.text().trim().contains("代理人：")){
                                    	//代理人
                                        agents = li2Element.text().trim().replace("代理人：","").trim();
                                        agents =agents.replaceAll(";[\\s\\p{Zs}]+", ";");
                                    }
                                    if(li2Element.text().trim().contains("优先权：")){
                                    	//代理人
                                    	priority_number = li2Element.text().trim().replace("优先权：","").trim();
                                    }
                                    if(li2Element.text().trim().contains("PCT进入国家阶段日：")){
                                    	//PCT进入国家阶段日
                                        pct_date = li2Element.text().trim().replace("PCT进入国家阶段日：","").trim();
                                    }
                                    if(li2Element.text().trim().contains("PCT申请数据：")){
                                    	//PCT申请数据
                                        pct_data = li2Element.text().trim().replace("PCT申请数据：","").trim();
                                    }
                                    if(li2Element.text().trim().contains("PCT公布数据：")){
                                    	//PCT公布数据
                                        pct_data_impl = li2Element.text().trim().replace("PCT公布数据：","").trim();
                                    }
                                }
	                        }else{
	                            // 表示只有分类号 没有其他信息
	                            String subject_sc = li9Elements.first().text().trim().replace("分类号：","").trim();
	                            //主分类号
	                            subject_csc = subject_sc.split(";")[0].trim();
	                            //分类号
	                            subject_isc = subject_sc;
	                        }

	                    }else{
	                        throw new InterruptedException("fenneihao is null");
	                    }


	                }else{
	                    throw new InterruptedException("ul is null");
	                }
	                //img
	                Elements imgdivElements = box.select("div.cp_img");
	                Elements imgElements = imgdivElements.first().select("> img");
	                //图片地址
	                imgurl = imgElements.first().attr("src");
	                //摘要
	                Elements absdivElements = box.select("div.cp_jsh");
	                if (!absdivElements.isEmpty()) {
	                	//摘要
	                    abstracts = absdivElements.first().text().trim();
	                    abstracts = abstracts.replace("摘要：","").trim();
	                }
	               

	    			subject_isc =subject_isc.replaceAll(";[\\s\\p{Zs}]+", ";");
	    			creator_cluster =creator_cluster.replaceAll(";[\\s\\p{Zs}]+", ";");
	    			creator =creator.replaceAll(";[\\s\\p{Zs}]+", ";");
	    			
	    			{
	    				//处理数据
	    				//对 专利申请号 去除CN 去除点 方便获取类型
	    				String typedeal = identifier_pissn.replace("CN", "").replace(".","").trim();
	    				int length = typedeal.length();
	    				if (length == 9 || length == 8) {
	    					raw_type = String.valueOf(typedeal.charAt(2));
	    				}else if (length == 13 || length == 12) {
	    					raw_type = String.valueOf(typedeal.charAt(4));
						}else {
							throw new InterruptedException("申请号长度不对  请检查"+typedeal+":"+identifier_pissn);
						}
	    				
	    				//处理申请号保持结构一致
	       				length = typedeal.length();
	       				if (length == 8) {
	       					String modes = get_check_bit(typedeal);
	       					typedeal = typedeal+modes;
	       					StringBuffer sbs = new StringBuffer();
	       					identifier_pissn = "CN"+sbs.append(typedeal).insert(8,".").toString();
	       				}else if (length == 9) {
	       					StringBuffer sbs = new StringBuffer();
	       					identifier_pissn = "CN"+sbs.append(typedeal).insert(8,".").toString();
	    				}else if (length == 12) {
	    					String modes = get_check_bit(typedeal);
	       					typedeal = typedeal+modes;
	       					StringBuffer sbs = new StringBuffer();
	       					identifier_pissn = "CN"+sbs.append(typedeal).insert(12,".").toString();
	    				}else if (length == 13) {
	    			        StringBuffer sbs = new StringBuffer();
	    			        identifier_pissn = "CN"+sbs.append(typedeal).insert(12,".").toString();
						}else {
							throw new InterruptedException("申请号长度不对  请检查"+typedeal+":"+identifier_pissn);
						}
	    				
	    				//优先权项的处理
	    				//第一步有可能有多个  按照;进行分割
	    				if (!priority_number.trim().equals("")) {
		    				String[] sourceStrArray = priority_number.split(";");
	    				    for (int i = 0; i < sourceStrArray.length; i++) {
	    				    	String[] sourcesubArray = sourceStrArray[i].trim().split(" ");
	    				    	String countrys = "";
	    				    	String priority_no_temp = "";
	    				    	for (int j = 0; j < sourcesubArray.length; j++) {
	    				    		//日期的遍历
	    				    		String temp = sourcesubArray[j].replace(".", "").trim();
	    				    		if (temp.length() == 8 && StringHelper.isNumeric(temp) && DateTimeHelper.checkDateByRange(temp,10000000,30000000)) {
	    				    			priority_date =  priority_date + temp +";";
	    				    		}else if (sourcesubArray[j].trim().length() == 2 && CountryList.contains(sourcesubArray[j].trim())) {
										countrys = sourcesubArray[j].trim();
									}else {
										priority_no_temp = sourcesubArray[j].trim();
									}
	    				    	}
	    				    	priority_no = priority_no + countrys+" "+priority_no_temp+";";
	    				    	priority_no = priority_no.trim();
	    			        }
	    				}
	    			}
	    			
	    			
				    priority_no = StringHelper.cleanSemicolon(priority_no);
				    priority_date = StringHelper.cleanSemicolon(priority_date);
				    
				    String sub_db_id = "00029";
					String rawid = identifier_standard;
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
	                
	                xObj.data.put("app_no", identifier_pissn);
	    			xObj.data.put("app_date", date_created);
	    			xObj.data.put("pub_no", identifier_standard);
	    			xObj.data.put("pub_date", date_impl);
	    			xObj.data.put("ipc_no_1st", subject_csc);
	    			xObj.data.put("ipc_no", subject_isc);
	    			xObj.data.put("title", title);
	    			xObj.data.put("applicant", creator_cluster);
	    			xObj.data.put("author", creator);
	    			xObj.data.put("applicant_addr", creator_institution);
	    			//国省代码
	    			xObj.data.put("organ_area", "");
	    			xObj.data.put("agency", agency);
	    			xObj.data.put("agent", agents);
	    			//优先权
	    			xObj.data.put("priority", priority_number);
	    			//优先权号
	    			xObj.data.put("priority_no", priority_no);
	    			//优先权日
	    			xObj.data.put("priority_date", priority_date);
	    			//进入国家阶段日
	    			xObj.data.put("pct_enter_nation_date", pct_date);
	    			//申请数据
	    			xObj.data.put("pct_app_data", pct_data);
	    			//公布数据
	    			xObj.data.put("pct_pub_data", pct_data_impl);
	    			//分案原申请号(我们的表中没有)
	    			xObj.data.put("old_app_no", "");
	    			//法律状态
	    			xObj.data.put("legal_status", "");
	    			//引证文献(没有 但是保留在hadoop平台)
	    			xObj.data.put("Citing_literature", "");
	    			xObj.data.put("abstract", abstracts);
	    			//关键词没有
	    			xObj.data.put("keyword", "");
	    			//专利类型
	    			xObj.data.put("raw_type", raw_type);
	    			//主权项 没有
	    			xObj.data.put("claim", "");
	    			xObj.data.put("country", country);
	    			xObj.data.put("language", language);
	    			xObj.data.put("cover_path", cover_path);
	    			xObj.data.put("coverurl", imgurl);
	    			xObj.data.put("batch", batch);
	    			xObj.data.put("down_date", "20181129");
	                
	    			context.getCounter("map", "count").increment(1);
	    			
	    			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
	    			context.write(new Text(identifier_standard), new BytesWritable(bytes));	

	            }else {
	                throw new InterruptedException("lin is null");
	            }
	        }
	                				
					
	    }				
	}
}
