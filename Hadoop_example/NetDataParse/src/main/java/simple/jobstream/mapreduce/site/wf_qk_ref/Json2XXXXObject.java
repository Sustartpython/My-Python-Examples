package simple.jobstream.mapreduce.site.wf_qk_ref;

import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.InputStreamReader;
import java.io.OutputStreamWriter;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import javax.script.Invocable;
import javax.script.ScriptEngine;
import javax.script.ScriptEngineManager;
import javax.script.ScriptException;

import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FSDataOutputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Mapper.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;

import com.google.gson.Gson;
import com.google.gson.JsonArray;
import com.google.gson.JsonElement;
import com.google.gson.JsonObject;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 60;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "wf_qk_ref." + this.getClass().getSimpleName();
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
//		JobConfUtil.setTaskPerMapMemory(job, 3072);
//		JobConfUtil.setTaskPerReduceMemory(job, 5120);
		
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
		
		static ScriptEngineManager manager = null;   
		static ScriptEngine engine = null;     
		
		private static String rawsourceid;
		private static String refertext = "";
		private static String strtitle = "";
		private static String strtype = "";
		private static String strname = "";
		private static String strwriter1 = "";
		private static String stryearvolnum = "";
		private static String strpubwriter = "";
		private static String strpages = "";
		private static String doi = "";
		private static String disproof_id = "";		// 反证后的万方ID
		
		public void setup(Context context) throws IOException,
				InterruptedException {
		    // 获取HDFS文件系统  
		    FileSystem fs = FileSystem.get(context.getConfiguration());
		
		    FSDataInputStream fin = fs.open(new Path("/RawData/wanfang/qk/template/format_ref.js")); 
		    BufferedReader in = null;
		    String line = "";
		    String jsCode = "";
		    manager = new ScriptEngineManager();   
			engine = manager.getEngineByName("javascript");    
		    try {
		        in = new BufferedReader(new InputStreamReader(fin, "UTF-8"));
		        while ((line = in.readLine()) != null) {
		        	line = line.replaceAll("\\s+$", "");	//去掉右侧空白
		        	jsCode += line + "\n";		        	
		        }
		    } finally {
		        if (in!= null) {
		        	in.close();
		        }
		    }
		    try {
		    	System.out.println("*******************jsCode:\n" + jsCode);
				engine.eval(jsCode);
			} catch (ScriptException e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			} 
		}
		
		//清理的分号和空白
		static String cleanLastSemicolon(String text) {
			text = text.replace('；', ';');			//全角转半角
			text = text.replaceAll(";\\s+?", ";");	//去掉分号后的空白			
			text = text.replaceAll("[\\s;]+?$", "");	//去掉最后多余的空白和分号
			
			return text;
		}
		
		//获取strtitle
		private static String getStrTitle(Element itemElement) {
			String strtitle = "";
			//String liText = itemElement.text().trim();
			
			Element aElement = itemElement.select("a[href~=^(?!.*Paper).*$]").first();
			if (aElement != null) {
				strtitle = aElement.text().trim();
				return strtitle;
			}

			return strtitle;
		}
		
		//获取strtype
		private static String getStrType(Element itemElement) {
			String strtype = "";
			//String liText = itemElement.text().trim();
			
			Element aElement = itemElement.select("a[href~=^(?!.*Paper).*$]").first();
			if (aElement != null) {
				if (aElement.nextSibling() != null) {
					String line = Jsoup.parse(aElement.nextSibling().outerHtml()).text();
					Pattern pattern = Pattern.compile("\\[(.+?)\\]");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						strtype = matcher.group(1).trim();
					} 
				}				
			}
			
			return strtype;
		}
		
		//获取strname(刊名)
		private static String getStrName(Element itemElement, String strtype) {
			String strname = "";
			String liText = itemElement.text().trim();
			
			if (strtype.toUpperCase().equals("D")) {
				int idx = liText.indexOf("[D].");
				if (idx > 0) {
					strname = liText.substring(idx+4);
					strname = strname.replaceAll("[\\s\\d,\\.]+$", "").trim();	//去掉最后多余的空白、数字、逗号和点
				}
			}
			else {
				Element aElement = itemElement.select("a[href~=Periodical-.+aspx]").first();
				if (aElement != null) {
					strname = aElement.text().trim();
				}
				strname = strname.replaceAll("[\\s,.]+?$", "").trim();		//去掉尾部的空白、逗号、点。
			}
			
			return strname;
		}
		
		//作者
		private static String getWriter(Element itemElement) {
			String writer = "";
			String liText = itemElement.text().trim();
			
			for (Element aElement : itemElement.select("a[href*=Paper]")) {
				writer += aElement.text().trim() + ";";
			}
			writer = cleanLastSemicolon(writer);
			
			return writer;
		}
		
		//年卷期
		//内容中：年期+页码+doi，页码和doi都可能不存在
		private static String getYearNum(Element itemElement) {
			String stryearvolnum = "";
			//String liText = itemElement.text().trim();
			
			for (Node node : itemElement.childNodes()) {
				if (node.nodeName().equals("#text")) {
					String line = node.toString().trim();
					Pattern pattern = Pattern.compile("((19|20)[,\\d\\(\\)]+)");
					Matcher matcher = pattern.matcher(line);
					if (matcher.find()) {
						stryearvolnum = matcher.group(0).trim();
					} 
				}
			}
				
			return stryearvolnum;
		}
		
		//出版社
		private static String getStrPubWriter(Element itemElement) {
			String strpubwriter = "";
			String liText = itemElement.text().trim();

			return strpubwriter;
		}
		
		// 获取 json 值，可能为一个或多个或没有此键
		static String getJsonValue(JsonObject jsonObject, String jsonKey) {
			String line = "";
			
			JsonElement jsonValueElement = jsonObject.get(jsonKey);			
			if ((null == jsonValueElement) || jsonValueElement.isJsonNull()) {
				line = "";
			}					
			else if (jsonValueElement.isJsonArray()) {
				for (JsonElement jEle : jsonValueElement.getAsJsonArray()) {
					line += jEle.getAsString().trim() + ";";
				}
			}
			else {
				line = jsonValueElement.getAsString().trim();
			}
			line = line.replaceAll(";$", "").trim();		// 去掉末尾的分号
			
			return line;
		}
		
		static String getRefertext(String jsonText) {
			String html = "";
			String refertext = "";			
			
			Invocable invoke = (Invocable)engine; 
			try {
				html = (String)invoke.invokeFunction("joinRef", jsonText);
				refertext = Jsoup.parse(html).text().trim();
			} catch (Exception e) {
				// TODO Auto-generated catch block
				e.printStackTrace();
			}
			
			
			return refertext;
		}
		
		/*
		 * 
		 {
            "Tutor": null,
            "Issue": "",
            "Periodical": null,
            "Degree": null,
            "TitleInfo": null,
            "Orgnization": null,
            "Symbol": "Unknown",
            "Title": "激光光谱学:基本概念和仪器手段",
            "AuthorInfo": null,
            "PeriodicalCode": null,
            "Page": "29-40",
            "Publisher": "{H}北京:科学出版社",
            "Volumn": "",
            "SerialNum": 11,
            "School": null,
            "Type": "M",
            "Year": "1989",
            "HasFulltext": false,
            "ArticaleId": "wlxb201403012^11",
            "Author": "戴姆特瑞德W%严光耀%沈珊雄%夏慧荣",
            "DOI": null
        }
		 */
		//处理一个item，即一条引文
		public static void procOneItem(JsonElement refJsonElement) {
			{
				refertext = "";
				strtitle = "";
				strtype = "";
				strname = "";
				strwriter1 = "";
				stryearvolnum = "";
				strpubwriter = "";
				strpages = "";
				doi = "";
				disproof_id = "";
			}
			System.out.println(refJsonElement);
			
			JsonObject refJsonObject = refJsonElement.getAsJsonObject();
			
			String Tutor = getJsonValue(refJsonObject, "Tutor");
			String Issue = getJsonValue(refJsonObject, "Issue");
			String Periodical = getJsonValue(refJsonObject, "Periodical");
			String Degree = getJsonValue(refJsonObject, "Degree");
			String TitleInfo = getJsonValue(refJsonObject, "TitleInfo");
			String Orgnization = getJsonValue(refJsonObject, "Orgnization");
			String Symbol = getJsonValue(refJsonObject, "Symbol");
			String Title = getJsonValue(refJsonObject, "Title");
			String AuthorInfo = getJsonValue(refJsonObject, "AuthorInfo");
			String PeriodicalCode = getJsonValue(refJsonObject, "PeriodicalCode");
			String Page = getJsonValue(refJsonObject, "Page");
			String Publisher = getJsonValue(refJsonObject, "Publisher");
			String Volumn = getJsonValue(refJsonObject, "Volumn");
			String SerialNum = getJsonValue(refJsonObject, "SerialNum");
			String School = getJsonValue(refJsonObject, "School");
			String Type = getJsonValue(refJsonObject, "Type");
			String Year = getJsonValue(refJsonObject, "Year");
			String HasFulltext = getJsonValue(refJsonObject, "HasFulltext");
			String ArticaleId = getJsonValue(refJsonObject, "ArticaleId");
			String Author = getJsonValue(refJsonObject, "Author");
			String DOI = getJsonValue(refJsonObject, "DOI");

			
			strtitle = Title.trim();
			int idx = strtitle.indexOf('%');
			if (idx > 0) {
				strtitle = strtitle.substring(0, idx).trim();
			}
			strtype = Type.trim();
			strname = Periodical.trim();
			strwriter1 = Author.replace('%', ';');
			stryearvolnum = Year + "," + Volumn;
			if (Issue.length() > 0) {
				stryearvolnum += "(" + Issue + ")";
			}			
			strpubwriter = Publisher.trim();
			strpages = Page.trim();
			
			doi = DOI.trim();
			if (ArticaleId.indexOf('^') < 0) {
				disproof_id = ArticaleId;
			}
			
			if (strtype.equals("J")) {
				if ((strname.length() < 1) && (strpubwriter.length() > 0)) {
					strname = strpubwriter;
					strpubwriter = "";
				}
				refertext = Author.replace('%', ',') + "." + strtitle + "[" + strtype + "].";
				if (strname.length() > 0) {
					refertext += strname + ",";
				}				
				refertext +=  stryearvolnum;
				if (Page.length() > 0) {
					refertext += ":" + Page;
				}
				if (doi.length() > 0) {
					refertext += ".doi:" + doi;
				}				
				refertext += ".";
			}
			else {
				refertext = getRefertext(refJsonObject.toString()).trim();
			}
			
			
//			System.out.println("ArticaleId:" + ArticaleId);
//			System.out.println("rawsourceid:" + rawsourceid);
//			System.out.println("refertext:" + refertext);
//			System.out.println("strtitle:" + strtitle);
//			System.out.println("strtype:" + strtype);
//			System.out.println("strname:" + strname);
//			System.out.println("strwriter1:" + strwriter1);
//			System.out.println("stryearvolnum:" + stryearvolnum);
//			System.out.println("strpubwriter:" + strpubwriter);
//			System.out.println("strpages:" + strpages);
//			System.out.println("doi:" + doi);
//			System.out.println("disproof_id:" + disproof_id);
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
		        String pathfile = "/vipuser/walker/log/log_map/" + nowDate + ".txt";
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
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, JsonElement>>(){}.getType();
			Map<String, JsonElement> mapField = gson.fromJson(text, type);	
			
			rawsourceid = mapField.get("article_id").getAsString().trim();
			if (rawsourceid.length() < 1) {
				context.getCounter("map", "rawsourceid.length() < 1").increment(1);
				return;
			}
			
			JsonArray refJsonArray = mapField.get("ref").getAsJsonArray();
			
			for (JsonElement refJsonElement : refJsonArray) {			
				procOneItem(refJsonElement); 			
				
				if (refertext.length() < 1) {
					context.getCounter("map", "refertext.length() < 1").increment(1);
					log2HDFSForMapper(context, "refertext.length() < 1\n" + text);
					continue;
				}
				
				XXXXObject xObj = new XXXXObject();
				xObj.data.put("rawsourceid", rawsourceid);
				xObj.data.put("refertext", refertext);
				xObj.data.put("strtitle", strtitle);
				xObj.data.put("strtype", strtype);
				xObj.data.put("strname", strname);
				xObj.data.put("strwriter1", strwriter1);
				xObj.data.put("stryearvolnum", stryearvolnum);
				xObj.data.put("strpubwriter", strpubwriter);
				xObj.data.put("strpages", strpages);
				xObj.data.put("doi", doi);
				xObj.data.put("disproof_id", disproof_id);
				
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
			String lngID = "W_";
			for (int i = 0; i < rawsourceid.length(); i++) {
				lngID += String.format("%d", rawsourceid.charAt(i) + 0);
			}
			lngID = lngID + String.format("%04d", idx);
			
			return lngID;
		}		
		
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			/* 后面没有merge过程才能在这里编号
			String rawsourceid = key.toString();
			int idx = 0;
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
						
						String lngID = getLngIDByRawSourceID(rawsourceid, ++idx); 
						context.write(new Text(lngID), item);
					}
				}
			}
			*/

			Set<String> refertextSet = new HashSet<String>();				
			for (BytesWritable item : values) {
				XXXXObject xObj = new XXXXObject();
				VipcloudUtil.DeserializeObject(item.getBytes(), xObj);
				
				String refertext = "";
				for (Map.Entry<String, String> updateItem : xObj.data.entrySet()) {
					if (updateItem.getKey().equals("refertext")) {
						refertext = updateItem.getValue().trim();
						break;
					}
				}
				
				if (refertextSet.contains(refertext)) {	//新数据中排除重复引文
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
