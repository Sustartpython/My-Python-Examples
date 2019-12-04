package simple.jobstream.mapreduce.site.duxiuBook;

import java.io.IOException;
import java.lang.reflect.Type;
import java.text.DateFormat;
import java.text.SimpleDateFormat;
import java.util.Date;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

import simple.jobstream.mapreduce.common.vip.UniqXXXXObjectReducer;
import simple.jobstream.mapreduce.common.vip.VipIdEncode;

//统计wos和ei的数据量
public class Json2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
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

	public void SetMRInfo(Job job) {
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
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

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		public String batch = "";
		
		public void setup(Context context) throws IOException, InterruptedException {
			batch = context.getConfiguration().get("batch");
		}
		
		public static String getKeyword(String text) {
			Map<String, String> mapkeyword =new HashMap<String, String>();
			text = text.replace("（", "(").replace("）", ")").replace("：", ":");
			text = text.replace("；", ";").replace("--", ";").replace("－", ";");
			text = text.replace(" ～", "～");
			if (text.contains("(")) {
				Matcher matcher = Pattern.compile("\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)").matcher(text);
				text = text.replaceAll("\\([^()]*(?:\\([^()]*\\)[^()]*)*\\)", ";");
				text = text.replace(";，", ",").replace("，;", ",");
				text = text.replace("(", ";").replace(")", ";");
				while (matcher.find()) {
					String insideString = matcher.group();
					insideString = insideString.replaceAll("^\\(|\\)$", "").replaceAll("\\(|\\)", ";").replaceAll(": ;", ":");
					insideString = insideString.replaceAll(": ", ":").replaceAll("\\s+", ";").replaceAll(";+", ";");
					for (String spString : insideString.split(";")) {
						if (spString.contains(":") && !spString.endsWith(":")) {
							mapkeyword.put(spString.split(":")[1], "");
						}
						else if (!spString.endsWith(":")) {
							mapkeyword.put(spString, "");
						}					
						
					}			
				}
				text = text.replaceAll(";+", ";");
			}

			text = text.replaceAll(": ", ":").replaceAll("([^a-zA-Z\\s]+)\\s+", "$1;").replaceAll("-", ";");
			text = text.replaceAll(";\\s+",";").replaceAll(";+", ";");
			for (String key : text.split(";")) {
				if (key.contains(":") && !key.endsWith(":")) {
					mapkeyword.put(key.split(":")[1], "");
				}
				else if (!key.endsWith(":")){
					mapkeyword.put(key, "");
				}		
			}
			
			String keyword="";
			for (String key : mapkeyword.keySet()) {
				keyword = keyword + key + ";";
			}
			keyword = keyword.replaceAll(";$", "").replaceAll("^;", "").replaceAll(";+", ";");
			keyword = keyword.replace("，", ",");
			keyword = keyword.replaceAll(",$", "").replaceAll("^,", "");		
			return keyword;
		}

		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken<Map<String, String>>() {
			}.getType();
			Map<String, String> mapJson = gson.fromJson(value.toString(), type);

			String d = mapJson.get("d").trim();
			String rawid = mapJson.get("dxNum").trim();
			String down_date = mapJson.get("down_date").trim();
//			String down_date = "20190527";
			String html = mapJson.get("detail").trim();	    
	    	
			Document doc = Jsoup.parse(html);
		
			String title_c = "";//标题
			String Showwriter = "";//作者
			String press_year = "";//发行商，地区，年份
			String tsisbn = "";//isbn号
			String Pagecount = "";//页码数
			String tsseriesname = "";//丛书名
			String tsprice="";//价格
			String keyword_c="";//关键字
			String class_ = "";//中图分类
			String remark_c = "";//内容提要
			String Strreftext = "";//参考文献格式
			
			Element eleTitle = doc.select("div.card_text > dl > dt").first();
			if (eleTitle != null) {
				title_c = eleTitle.text().trim();
			}
			else {
				title_c = doc.title().trim();
				if (title_c.endsWith("_图书搜索")) {
					title_c = title_c.substring(0, title_c.length() - "_图书搜索".length());
				}
			}
			
			for (Element ele: doc.select("div.card_text > dl > dd")) {
				String line = ele.text().trim();
				if (line.startsWith("作 者 ：")) {
					Showwriter = line.substring("作 者 ：".length());
				}
				else if (line.startsWith("出版发行 :")) {
					press_year = line.substring("出版发行 :".length());
				}
				else if (line.startsWith("ISBN号 ：")) {
					tsisbn = line.substring("ISBN号 ：".length());
				}
				else if (line.startsWith("页 数 ：")) {
					Pagecount = line.substring("页 数 ：".length());
				}
				else if (line.startsWith("原书定价 : ")) {
					tsprice = line.substring("原书定价 : ".length());
				}
				else if (line.startsWith("中图法分类号 : ")) {
					class_ = line.substring("中图法分类号 : ".length());
				}
				else if (line.startsWith("参考文献格式 :")) {
					Strreftext = line.substring("参考文献格式 :".length());
				}
				else if (line.startsWith("丛书名 : ")) {
					tsseriesname = line.substring("丛书名 : ".length());
				}
				else if (line.startsWith("主题词 : ")) {
					keyword_c = line.substring("主题词 : ".length());
				}
				else if (line.startsWith("内容提要:")) {
					remark_c = line.substring("内容提要:".length());
				}
							 
			}
			
			Element eleRemark = doc.select("#little").first();
			if (eleRemark != null) {
				remark_c = eleRemark.text().trim();
			}
			
			
			String NetFullTextAddr = "";
			String lngID = "";
			String tspubdate = "";
			String years = "";
			String tsprovinces = "";
			String tspress = "";
			
			String product = "DUXIU";
			String sub_db = "TS";
			String provider = "CHAOXING";
			String sub_db_id = "00007";
			String source_type = "1";
			String country = "CN";
			String language = "ZH";
			
			
			
			NetFullTextAddr = String.format("http://book.duxiu.com/bookDetail.jsp?dxNumber=%s&d=%s", rawid, d);
//			lngID = String.format("DU_TS_%d", (Long.parseLong(rawid) * 2 + 7));
			lngID = VipIdEncode.getLngid("00007",rawid,false);


			int batchyear = 0;

			Date dt = new Date();
			DateFormat df = new SimpleDateFormat("yyyy");
			String nowDate = df.format(dt);
			batchyear = Integer.parseInt(nowDate) + 1;
			Pattern pa = Pattern.compile("\\d{4}(\\.\\d{2})?");
			String[] tmp = press_year.split(",");
			if (tmp.length > 1) {
				try {
					Matcher mat = pa.matcher(tmp[tmp.length - 1].trim());
					if (mat.find()) {
						if (1000 < Integer.parseInt(mat.group().substring(0, 4))
								&& Integer.parseInt(mat.group().substring(0, 4)) < batchyear) {
							tspubdate = mat.group();
							years = mat.group().substring(0, 4);
						} else {
							tspubdate = tmp[tmp.length - 1];
						}

					}
				} catch (Exception e) {
					//System.out.println(e.toString());
				}
				String xxx = press_year.substring(0, press_year.length() - tmp[tmp.length - 1].length());

				if (xxx.split("：").length > 1) {
					tsprovinces = xxx.split("：")[0].trim();
					tspress = xxx.split("：")[1].trim().replaceAll(" ,", "").replaceAll(",", ";");
				} else {
					tspress = xxx.trim().replaceAll(" ,", "").replaceAll(",", ";");
				}
			} else {
				Matcher mat = pa.matcher(tmp[0]);
				if (mat.find()) {

					if (1000 < Integer.parseInt(mat.group().substring(0, 4))
							&& Integer.parseInt(mat.group().substring(0, 4)) < batchyear) {

						tspubdate = mat.group().trim();
						years = tspubdate.substring(0, 4).trim();
					} else {
						tspress = tmp[0].trim();
					}
				} else {
					if (tmp[0].split("：").length > 1) {
						tsprovinces = tmp[0].split("：")[0].trim();
						tspress = tmp[0].split("：")[1].trim();
					} else {
						tspress = tmp[0].trim();
					}
				}
			}

			if (Pattern.compile("\\((?:[^()]+\\s[^()]+)+\\)").matcher(keyword_c).find()) {
				keyword_c = keyword_c.replaceAll("\\)\\s+", ");");
			} else {
				keyword_c = keyword_c.replaceAll(":\\s+", ":").trim();
			}
			keyword_c = keyword_c.replaceAll("\\\\", ";");
			keyword_c = getKeyword(keyword_c);


			Showwriter = Showwriter.replace('；', ';').replace('，', ';').replace(' ', ';').replace(',', ';').trim();//作者
			class_ = class_.replace(';', ' ').replace('；', ' ').trim();//中图分类
			tsprovinces = tsprovinces.replace(' ', ';').trim(); //机构
			
			String pub_date = tspubdate.replace(".", "");
			if (pub_date.length()==6) {
				pub_date = pub_date + "00";
			}
			else if (pub_date.length()==4) {
				pub_date = pub_date + "0000";
			}
			
			String clc_no = class_;
			clc_no = clc_no.replaceAll("\\(.*\\)", "").trim();
			
			tsisbn = tsisbn.replace("-", "");
			
			XXXXObject xObjout = new XXXXObject();
			
			xObjout.data.put("lngid",lngID);
			xObjout.data.put("rawid",rawid);
			xObjout.data.put("rawid_alt",d);
			xObjout.data.put("product",product);
			xObjout.data.put("sub_db",sub_db);
			xObjout.data.put("provider",provider);
			xObjout.data.put("sub_db_id",sub_db_id);
			xObjout.data.put("source_type",source_type);
			xObjout.data.put("provider_url",NetFullTextAddr);
			xObjout.data.put("country",country);
			xObjout.data.put("language",language);
			xObjout.data.put("title",title_c);
			xObjout.data.put("author",Showwriter);
			xObjout.data.put("pub_date",pub_date);
			xObjout.data.put("pub_year",years);
			xObjout.data.put("publisher",tspress);
			xObjout.data.put("pub_place",tsprovinces);
			xObjout.data.put("subject",class_);
			xObjout.data.put("clc_no",clc_no);
			xObjout.data.put("keyword",keyword_c);
			xObjout.data.put("abstract",remark_c);
			xObjout.data.put("isbn",tsisbn);
			xObjout.data.put("page_cnt",Pagecount);
			xObjout.data.put("title_series",tsseriesname);
			xObjout.data.put("price",tsprice);
			xObjout.data.put("citation",Strreftext);
			xObjout.data.put("down_date",down_date);
			xObjout.data.put("batch",batch);		

			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObjout);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}

	}
}