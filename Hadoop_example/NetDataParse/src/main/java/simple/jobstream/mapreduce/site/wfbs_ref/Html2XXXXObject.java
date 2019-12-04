package simple.jobstream.mapreduce.site.wfbs_ref;

import java.io.IOException;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

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
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.nodes.Node;
import org.jsoup.nodes.TextNode;
import org.omg.CosNaming.NamingContextPackage.NotEmpty;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 60;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "wf_bs_ref." + this.getClass().getSimpleName();
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
		
		
		//处理一个item，即一条引文
		private static void procOneItem(Context context, String rawsourceid, Element itemElement) 
				throws IOException, InterruptedException  {
			String refertext = "";
			String strtitle = "";
			String strtype = "";
			String strname = "";
			String strwriter1 = "";
			String stryearvolnum = "";
			String strpubwriter = "";
			
			//无全文时去掉第一个div，有全文时去掉“全文”链接
			Element divBlank = itemElement.select("div.left.fulltext.no-fulltext").first();	//无全文
			if (divBlank != null) {
				divBlank.remove();
			}
			Element linkFulltext = itemElement.select("a.left.fulltext.has-fulltext").first();	//有全文
			if (linkFulltext != null) {
				linkFulltext.remove();
			}
			
			refertext = itemElement.text().trim();
			if (refertext.length() < 1) {
				context.getCounter("map", "null refertext").increment(1);
				return;
			}
			
			strtitle = getStrTitle(itemElement);
			strtype = getStrType(itemElement);
			strname = getStrName(itemElement, strtype);
			strwriter1 = getWriter(itemElement);
			stryearvolnum = getYearNum(itemElement);
			strpubwriter = getStrPubWriter(itemElement);
			
			if (strpubwriter.trim().length() > 0) {
				context.getCounter("map", "strpubwriter").increment(1);
			}
			else {
				context.getCounter("map", "null strpubwriter").increment(1);
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
			
			context.getCounter("map", "count").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawsourceid), new BytesWritable(bytes));		
		}
				
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString().trim();
	    	
	    	int idx = text.indexOf('★');
	    	if (idx < 1) {
	    		context.getCounter("map", "idx < 1").increment(1);
				return;
			}
	    		    	
	    	String rawsourceid = text.substring(0, idx);	//filename
	    	if (!rawsourceid.endsWith("_ref")) {
	    		context.getCounter("map", "rawsourceid not endswith _ref").increment(1);
				return;
			}
	    	rawsourceid = rawsourceid.substring(0, rawsourceid.length()-4);
	    	
	    	String html = text.substring(idx+1).trim();
	    	Document doc = Jsoup.parse(html);
	    	
	    	Element divPaperList = doc.select("div.paper-list").first();
	    	if (divPaperList == null) {
	    		context.getCounter("map", "divPaperList == null").increment(1);
				return;
			}
	    	
	    	for (Element itemElement : divPaperList.select("div.item")) {
				procOneItem(context, rawsourceid, itemElement);
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
