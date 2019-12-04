package simple.jobstream.mapreduce.site.wanfang_hy;

import java.io.IOException;
import java.util.HashMap;

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

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class XXXXObjectHY extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 10;
	private static int reduceNum = 10;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "XXXXObjectHY";
		if (testRun) {
			jobName = "test_" + jobName;
		}
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
		System.out.println(job.getConfiguration().get("io.compression.codecs"));

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(BytesWritable.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(BytesWritable.class);
	    
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
		
		public static HashMap<String,String> parseHtml(String htmlText){
			HashMap<String,String> map = new HashMap<String,String>();
			String rawid = "";//id
			String title_c = "";//主题
			String years="";//会议年份
			String hymeetingrecordname="";//母体文献
			String Showwriter = "";//作者
			String Showorgan = "";//机构
			String media_c = "";//会议名称
			String hymeetingplace = "";//会议地点
			String keyword_c = "";//关键词
			String remark_c = "";//摘要
			String hypressorganization = "";//主办单位
			String flh = "";
			String hymeetingdate = "";
			String years2 = "";
			
			int idx = htmlText.indexOf('★');
			rawid = htmlText.substring(0,idx);	
			Document doc = Jsoup.parse(htmlText);
			try{
				title_c  = doc.select("h1").text();
			}catch(Exception e){
				title_c = "";
			}
			try{
				remark_c = doc.select("meta[name = description]").first().attr("content");
				remark_c = remark_c.substring(2).replace("\n", "").replace("\t", "").replace("\r", "");
			}catch(Exception e){
				remark_c = "";
			}
			try{
				keyword_c = doc.select("meta[name = keywords]").first().attr("content");
				keyword_c = keyword_c.replace(" ", ";");
			}catch(Exception e){
				keyword_c = "";
			}

			Element masthead = doc.select("div[class=fixed-width-wrap fixed-width-wrap-feild]").first();
			try{
				Element zuoz = doc.select("div[class=row row-author]").first();
				Elements links = zuoz.getElementsByTag("a");
				for (Element link : links) {
						 String linkText = link.text();
						 Showwriter = Showwriter+linkText+";";
					}
				}catch (Exception e) {
					// TODO: handle exception
					Showwriter = "";
				}
			try{
				Element div = masthead.select("div[class=fixed-width baseinfo-feild]").first();
				Elements divs = div.getElementsByTag("div");
				for (Element data : divs) {
					
					if(data.select("span[class=pre]").text().equals("作者单位：")){
						Showorgan=data.select("span[class=text]").text().replace("'", "''");
						}
					if(data.select("span[class=pre]").text().equals("母体文献：")){
						hymeetingrecordname = data.select("span[class=text]").text();
					}
					
					if(data.select("span[class=pre]").text().equals("会议名称：")){
						media_c = data.select("span[class=text]").text();
					}
					
					if(data.select("span[class=pre]").text().equals("会议时间：")){
						years = data.select("span[class=text]").text();
						hymeetingdate = years.replace("年", "-").replace("月", "-").replace("日", "");
						years2 = years.substring(0,4);
					}
					
					if(data.select("span[class=pre]").text().equals("会议地点：")){
						hymeetingplace = data.select("span[class=text]").text();
					}
					if(data.select("span[class=pre]").text().equals("主办单位：")){
						hypressorganization = data.select("span[class=text]").text();
					}
					if(data.select("span[class=pre]").text().equals("分类号：")){
						flh = data.select("span[class=text]").text();
					}
				}
			}catch(Exception e){
				e.printStackTrace();
			}
			
			
			if (rawid.trim().length() > 0) {			
				map.put("rawid", rawid);
				map.put("title_c", title_c);
				map.put("years", years2);
				map.put("hymeetingrecordname", hymeetingrecordname);
				map.put("Showwriter", Showwriter);
				map.put("Showorgan", Showorgan);
				map.put("media_c", media_c);
				map.put("hymeetingplace", hymeetingplace);
				map.put("keyword_c", keyword_c);
				map.put("remark_c", remark_c);
				map.put("hypressorganization", hypressorganization);
				map.put("hymeetingdate", hymeetingdate);
				map.put("flh", flh);
				
			}
			return map;

		}
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String text = value.toString();
	    	int idx = text.indexOf('★');
	    	if (idx < 1) {
	    		context.getCounter("map", "not find ★").increment(1);	
	    		return;
			}

	    	HashMap<String, String>map = parseHtml(text);
			if(map.size() < 1){
				context.getCounter("map", "map.size() < 1").increment(1);	
	    		return;				
			}
			if(map.get("title_c").equals("")){
				context.getCounter("map", "not find title_c").increment(1);
				return;
			}
			
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("rawid", map.get("rawid"));
			xObj.data.put("title_c",map.get("title_c"));
			
			xObj.data.put("years", map.get("years"));
			xObj.data.put("hymeetingrecordname",map.get("hymeetingrecordname"));
			xObj.data.put("Showwriter", map.get("Showwriter"));
			
			xObj.data.put("Showorgan", map.get("Showorgan"));
			xObj.data.put("media_c", map.get("media_c"));
			xObj.data.put("hymeetingplace", map.get("hymeetingplace"));
			xObj.data.put("keyword_c", map.get("keyword_c"));
			xObj.data.put("remark_c", map.get("remark_c"));
			xObj.data.put("hypressorganization", map.get("hypressorganization"));
			xObj.data.put("hymeetingdate", map.get("hymeetingdate"));
			xObj.data.put("flh", map.get("flh"));
			byte[] bytes = com.process.frame.util.VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(map.get("rawid")), new BytesWritable(bytes));		
		}
	}
	
	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//ѡ����һ��
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//��buffer��Ϊʵ�ʳ���
		
			context.write(key, bOut);
		}
	}
}
