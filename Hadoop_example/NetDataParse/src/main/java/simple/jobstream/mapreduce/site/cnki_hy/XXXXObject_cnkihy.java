
package simple.jobstream.mapreduce.site.cnki_hy;

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
public class XXXXObject_cnkihy extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 40;
	private static int reduceNum = 40;
	
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
			String description_fund = "";
			
			int idx = htmlText.indexOf('★');
			rawid = htmlText.substring(0,idx);	
			Document doc = Jsoup.parse(htmlText);
			try{
				title_c  = doc.select("h1").text();
			}catch(Exception e){
				title_c = "";
			}
			try{
				Element div = doc.select("div[class=summary pad10]").first();
				Elements divs = div.select("p");
				for (Element data : divs) {
					
					if(data.text().startsWith("【作者】")){
						Showwriter=data.text().replace("'", "''").replace("【作者】", "").replace("'", "''");
						}
					if(data.text().startsWith("【机构】")){
						Showorgan = data.text().replace("【机构】", "").replace("'", "''");
					}
					
					if(data.text().startsWith("【摘要】")){
						remark_c = data.text().replace("【摘要】", "").replace("'", "''");
					}
				}
				}catch(Exception e){
				e.printStackTrace();
			}
			try{
				Elements divs = doc.select("div[class = keywords]");
				for(Element div:divs){
					if(div.text().startsWith("【关键词】")){
						keyword_c = div.text();
						keyword_c = keyword_c.replace("【关键词】", "").replace("；", ";").replace("'", "''");
					}
					if(div.text().startsWith("【基金】")){
						description_fund = div.text();
						description_fund = description_fund.replace("【基金】", "").replace("；", ";").replace("'", "''");
					}
			
				}
				}catch(Exception e){
				e.printStackTrace();
			}
			try {
				Element div = doc.select("div[class = summary]").first();
				Elements lis = div.select("li");
				for(Element li:lis){
					if(li.text().startsWith("【会议录名称】")){
						hymeetingrecordname = li.text();
						hymeetingrecordname = hymeetingrecordname.replace("【会议录名称】", "").replace("'", "''");
					}
					if(li.text().startsWith("【会议名称】")){
						media_c = li.text();
						media_c = media_c.replace("【会议名称】", "").replace("'", "''");
					}
					if(li.text().startsWith("【会议时间】")){
						hymeetingdate = li.text();
						hymeetingdate = hymeetingdate.replace("【会议时间】", "").replace("'", "''");
						years = hymeetingdate.substring(0,4);
					}
					if(li.text().startsWith("【会议地点】")){
						hymeetingplace = li.text();
						hymeetingplace = hymeetingplace.replace("【会议地点】", "").replace("'", "''");
					}
					if(li.text().startsWith("【分类号】")){
						flh = li.text();
						flh = flh.replace("【分类号】", "").replace("'", "''");
					}
					if(li.text().startsWith("【主办单位】")){
						hypressorganization = li.text();
						hypressorganization = hypressorganization.replace("【主办单位】", "").replace("'", "''");
					}
				}
			} catch (Exception e) {
				// TODO: handle exception
			}
			
			
			if (rawid.trim().length() > 0) {			
				map.put("rawid", rawid);
				map.put("title_c", title_c);
				map.put("years", years);
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
				map.put("description_fund", description_fund);
				
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
