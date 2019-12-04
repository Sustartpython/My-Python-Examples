package simple.jobstream.mapreduce.site.cnki_fg;

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
public class XXXXObject_cnkifg extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = "XXXXObjectfg";
		if (testRun) {
			jobName = jobName;
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
			String title = "";//主题
			String date_created="";//发布日期
			String date_impl="";//实施日期
			String identifier_standard = "";//发文字号
			String creator_release = "";//发布机关
			String description_type = "";//效力级别
			String legal_status = "";//时效性
			String subject = "";//关键词
			String description = "";//摘要
			
			int idx = htmlText.indexOf('★');
			rawid = htmlText.substring(0,idx);	
			Document doc = Jsoup.parse(htmlText);
			try {
				Element titl =  doc.getElementById("title");
				title  = titl.text();
			} catch (Exception e) {
				// TODO: handle exception
			}
			try {
				Element div = doc.select("div[class=summary pad10]").first();
				Elements allp =  div.select("p");
				for (Element data : allp) {
					
					if(data.text().startsWith("【发布日期】")){
						date_created = data.text().replace("'", "''").replace("【发布日期】", "").replace("'", "''");
						}
					if(data.text().startsWith("【实施日期】")){
						date_impl = data.text().replace("【实施日期】", "").replace("'", "''");
					}
					
					if(data.text().startsWith("【发文字号】")){
						identifier_standard = data.text().replace("【发文字号】", "").replace("'", "''");
					}
					if(data.text().startsWith("【发布机关】")){
						creator_release = data.text().replace("【发布机关】", "").replace("'", "''");
					}
					if(data.text().startsWith("【正文快照】")){
						description = data.text().replace("【正文快照】", "").replace("'", "''");
					}
					if(data.text().startsWith("【效力级别】")){
						description_type = data.text().replace("【效力级别】", "").replace("'", "''");
					}
					if(data.text().startsWith("【时效性】")){
						legal_status = data.text().replace("【时效性】", "").replace("'", "''");
					}
				}
				Element kws = doc.select("div[class = keywords]").first();
				
				subject = kws.text().replace("【关键词】", "").replace("；", ";").replace("'", "''");
			} catch (Exception e) {
				// TODO: handle exception
			}
			
				
			
			
			if (rawid.trim().length() > 0) {			
				map.put("rawid", rawid);
				map.put("title", title);
				map.put("date_created", date_created);
				map.put("date_impl", date_impl);
				map.put("identifier_standard", identifier_standard);
				map.put("creator_release", creator_release);
				map.put("description_type", description_type);
				map.put("legal_status", legal_status);
				map.put("subject", subject);
				map.put("description", description);
				
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
			if(map.get("title").equals("")){
				context.getCounter("map", "not find title").increment(1);
				return;
			}
			
			XXXXObject xObj = new XXXXObject();
			
			xObj.data.put("rawid", map.get("rawid"));
			xObj.data.put("title", map.get("title"));
			xObj.data.put("date_created", map.get("date_created"));
			xObj.data.put("date_impl", map.get("date_impl"));
			xObj.data.put("identifier_standard", map.get("identifier_standard"));
			xObj.data.put("creator_release", map.get("creator_release"));
			xObj.data.put("description_type", map.get("description_type"));
			xObj.data.put("legal_status", map.get("legal_status"));
			xObj.data.put("subject", map.get("subject"));
			xObj.data.put("description", map.get("description"));
			
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
