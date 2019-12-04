package simple.jobstream.mapreduce.site.vip_bz;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.Dictionary;
import java.util.Hashtable;
import java.util.List;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.IntWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.apache.hadoop.metrics.spi.NullContext;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Json2Txt extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 1;
	private static int reduceNum = 1;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
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
		job.getConfiguration().setFloat("mapred.reduce.slowstart.completed.maps", 0.7f);
		System.out.println("******mapred.reduce.slowstart.completed.maps*******" + job.getConfiguration().get("mapred.reduce.slowstart.completed.maps"));
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		
		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);
		
	    job.setMapOutputKeyClass(Text.class);
	    job.setMapOutputValueClass(Text.class);
	    
	    job.setOutputKeyClass(Text.class);
	    job.setOutputValueClass(NullWritable.class);
	    
	    //job.setInputFormatClass(SimpleTextInputFormat.class);
	    job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(TextOutputFormat.class);
	    
	    TextOutputFormat.setCompressOutput(job, false);
	    
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
			Mapper<LongWritable, Text, Text, Text> {
		
		static int cnt = 0;	
		
		
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	
	    	String line = value.toString().trim();
	    	
	    	Gson gson = new Gson();
			Type type = new TypeToken< Map<String,Object>>(){}.getType();
			
			
			
			//Map<String,  Map<String, String>> mapField = gson.fromJson(value.toString(), type);
			Map<String, List<Map<String, Object>>> mapField = gson.fromJson(line, type);			
			
			List<Map<String, Object>> listData = mapField.get("data");
			
			if (listData.size() < 1) {
				context.getCounter("map", "listData null").increment(1);
				return;
			}
			
			
			String rawid = "";
			String rowId = "";
			String ID = "";
			String bzID = "";
			String bzName_c = "";
			String bzName_e = "";
			String bzFirstClass = "";
			String creator_release = "";
			String bzMainType = "";
			String years = "";
			String identifier_standard = "";
			String date_created = "";
			String date_impl = "";
			String date_assure = "";
			String date_ban = "";
			String bzSource = "";
			String bzStatus = "";
			String country = "";
			String creator_drafting = "";
			String drafter = "";
			String committee = "";
			String keyword_c = "";
			String keyword_e = "";
			String subject_csc = "";
			String subject_isc = "";
			String ranges = "";
			String page = "";
			String language = "";
			String fullNetAddr = "";
			String bzsubsbz = "";
			String bzrefbz = "";
			String bzrelationship = "";
			String bzreplacedbz = "";
			String refbz = "";
			String relationship = "";
			String bznum2 = "";
			String lastedUpdateType = "";
			String lastUpdateTime = "";
			
			for (Map<String, Object> mapRoot : listData) {
				if (mapRoot.containsKey("rowId") && (mapRoot.get("rowId")!=null)) {
					rowId = mapRoot.get("rowId").toString().trim();
				}
				
				if (mapRoot.containsKey("ID") && (mapRoot.get("ID")!=null)) {
					ID = mapRoot.get("ID").toString().trim();
				}
				
				if (mapRoot.containsKey("BZID") && (mapRoot.get("BZID")!=null)) {
					bzID = mapRoot.get("BZID").toString().trim();
				}
				
				if (mapRoot.containsKey("标准名称") && (mapRoot.get("标准名称")!=null)) {
					bzName_c = mapRoot.get("标准名称").toString().trim();
				}
				
				if (mapRoot.containsKey("英文标准名称") && (mapRoot.get("英文标准名称")!=null)) {
					bzName_e = mapRoot.get("英文标准名称").toString().trim();
				}
				
				if (mapRoot.containsKey("中国标准一级分类号") && (mapRoot.get("中国标准一级分类号")!=null)) {
					bzFirstClass = mapRoot.get("中国标准一级分类号").toString().trim();
				}
				
				if (mapRoot.containsKey("二级标准类别") && (mapRoot.get("二级标准类别")!=null)) {
					creator_release = mapRoot.get("二级标准类别").toString().trim();
				}
				
				if (mapRoot.containsKey("标准类别") && (mapRoot.get("标准类别")!=null)) {
					bzMainType = mapRoot.get("标准类别").toString().trim();
				}
				
				if (mapRoot.containsKey("年份") && (mapRoot.get("年份")!=null)) {
					years = mapRoot.get("年份").toString().trim();
				}
				
				if (mapRoot.containsKey("标准编号") && (mapRoot.get("标准编号")!=null)) {
					identifier_standard = mapRoot.get("标准编号").toString().trim();
				}
				
				if (mapRoot.containsKey("发布日期") && (mapRoot.get("发布日期")!=null)) {
					date_created = mapRoot.get("发布日期").toString().trim();
				}
				
				if (mapRoot.containsKey("实施日期") && (mapRoot.get("实施日期")!=null)) {
					date_impl = mapRoot.get("实施日期").toString().trim();
				}
				
				if (mapRoot.containsKey("确认日期") && (mapRoot.get("确认日期")!=null)) {
					date_assure = mapRoot.get("确认日期").toString().trim();
				}
				
				if (mapRoot.containsKey("废止日期") && (mapRoot.get("废止日期")!=null)) {
					date_ban = mapRoot.get("废止日期").toString().trim();
				}
				
				if (mapRoot.containsKey("标准来源") && (mapRoot.get("标准来源")!=null)) {
					bzSource = mapRoot.get("标准来源").toString().trim();
				}
				
				if (mapRoot.containsKey("标准状态") && (mapRoot.get("标准状态")!=null)) {
					bzStatus = mapRoot.get("标准状态").toString().trim();
				}
				
				if (mapRoot.containsKey("国别") && (mapRoot.get("国别")!=null)) {
					country = mapRoot.get("国别").toString().trim();
				}
				
//				if (mapRoot.containsKey("发布单位")) {
//					creator_release = mapRoot.get("发布单位").trim();
//				}
				
				if (mapRoot.containsKey("起草单位") && (mapRoot.get("起草单位")!=null)) {
					creator_drafting = mapRoot.get("起草单位").toString().trim();
				}
				
				if (mapRoot.containsKey("起草人") && (mapRoot.get("起草人")!=null)) {
					drafter = mapRoot.get("起草人").toString().trim();
				}

				if (mapRoot.containsKey("标准技术委员会") && (mapRoot.get("标准技术委员会")!=null)) {
					committee = mapRoot.get("标准技术委员会").toString().trim();
				}
				
				if (mapRoot.containsKey("主题词") && (mapRoot.get("主题词")!=null)) {
					keyword_c = mapRoot.get("主题词").toString().trim();
				}
				
				if (mapRoot.containsKey("英文主题词") && (mapRoot.get("英文主题词")!=null)) {
					keyword_e = mapRoot.get("英文主题词").toString().trim();
				}
				
				if (mapRoot.containsKey("中国标准分类号") && (mapRoot.get("中国标准分类号")!=null)) {
					subject_csc = mapRoot.get("中国标准分类号").toString().trim();
				}
				
				if (mapRoot.containsKey("国际标准分类号") && (mapRoot.get("国际标准分类号")!=null)) {
					subject_isc = mapRoot.get("国际标准分类号").toString().trim();
				}
				
				if (mapRoot.containsKey("适用范围") && (mapRoot.get("适用范围")!=null)) {
					ranges = mapRoot.get("适用范围").toString().trim();
				}
				
				if (mapRoot.containsKey("页码") && (mapRoot.get("页码")!=null)) {
					page = mapRoot.get("页码").toString().trim();
				}
				
				if (mapRoot.containsKey("正文语种") && (mapRoot.get("正文语种")!=null)) {
					language = mapRoot.get("正文语种").toString().trim();
				}
				
				if (mapRoot.containsKey("全文地址") && (mapRoot.get("全文地址")!=null)) {
					fullNetAddr = mapRoot.get("全文地址").toString().trim();
				}
				
				if (mapRoot.containsKey("代替标准及时间") && (mapRoot.get("代替标准及时间")!=null)) {
					bzsubsbz = mapRoot.get("代替标准及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("引用标准及时间") && (mapRoot.get("引用标准及时间")!=null)) {
					bzrefbz = mapRoot.get("引用标准及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("采用关系及时间") && (mapRoot.get("采用关系及时间")!=null)) {
					bzrelationship = mapRoot.get("采用关系及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("被代替标准及时间") && (mapRoot.get("被代替标准及时间")!=null)) {
					bzreplacedbz = mapRoot.get("被代替标准及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("被引用标准及时间") && (mapRoot.get("被引用标准及时间")!=null)) {
					refbz = mapRoot.get("被引用标准及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("被采用关系及时间") && (mapRoot.get("被采用关系及时间")!=null)) {
					relationship = mapRoot.get("被采用关系及时间").toString().trim();
				}
				
				if (mapRoot.containsKey("最后更新时间") && (mapRoot.get("最后更新时间")!=null)) {
					lastUpdateTime = mapRoot.get("最后更新时间").toString().trim();
				}
				
				if (mapRoot.containsKey("最后更新类型") && (mapRoot.get("最后更新类型")!=null)) {
					lastedUpdateType = mapRoot.get("最后更新类型").toString().trim();
				}
		
				
				
//				if (bzName_c.isEmpty() && bzName_e.isEmpty()) {
//					context.getCounter("map", "bzName null").increment(1);
//					continue;
//				}
				
				if (lastedUpdateType.equals("删除")) {
					context.getCounter("map", "删除").increment(1);
					continue;
				}
					
				
				//获取bznum2
				bznum2 = identifier_standard.replaceAll("[^a-z^A-Z^0-9]", "");
				
				rawid = identifier_standard;
				
			
				context.getCounter("map", "count").increment(1);
				
				context.write(new Text(identifier_standard), new Text(bzID));	
				
			}		
		
		}
	}


	public static class ProcessReducer extends
		Reducer<Text, Text, Text, NullWritable> {
		public void reduce(Text key, Iterable<Text> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			int cnt = 0;
			String outLine = key.toString() + "\t";
			for (Text item : values) {
				cnt += 1;
				outLine += item.toString() + ";";
			}
			if (cnt < 2) {
				return;
			}
				
			context.getCounter("reduce", "count").increment(cnt);	
			context.getCounter("reduce", "out").increment(1);	
		
			context.write(new Text(outLine), NullWritable.get());
		}
	}
}
