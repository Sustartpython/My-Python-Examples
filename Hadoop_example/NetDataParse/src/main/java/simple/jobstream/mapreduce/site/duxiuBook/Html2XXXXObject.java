package simple.jobstream.mapreduce.site.duxiuBook;

import java.io.IOException;

import org.apache.hadoop.hdfs.server.namenode.status_jsp;
import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.VipcloudUtil;

//统计wos和ei的数据量
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static boolean testRun = false;
	private static int testReduceNum = 5;
	private static int reduceNum = 5;
	
	public static  String inputHdfsPath = "";
	public static  String outputHdfsPath = "";
	
	public void pre(Job job)
	{
		String jobName = "DuxiuHtml2XXXXObject";
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

	public void SetMRInfo(Job job) {
		job.getConfiguration().set("io.compression.codecs", "org.apache.hadoop.io.compress.GzipCodec,org.apache.hadoop.io.compress.DefaultCodec");
		System.out.println("******io.compression.codecs*******" + job.getConfiguration().get("io.compression.codecs"));
		
		job.setInputFormatClass(TextInputFormat.class);
	    job.setOutputFormatClass(SequenceFileOutputFormat.class);

		job.setMapOutputKeyClass(Text.class);
		job.setMapOutputValueClass(BytesWritable.class);

		job.setOutputKeyClass(Text.class);
		job.setOutputValueClass(BytesWritable.class);

		job.setMapperClass(ProcessMapper.class);
		job.setReducerClass(ProcessReducer.class);

		TextOutputFormat.setCompressOutput(job, false);

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

	// ======================================处理逻辑=======================================
	//继承Mapper接口,设置map的输入类型为<Object,Text>
	//输出类型为<Text,IntWritable>
	public static class ProcessMapper extends Mapper<LongWritable, Text, Text, BytesWritable> {
		
		static int cnt = 0;	
		
	    public void map(LongWritable key, Text value, Context context) throws IOException, InterruptedException {
	    	cnt += 1;
	    	if (cnt == 1) {
				System.out.println("text:" + value.toString());
			}
	    	/*
			String indexs = value.toString().substring(0, 47);
	    	
			String html = value.toString().substring(46);
			if (!html.startsWith("<!DOCTYPE") && !html.endsWith("<\\html>")) {
				return;
			}
			*/
	    	String text = value.toString().trim();
	    	int idx1 = text.indexOf('★');
	    	int idx2 = text.indexOf('☆');
	    	if (!((idx1 > 0) && (idx2 > idx1))) {
	    		context.getCounter("map","idx < 1").increment(1);
	    		return;
	    	}
	    	String rawid = "";//
		    String d ="";//
		    rawid = text.substring(0,idx1).trim();
		    d = text.substring(idx1+1,idx2).trim();
		    String html = text.substring(idx2+1).trim();
		    
		    /*
		    if (html.toUpperCase().indexOf(rawid.toUpperCase()) < 0) {
		    	context.getCounter("map","error: html not find filename").increment(1);
		    	return;
		    }*/
	    	
			Document doc = Jsoup.parse(html);
			//所有的字段信息
		
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
				
//				else if (line.startsWith("附 注 : ")) {
//					keyword_c = line.substring("附 注 : ".length());
//				}				 
			}
			
			Element eleRemark = doc.select("#little").first();
			if (eleRemark != null) {
				remark_c = eleRemark.text().trim();
			}
			
			
			
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("title_c",title_c);	
			xObj.data.put("Showwriter", Showwriter);
			xObj.data.put("class", class_);
			xObj.data.put("keyword_c", keyword_c);
			xObj.data.put("remark_c", remark_c);
			xObj.data.put("tsisbn", tsisbn);
			xObj.data.put("Pagecount", Pagecount);
			xObj.data.put("tsseriesname", tsseriesname);
			xObj.data.put("tsprice", tsprice);
			xObj.data.put("Strtrftext", Strreftext);
			xObj.data.put("rawid", rawid);
			xObj.data.put("press_year", press_year);
			xObj.data.put("d", d);

			
			//String textString = String.format("%s %s %s %s %s %s %s %s %s %s %s", title_c,showwriter,class_,keyword,remark_c,isbn,page,seriesname,charge,strreftext,rawid);
			context.getCounter("map", "count").increment(1);
			//context.write(new Text(textString), NullWritable.get());
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(rawid), new BytesWritable(bytes));
		}

	}
	//继承Reducer接口，设置Reduce的输入类型为<Text,IntWritable>
	//输出类型为<Text,IntWritable>
	public static class ProcessReducer extends Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, Context context) throws IOException, InterruptedException {
			/*
			Text out =  new Text()
			for (BytesWritable val:values){
				VipcloudUtil.DeserializeObject(val.getBytes(), out);
				context.write(key,out);
			}
		//context.getCounter("reduce", "count").increment(1);*/
			
			BytesWritable bOut = new BytesWritable();	//������������
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//ѡ������һ��
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//��buffer��Ϊʵ�ʳ���
		
			context.write(key, bOut);
			
		}
	}
}