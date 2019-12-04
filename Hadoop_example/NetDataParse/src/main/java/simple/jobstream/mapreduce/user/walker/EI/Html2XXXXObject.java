package simple.jobstream.mapreduce.user.walker.EI;

import java.io.IOException;
import java.lang.reflect.Type;
import java.util.HashSet;
import java.util.Map;

import org.apache.hadoop.io.BytesWritable;
import org.apache.hadoop.io.LongWritable;
import org.apache.hadoop.io.NullWritable;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapreduce.InputFormat;
import org.apache.hadoop.mapreduce.Job;
import org.apache.hadoop.mapreduce.Mapper;
import org.apache.hadoop.mapreduce.Reducer;
import org.apache.hadoop.mapreduce.Reducer.Context;
import org.apache.hadoop.mapreduce.lib.input.TextInputFormat;
import org.apache.hadoop.mapreduce.lib.output.MultipleOutputs;
import org.apache.hadoop.mapreduce.lib.output.SequenceFileOutputFormat;
import org.apache.hadoop.mapreduce.lib.output.TextOutputFormat;
import org.jsoup.Jsoup;
import org.jsoup.nodes.Document;
import org.jsoup.nodes.Element;
import org.jsoup.select.Elements;

import com.google.gson.Gson;
import com.google.gson.reflect.TypeToken;
import com.process.frame.base.InHdfsOutHdfsJobInfo;
import com.process.frame.base.BasicObject.BXXXXObject;
import com.process.frame.base.BasicObject.XXXXObject;
import com.process.frame.util.SimpleTextInputFormat;
import com.process.frame.util.VipcloudUtil;

//将JSON格式转化为BXXXXObject格式，包含去重合并
public class Html2XXXXObject extends InHdfsOutHdfsJobInfo {
	private static int reduceNum = 50;
	
	public static String inputHdfsPath = "";
	public static String outputHdfsPath = "";	//这个目录会被删除重建
  
	public void pre(Job job)
	{
		String jobName = this.getClass().getSimpleName();
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
		
	    public void map(LongWritable key, Text value, Context context
	                    ) throws IOException, InterruptedException {
	    	context.getCounter("map", "in").increment(1);
	    	
	    	Document doc = Jsoup.parse(value.toString());
			
			Elements eles = doc.select("input[name=\"docidlist\"]");
			if (eles.size() != 1) {
				context.getCounter("map", "docidlist eles.size() != 1").increment(1);	
				return;
			}
			String rawid = eles.get(0).attr("value");
			if (!rawid.startsWith("cpx_")) {
				context.getCounter("map", "rawid not startsWith cpx").increment(1);	
				return;
			}

			//Elements eles = doc.select("table[xmlns:pid]"); 		//这个地方不同批次有问题
			eles = doc.getElementsByTag("table");
			Element table = null;
			for (Element ele: eles) {
				context.getCounter("map", ele.parent().tagName()).increment(1);	   
				if (ele.parent().tagName() == "td") {
					table = ele;
				}
			}

			if (table == null) {		//找不到内层table
				context.getCounter("map", "table == null").increment(1);	   
				return;
			}
							
			String AccessionNumber = "";
			String Title = "";
			String Authors = "";
			String AuthorAffiliation = "";		//<option value="AF">Author affiliation</option>
			String CorrAuthorAffiliation = "";
			String CorrespondingAuthor = "";
			String SourceTitle = "";
			String Volume = "";
			String Issue = "";
			String Abstract = "";
			String CODEN = "";			//<option value="CN">CODEN</option>
			String ISSN = "";			//<option value="SN">ISSN</option>
			String EISSN = "";
			String ISBN10 = "";
			String ISBN13 = "";
			String DocumentType = "";
			String DOI = "";
			String ControlledTerms = "";
			String UncontrolledTterms = "";
			String MainHeading = "";
			String Pages = "";
			String ConferenceLocation = "";
			String IssueDate = "";
			String ConferenceDate = "";
			String Sponsor = "";
			String Publisher = "";		//<option value="PN">Publisher</option>
			String PublicationYear = "";			
			String ClassificationCode = "";		//EI分类号<option value="CL">Ei Classification code</option>
			String ConferenceCode = "";			//<option value="CC">Conference code</option>
			String Language = "";
			
			for (Element eleTr : table.getElementsByTag("tr")) {
				String text = eleTr.text();
				if (text.startsWith("Accession number:")) {
					AccessionNumber = text.substring("Accession number:".length()).trim();
				}
				else if (text.startsWith("Title:")) {
					Title = text.substring("Title:".length()).trim();
				}
				else if (text.startsWith("Authors:")) {
					eleTr = eleTr.html(eleTr.html().replace("<sup>", "[").replace("</sup>", "]"));	//替换sup为中括号
					Authors = eleTr.text().substring("Authors:".length()).trim();	//去掉前缀					
					Authors = Authors.replaceAll("\\s", " ");	//精简空白
					Authors = Authors.replace(" ;", ";");		//去掉分号前的空格
					Authors = Authors.replace("[]", "");	//去掉空的中括号
					Authors = Authors.trim();
				}
				else if (text.startsWith("Author affiliation:")) {
					for (Element eleTab : eleTr.getElementsByTag("table")) {	//一行机构一个table
						eleTab = eleTab.html(eleTab.html().replace("<sup>", "[").replace("</sup>", "]"));	//替换sup为中括号
						AuthorAffiliation += eleTab.text().replace(';', ',') + ";";		//替换单个机构里面的分号为逗号
					}	
					AuthorAffiliation = AuthorAffiliation.replaceAll("\\s", " ");	//精简空白
					AuthorAffiliation = AuthorAffiliation.replace("] ", "]"); 	//去掉标号后的空格
					AuthorAffiliation = AuthorAffiliation.trim();
				}
				else if (text.startsWith("Corr. author affiliation:")) {
					CorrAuthorAffiliation = text.substring("Corr. author affiliation:".length()).trim();
				}
				else if (text.startsWith("Corresponding author:")) {
					CorrespondingAuthor = text.substring("Corresponding author:".length()).trim();				
				}
				else if (text.startsWith("Source title:")) {
					SourceTitle = text.substring("Source title:".length()).trim();		
				}
				else if (text.startsWith("Volume:")) {
					Volume = text.substring("Volume:".length()).trim();		
				}
				else if (text.startsWith("Issue:")) {
					Issue = text.substring("Issue:".length()).trim();		
				}
				else if (text.startsWith("Abstract:")) {
					Abstract = text.substring("Abstract:".length()).trim();	
				}
				else if (text.startsWith("CODEN:")) {
					CODEN = text.substring("CODEN:".length()).trim();		
				}
				else if (text.startsWith("ISSN:")) {
					ISSN = text.substring("ISSN:".length()).trim();		
				}
				else if (text.startsWith("E-ISSN:")) {
					EISSN = text.substring("E-ISSN:".length()).trim();	
				}
				else if (text.startsWith("ISBN-10:")) {
					ISBN10 = text.substring("ISBN-10:".length()).trim();	
				}
				else if (text.startsWith("ISBN-13:")) {
					ISBN13 = text.substring("ISBN-13:".length()).trim();	
				}
				else if (text.startsWith("Document type:")) {
					DocumentType = text.substring("Document type:".length()).trim();	
				}
				else if (text.startsWith("DOI:")) {
					DOI = text.substring("DOI:".length()).trim();	
				}
				else if (text.startsWith("Controlled terms:")) {
					ControlledTerms = text.substring("Controlled terms:".length()).trim();	
				}
				else if (text.startsWith("Uncontrolled terms:")) {
					UncontrolledTterms = text.substring("Uncontrolled terms:".length()).trim();	
				}
				else if (text.startsWith("Main heading:")) {
					MainHeading = text.substring("Main heading:".length()).trim();	
				}
				else if (text.startsWith("Pages:")) {
					Pages = text.substring("Pages:".length()).trim();	
				}
				else if (text.startsWith("Conference location:")) {
					ConferenceLocation = text.substring("Conference location:".length()).trim();	
				}
				else if (text.startsWith("Issue date:")) {
					IssueDate = text.substring("Issue date:".length()).trim();	
				}
				else if (text.startsWith("Conference date:")) {
					ConferenceDate = text.substring("Conference date:".length()).trim();	
				}
				else if (text.startsWith("Sponsor:")) {
					Sponsor = text.substring("Sponsor:".length()).trim();	
				}
				else if (text.startsWith("Publisher:")) {
					Publisher = text.substring("Publisher:".length()).trim();	
				}
				else if (text.startsWith("Classification code:")) {
					ClassificationCode = text.substring("Classification code:".length()).trim();	
				}
				else if (text.startsWith("Conference code:")) {
					ConferenceCode = text.substring("Conference code:".length()).trim();	
				}
				else if (text.startsWith("Language:")) {
					ConferenceCode = text.substring("Language:".length()).trim();	
				}
				else if (text.startsWith("Publication year:")) {
					PublicationYear = text.substring("Publication year:".length()).trim();	
				}
			}
	    	
			if (rawid.length() < 1 || AccessionNumber.length() < 1 || Title.length() < 0) {
				context.getCounter("map", "someone null").increment(1);
				return;
			}
	    	
			XXXXObject xObj = new XXXXObject();
			xObj.data.put("rawid", rawid);
			xObj.data.put("AccessionNumber", AccessionNumber);
			xObj.data.put("Title", Title);
			xObj.data.put("Authors", Authors);
			xObj.data.put("AuthorAffiliation", AuthorAffiliation);
			xObj.data.put("CorrAuthorAffiliation", CorrAuthorAffiliation);
			xObj.data.put("CorrespondingAuthor", CorrespondingAuthor);
			xObj.data.put("SourceTitle", SourceTitle);
			xObj.data.put("Volume", Volume);
			xObj.data.put("Issue", Issue);
			xObj.data.put("Abstract", Abstract);
			xObj.data.put("CODEN", CODEN);
			xObj.data.put("ISSN", ISSN);
			xObj.data.put("EISSN", EISSN);
			xObj.data.put("ISBN10", ISBN10);
			xObj.data.put("ISBN13", ISBN13);
			xObj.data.put("DocumentType", DocumentType);
			xObj.data.put("DOI", DOI);
			xObj.data.put("ControlledTerms", ControlledTerms);
			xObj.data.put("UncontrolledTterms", UncontrolledTterms);
			xObj.data.put("MainHeading", MainHeading);
			xObj.data.put("Pages", Pages);
			xObj.data.put("ConferenceLocation", ConferenceLocation);
			xObj.data.put("IssueDate", IssueDate);
			xObj.data.put("ConferenceDate", ConferenceDate);
			xObj.data.put("Sponsor", Sponsor);
			xObj.data.put("Publisher", Publisher);
			xObj.data.put("PublicationYear", PublicationYear);
			
			xObj.data.put("ClassificationCode", ClassificationCode);
			xObj.data.put("ConferenceCode", ConferenceCode);
			xObj.data.put("Language", Language);
			
			context.getCounter("map", "out").increment(1);
			
			byte[] bytes = VipcloudUtil.SerializeObject(xObj);
			context.write(new Text(AccessionNumber), new BytesWritable(bytes));			
		}
	}
	

	public static class ProcessReducer extends
		Reducer<Text, BytesWritable, Text, BytesWritable> {
		public void reduce(Text key, Iterable<BytesWritable> values, 
		                   Context context
		                   ) throws IOException, InterruptedException {
			
			BytesWritable bOut = new BytesWritable();	//用于最后输出
			for (BytesWritable item : values) {
				if (item.getLength() > bOut.getLength()) {	//选最大的一个
					bOut.set(item.getBytes(), 0, item.getLength());
				}
			}
			
			context.getCounter("reduce", "count").increment(1);			
			
			bOut.setCapacity(bOut.getLength()); 	//将buffer设为实际长度
		
			context.write(key, bOut);
		}
	}
}
