package simple.jobstream.mapreduce.site.ieeejournal;

import java.io.IOException;
import java.util.LinkedHashSet;

import com.process.frame.JobStreamImpl;
import com.vipcloud.JobNode.JobNode;

public class IEEESimpleJobStream extends JobStreamImpl
{
	/**
	 * 简单的测试统计用例
	 */
	public LinkedHashSet<JobNode> SimpleJob()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";
		
		JobNode SimpleJob = new JobNode("SimpleJob", defaultRootDir,
				0, "simple.jobstream.mapreduce.chenyong.ieeejournal.Json2XXXXObjectIEEE");		
		SimpleJob.setConfig("inputHdfsPath", "/RawData/CQU/ieee/big_htm/big_json");
		SimpleJob.setConfig("outputHdfsPath", "/RawData/CQU/ieee/XXXXObject");
		result.add(SimpleJob);
		
		return result;
	}
	//IEEE解析
	public LinkedHashSet<JobNode> Parse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/CQU/ieee/big_htm/big_json";//文件输入目录
		String rawXXXXObjectDir = "/RawData/CQU/ieee/XXXXObject";//新XXXXOBj 输出目录
		String latest_tempDir = "/RawData/CQU/ieee/latest_temp";	//新数据合并老数据后生成，新一轮的总数据存放地址
		String latestDir = "/RawData/CQU/ieee/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/CQU/ieee/new_data";//本次新增的数据（XXXXObject）
		String stdhyDir = "/RawData/CQU/ieee/stdieee";//DB3目录
		
		
		//生成新的XXXXOBJ
		JobNode Json2XXXXObject = new JobNode("Parse", defaultRootDir,
				0, "simple.jobstream.mapreduce.chenyong.ieeejournal.JsonXXXXObjectIEEE");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeIEEEXXXXObject2Temp  = new JobNode("Parse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.chenyong.ieeejournal.MergeIEEEXXXXObject2Temp");
		MergeIEEEXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeIEEEXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("Parse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.chenyong.ieeejournal.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdIEEE = new JobNode("Parse", defaultRootDir,
				0, "simple.jobstream.mapreduce.chenyong.ieeejournal.StdIEEE");		
		StdIEEE.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdIEEE.setConfig("outputHdfsPath", stdhyDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("Parse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.chenyong.hy.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		//正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeIEEEXXXXObject2Temp);
		MergeIEEEXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdIEEE);
		StdIEEE.addChildJob(Temp2Latest);
		
		
		
		//导出完整数据	
	/*	StdIEEE.setConfig("inputHdfsPath", latestDir);
		StdIEEE.setConfig("outputHdfsPath", stdhyDir);
		result.add(StdIEEE);
	*/
		
		return result;
	}
	
	@Override
	public void initJobStreamData(String genDataTag) throws IOException
	{
		//this.addSubJobStream("SimpleJob", SimpleJob());
		
		//this.addSubJobStream("wosParse", wosParse());
		
		this.addSubJobStream("SimpleJob", SimpleJob());//代码文件名Parse
		
		//this.addSubJobStream("SDQKParse", SDQKParse());
	}
	
}
