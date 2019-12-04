package simple.jobstream.mapreduce.site.pkuyeecaselaw;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/pkuyeelaw/case/big_json/20181207";
		String rawXXXXObjectDir = "/RawData/pkuyeelaw/case/XXXXObject";
		String latest_tempDir = "/RawData/pkuyeelaw/case/latest_temp";	//临时成品目录
		String latestDir = "/RawData/pkuyeelaw/case/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/pkuyeelaw/case/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/pkuyeelaw/case/new_data/StdLawyee";

		JobNode Html2XXXXObject = new JobNode("LawyeeCase", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.pkuyeecaselaw.JSON2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("LawyeeCase", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkuyeecaselaw.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("LawyeeCase", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkuyeecaselaw.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("LawyeeCase", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.pkuyeecaselaw.StdZTCASE");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("LawyeeCase", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkuyeecaselaw.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		/*/
		
		//导出数据到db3
		StdDb3.setConfig("inputHdfsPath", latestDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		result.add(StdDb3);
		//*/
		

		
		return result;
	}
}
