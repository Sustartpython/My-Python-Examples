package simple.jobstream.mapreduce.site.pkulawlaw;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/pkulaw/law/big_htm/20181207";
		String rawXXXXObjectDir = "/RawData/pkulaw/law/XXXXObject";
		String latest_tempDir = "/RawData/pkulaw/law/latest_temp";	//临时成品目录
		String latestDir = "/RawData/pkulaw/law/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/pkulaw/law/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/pkulaw/law/new_data/StdPkuLaw";

		JobNode Html2XXXXObject = new JobNode("PkuLawData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.pkulawlaw.Html2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("PkuLawData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkulawlaw.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("PkuLawData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkulawlaw.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("PkuLawData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.pkulawlaw.StdZTFG");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		JobNode StdAll = new JobNode("PkuLawData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.pkulawlaw.StdZTFG");	
		StdAll.setConfig("inputHdfsPath", latestDir);
		StdAll.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("PkuLawData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.pkulawlaw.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);
		
		/*/
		
		//导出数据到db3
		StdDb3.setConfig("inputHdfsPath", latestDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		result.add(StdDb3);
		//*/
		result.add(StdAll);
		

		
		return result;
	}
}
