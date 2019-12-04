package simple.jobstream.mapreduce.site.cambridge;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/cambridgejournal/big_json/2019/20190620";
		String rawXXXXObjectDir = "/RawData/cambridgejournal/XXXXObject";
		String latest_tempDir = "/RawData/cambridgejournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/cambridgejournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/cambridgejournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/cambridgejournal/new_data/StdCambridge";

		JobNode Json2XXXXObject = new JobNode("CambridgeJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cambridge.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("CambridgeJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cambridge.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("CambridgeJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cambridge.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("CambridgeJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cambridge.StdCambridge");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("CambridgeJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cambridge.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		JobNode StdNew = new JobNode("CambridgeJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cambridge.StdCambridgebyTxt");	
		StdNew.setConfig("inputHdfsPath", latestDir);
		StdNew.setConfig("outputHdfsPath", stdDir);
		
		JobNode First2Latest = new JobNode("CambridgeJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cambridge.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode Json2Latest = new JobNode("CambridgeJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cambridge.Json2XXXXObject");		
		Json2Latest.setConfig("inputHdfsPath", rawHtmlDir);
		Json2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		
//		result.add(First2Latest);
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
