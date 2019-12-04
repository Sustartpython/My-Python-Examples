package simple.jobstream.mapreduce.site.science;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/sciencejournal/big_json/2019/20191202";
		String rawXXXXObjectDir = "/RawData/sciencejournal/XXXXObject";
		String latest_tempDir = "/RawData/sciencejournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/sciencejournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/sciencejournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/sciencejournal/new_data/StdScience";

		JobNode Json2XXXXObject = new JobNode("ScienceJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.science.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("ScienceJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.science.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("ScienceJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.science.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("ScienceJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.science.StdScience");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("ScienceJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.science.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		JobNode StdNew = new JobNode("ScienceJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.science.StdScience");	
		StdNew.setConfig("inputHdfsPath", rawXXXXObjectDir);
		StdNew.setConfig("outputHdfsPath", stdDir);
		
		JobNode First2Latest = new JobNode("ScienceJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.science.Json2XXXXObject");
		First2Latest.setConfig("inputHdfsPath", rawHtmlDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);			
		
		return result;
	}
}
