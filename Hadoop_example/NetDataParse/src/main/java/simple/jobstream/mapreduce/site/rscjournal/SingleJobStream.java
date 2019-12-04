package simple.jobstream.mapreduce.site.rscjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/rsc/rscjournal/big_json/2019/20190620";
		String rawXXXXObjectDir = "/RawData/rsc/rscjournal/XXXXObject";
		String latest_tempDir = "/RawData/rsc/rscjournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/rsc/rscjournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/rsc/rscjournal/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/rsc/rscjournal/new_data/StdRsc";

		JobNode Json2XXXXObject = new JobNode("RscJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.rscjournal.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("RscJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.rscjournal.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("RscJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.rscjournal.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("RscJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.rscjournal.StdRsc");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("RscJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.rscjournal.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
		JobNode StdNew = new JobNode("RscJournal", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.rscjournal.StdRsc");	
		StdNew.setConfig("inputHdfsPath", latestDir);
		StdNew.setConfig("outputHdfsPath", stdDir);
		
		JobNode First2Latest = new JobNode("RscJournal", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.rscjournal.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
//		result.addChildJob(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
