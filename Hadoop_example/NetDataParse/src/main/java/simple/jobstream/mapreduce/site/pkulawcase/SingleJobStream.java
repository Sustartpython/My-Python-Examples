package simple.jobstream.mapreduce.site.pkulawcase;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/pkulaw/case/big_json/20190604";
		String rawXXXXObjectDir = "/RawData/pkulaw/case/XXXXObject";
		String latest_tempDir = "/RawData/pkulaw/case/latest_temp";	//临时成品目录
		String latestDir = "/RawData/pkulaw/case/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/pkulaw/case/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/pkulaw/case/new_data/StdPkuLawCase";
		

		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("pkulawcase.Parse", DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.pkulawcase.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 200);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("pkulawcase.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 200);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("pkulawcase.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 100);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pkulawcase.Copy", latest_tempDir, latestDir);
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("pkulawcase.Copy", rawXXXXObjectDir, latestDir);
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.GenNewData");
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("pkulawcase.Std","simple.jobstream.mapreduce.site.pkulawcase.StdPkuLawCase2", 
				newXXXXObjectDir, stdDir, "pkulawcase","/RawData/_rel_file/zt_template.db3",10);
		
		//备份累积数据
//		JobNode Temp2Latest = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.Temp2Latest");
//		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
//		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		
//		JobNode StdNew = new JobNode("PkuLawCase", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.pkulawcase.StdPkuLawCase");	
//		StdNew.setConfig("inputHdfsPath", rawXXXXObjectDir);
//		StdNew.setConfig("outputHdfsPath", stdDir);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("pkulawcase.Std","simple.jobstream.mapreduce.site.pkulawcase.StdPkuLawCase2", 
				rawXXXXObjectDir, stdDir, "pkulawcase","/RawData/_rel_file/zt_template.db3",50);
		
		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
//		First2Latest.addChildJob(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
		
		
		return result;
	}
}
