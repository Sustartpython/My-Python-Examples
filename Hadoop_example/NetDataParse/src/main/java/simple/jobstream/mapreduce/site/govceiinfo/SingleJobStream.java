package simple.jobstream.mapreduce.site.govceiinfo;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		 
		String rawHtmlDir = "/RawData/ceigov/big_json/2019/20190415";
		String rawXXXXObjectDir = "/RawData/ceigov/XXXXObject";
		String latest_tempDir = "/RawData/ceigov/latest_temp";	//临时成品目录
		String latestDir = "/RawData/ceigov/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/ceigov/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/ceigov/new_data/StdCeigov";

//		JobNode Json2XXXXObject = new JobNode("govceiinfo", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.govceiinfo.Json2XXXXObject");		
//		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("govceiinfo.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.govceiinfo.Json2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				5);
		
		//将历史累积数据和新数据合并去重
//		JobNode MergeXXXXObject2Temp  = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.MergeXXXXObject2Temp");
//		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
//		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
//		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("govceiinfo.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 100);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("govceiinfo.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 100);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("govceiinfo.Copy", latest_tempDir, latestDir);
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("PkuLawCase", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.pkulawcase.GenNewData");
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
//		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("govceiinfo.Std","simple.jobstream.mapreduce.site.govceiinfo.StdGovceiinfo4zhitu", 
				newXXXXObjectDir, stdDir, "govceiinfo","/RawData/_rel_file/zt_template.db3",1);
//		
//		//备份累积数据
//		JobNode First2Latest = new JobNode("cei_gov", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.cei_gov.Temp2Latest");
//		First2Latest.setConfig("inputHdfsPath", latest_tempDir);
//		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		
	
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("govceiinfo.Std","simple.jobstream.mapreduce.site.govceiinfo.StdGovceiinfo4zhitu", 
				rawXXXXObjectDir, stdDir, "govceiinfo","/RawData/_rel_file/zt_template.db3",1);
		
		
		
		JobNode First2Latest = new JobNode("govceiinfo", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.govceiinfo.Temp2Latest");
		First2Latest.setConfig("inputHdfsPath", rawXXXXObjectDir);
		First2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
		
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		Json2XXXXObject.addChildJob(StdNew);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
//		result.add(StdNew);
		
		
		
		return result;
	}
}
