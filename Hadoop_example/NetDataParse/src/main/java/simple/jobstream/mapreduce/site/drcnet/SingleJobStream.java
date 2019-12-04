package simple.jobstream.mapreduce.site.drcnet;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/drcnet/big_json/2019/20190621";
		String rawXXXXObjectDir = "/RawData/drcnet/XXXXObject";
		String latest_tempDir = "/RawData/drcnet/latest_temp";	//临时成品目录
		String latestDir = "/RawData/drcnet/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/drcnet/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/drcnet/new_data/Stddrcnet";

//		JobNode Json2XXXXObject = new JobNode("drcnetbook", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.drcnet.Json2XXXXObject");		
//		Json2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("drcnet.parse", DateTimeHelper.getNowTimeAsBatch(), 
				"simple.jobstream.mapreduce.site.drcnet.Json2XXXXObject", rawHtmlDir, rawXXXXObjectDir, 10);
		
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("drcnet.Merge", rawXXXXObjectDir, latestDir, latest_tempDir, 10);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("drcnet.Extract", rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 10);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("drcnet.Copy", latest_tempDir, latestDir);

		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3("drcnet.Std","simple.jobstream.mapreduce.site.drcnet.StdDrcnet", 
				newXXXXObjectDir, stdDir, "drcnetinfo","/RawData/_rel_file/zt_template.db3",1);
		
		JobNode StdNew = JobNodeModel.getJobNode4Std2Db3("drcnet.Std","simple.jobstream.mapreduce.site.drcnet.StdDrcnet", 
				rawXXXXObjectDir, stdDir, "drcnetinfo","/RawData/_rel_file/zt_template.db3",1);
								
		
		JobNode First2Latest = JobNodeModel.getJobNode4CopyXXXXObject("drcnet.Copy", rawXXXXObjectDir, latestDir);

		
		//*
		//正常更新
		
		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdNew);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		result.add(First2Latest);
//		Json2XXXXObject.addChildJob(StdNew);
		
		
		return result;
	}
}
