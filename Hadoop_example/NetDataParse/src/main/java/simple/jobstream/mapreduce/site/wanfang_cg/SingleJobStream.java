package simple.jobstream.mapreduce.site.wanfang_cg;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> WFCgParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/wanfang/cg/big_json/2019/20190712";
		String rawXXXXObjectDir = "/RawData/wanfang/cg/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/cg/latest_temp";	//临时成品目录
		String latestDir = "/RawData/wanfang/cg/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/wanfang/cg/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/cg/new_data/cgdb3";

//		JobNode Html2XXXXObject = new JobNode("WFCgData", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.wanfang_cg.JSON2XXXXObject");		
//		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
//		//将历史累积数据和新数据合并去重
//		JobNode MergeXXXXObject2Temp  = new JobNode("WFCgData", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.wanfang_cg.MergeXXXXObject2Temp");
//		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
//		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
//		
//		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
//		JobNode GenNewData = new JobNode("WFCgData", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.wanfang_cg.GenNewData");
//		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
//		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
//		
//		JobNode StdDb3 = new JobNode("WFCgData", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.wanfang_cg.StdZLFCG");	
//		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
//		StdDb3.setConfig("outputHdfsPath", stdDir);
//		
//		//备份累积数据
//		JobNode Temp2Latest = new JobNode("WFCgData", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.wanfang_cg.Temp2Latest");
//		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
//		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		JobNode Html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"WFCgHtml2XXXObj",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.wanfang_cg.JSON2XXXXObject",
				rawHtmlDir,
				rawXXXXObjectDir,
				100);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"WFCGMergeXXXXObj", 
				rawXXXXObjectDir, 
				latestDir, 
				latest_tempDir,
				100);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"WFCGGenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
				
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3(
				"wanfangstd",
				"simple.jobstream.mapreduce.site.wanfang_cg.StdZLFCG", 
				newXXXXObjectDir, 
				stdDir, 
				"wanfang_zlf_cg",
				"/RawData/wanfang/cg/template/wanfang_cg_template.db3",
				1);
				
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"WFCG.Copy.Temp2Latest",
				latest_tempDir,
				latestDir);
	
		//*
		//正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);

		
//		result.add(StdDb3);
		
//		JobNode StdDb3_1 = JobNodeModel.getJobNode4Std2Db3(
//				"wanfangstd",
//				"simple.jobstream.mapreduce.site.wanfang_cg.StdZLFCG", 
//				latestDir, 
//				stdDir, 
//				"wanfang_zlf_cg",
//				"/RawData/wanfang/cg/template/wanfang_cg_template.db3",
//				3);
//		result.add(StdDb3_1);
		


		
		return result;
	}
	
	
}
