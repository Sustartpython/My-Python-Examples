package simple.jobstream.mapreduce.site.wanfang_cg.wanfang_cg_back;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> WFCgParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/wanfang/cg/big_json/2018/20181226";
		String rawXXXXObjectDir = "/RawData/wanfang/cg/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/cg/latest_temp";	//临时成品目录
		String latestDir = "/RawData/wanfang/cg/latest_back/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/wanfang/cg/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/cg/new_data/cgdb3";

		JobNode Html2XXXXObject = new JobNode("WFCgData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.wanfang_cg.JSON2XXXXObject");		
		Html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("WFCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.wanfang_cg.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = new JobNode("WFCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.wanfang_cg.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latest_tempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode StdDb3 = new JobNode("WFCgData", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.wanfang_cg.StdZLFCG");	
		StdDb3.setConfig("inputHdfsPath", newXXXXObjectDir);
		StdDb3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("WFCgData", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.wanfang_cg.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		//*
		//正常更新
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);

		
		//导出数据到db3
		StdDb3.setConfig("inputHdfsPath", latestDir);
		result.add(StdDb3);

		
		return result;
	}
	
	
}
