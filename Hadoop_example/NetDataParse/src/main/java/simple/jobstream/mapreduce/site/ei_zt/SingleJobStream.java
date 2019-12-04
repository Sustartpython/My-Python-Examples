package simple.jobstream.mapreduce.site.ei_zt;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> EIZTParse()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawJsonDir = "/RawData/elsevier/EI/detail/big_json/2019/20190703";
		String rawXXXXObjectDir = "/RawData/elsevier/EI/detail/XXXXObject";
		String latest_tempDir = "/RawData/elsevier/EI/detail/latest_temp";	//临时成品目录
		String latestRawDir = "/RawData/elsevier/EI/detail/latest_raw";	//成品目录
		String latestDir = "/RawData/elsevier/EI/detail/latest";
		String newXobjDir = "/RawData/elsevier/EI/detail/new_data";	
//		String newXXXXObjectDir = "/RawData/EI/detail/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdEIDir = "/RawData/elsevier/EI/detail/db3";
		
		
		
		/**/
		JobNode Json2XXXXObject = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ei_zt.Json2XXXXObject");		
		Json2XXXXObject.setConfig("inputHdfsPath", rawJsonDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.ei_zt.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestRawDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latest_tempDir);

		
		JobNode GenNewData  = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.ei_zt.GenNewData");
		GenNewData.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latest_tempDir);
		GenNewData.setConfig("outputHdfsPath", newXobjDir);
		
		JobNode Std2Db3 = new JobNode("EIJsonParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.ei_zt.Std2Db3");		
		Std2Db3.setConfig("inputHdfsPath", newXobjDir);
		Std2Db3.setConfig("outputHdfsPath", stdEIDir);
		Std2Db3.setConfig("reduceNum", "1");
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("EIJsonParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.ei_zt.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latest_tempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestRawDir);
		

		
		//正常更新（不区分new和update）
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);
		//
		
//		Std2Db3.setConfig("reduceNum", "1");
//		Std2Db3.setConfig("inputHdfsPath", latestRawDir);
//		result.add(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);

		//导出  latest
		
		//备份累积数据
//		JobNode Temp2LatestGetSource = new JobNode("EIJsonParse", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.ei_zt.Temp2LatestGetSource");
//		Temp2LatestGetSource.setConfig("inputHdfsPath", latestRawDir);
//		Temp2LatestGetSource.setConfig("outputHdfsPath", "/RawData/elsevier/EI/detail/test");
//		result.add(Temp2LatestGetSource);
		
		
//		Std2Db3.setConfig("inputHdfsPath", latestRawDir);
//		Std2Db3.setConfig("reduceNum", "1");
//		result.add(Std2Db3);
		
		
		JobNode CNnode = new JobNode("EICN", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.ei_zt.latestCN");
		CNnode.setConfig("inputHdfsPath", latestRawDir);
		CNnode.setConfig("outputHdfsPath", latest_tempDir);
		
		result.add(CNnode);
		CNnode.addChildJob(Temp2Latest);
		return result;
	}
	
}
