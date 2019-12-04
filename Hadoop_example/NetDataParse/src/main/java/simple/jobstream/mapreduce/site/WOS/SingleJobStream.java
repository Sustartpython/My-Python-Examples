package simple.jobstream.mapreduce.site.WOS;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";

		String rawJsonDir = "/RawData/WoS/WoS_Core_Collection/detail/big_json/2019/20190621";
		String rawXXXXObjectDir = "/RawData/WoS/WoS_Core_Collection/detail/XXXXObject";		//新到json数据转换的BXXXXObject
		String latestTempDir = "/RawData/WoS/WoS_Core_Collection/detail/latest_temp";	//临时成品目录
		String newXXXXObjectDir = "/RawData/WoS/WoS_Core_Collection/detail/new_data/XXXXObject";
		String stdDir = "/RawData/WoS/WoS_Core_Collection/detail/new_data/Std2Db3";
		String newXobjDir = "/RawData/WoS/WoS_Core_Collection/detail/new_data";
		String updateXobjDir = "/RawData/WoS/WoS_Core_Collection/detail/update_data";
		String latestDir = "/RawData/WoS/WoS_Core_Collection/detail/latest_raw";	//成品目录
		String newStdDir = "/RawData/WoS/WoS_Core_Collection/detail/new_data_db3";		//本次新增的数据（题录db3目录）
		String updateStdDir = "/RawData/WoS/WoS_Core_Collection/detail/update_data_db3";		//本次更新数据（题录db3目录）
		String db3Dir = "/RawData/WoS/WoS_Core_Collection/detail/db3";
		String newStdREFDir = "/RawData/WoS/WoS_Core_Collection/detail/new_data_ref_db3";		//本次新增的数据（引文db3目录）
		
		/* 将新到的json数据转换为 BXXXXObject*/
		JobNode Json2XXXXObject = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.Json2XXXXObject");
		Json2XXXXObject.setConfig("inputHdfsPath", rawJsonDir);
		Json2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.MergeXXXXObject2Temp");
		MergeXXXXObject2Temp.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + latestDir);
		MergeXXXXObject2Temp.setConfig("outputHdfsPath", latestTempDir);
		MergeXXXXObject2Temp.setConfig("newDataDir", newXobjDir);
		MergeXXXXObject2Temp.setConfig("updateDataDir", updateXobjDir);
		
//		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("WOSParse.Merge", rawXXXXObjectDir, latestDir, latestTempDir, 400);
		
		JobNode GenNewData = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.GenNewData");
		GenNewData.setConfig("inputHdfsPath", latestTempDir + "," + rawXXXXObjectDir);
		GenNewData.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		//生成题录db3（新数据）
//		JobNode New2Db3 = new JobNode("WOSParse", defaultRootDir, 0, 
//				"simple.jobstream.mapreduce.site.WOS.New2Db3");
//		New2Db3.setConfig("inputHdfsPath", newXXXXObjectDir);
//		New2Db3.setConfig("outputHdfsPath", stdDir);
		
		JobNode New2Db3 = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.New2Db3");
		New2Db3.setConfig("inputHdfsPath", newXXXXObjectDir);
		New2Db3.setConfig("outputHdfsPath", stdDir);
		
		//备份累积数据
		JobNode Temp2Latest = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.Temp2Latest");
		Temp2Latest.setConfig("inputHdfsPath", latestTempDir);
		Temp2Latest.setConfig("outputHdfsPath", latestDir);
		
		/* 正常更新 （区分new和update）
		result.add(Json2XXXXObject);	//将新到的json数据转换为 BXXXXObject
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);	//将历史累积数据和新数据合并去重
		MergeXXXXObject2Temp.addChildJob(New2Db3);	//不能是“总数据-老数据”，新数据要全刷一遍。
		New2Db3.addChildJob(Update2Db3);			//生成题录db3
		Update2Db3.addChildJob(Temp2Latest);	//备份累积数据
		//*/
		
		/* 正常更新 （不区分new和update）*/
//		result.add(MergeXXXXObject2Temp);	//将新到的json数据转换为 BXXXXObject
//		MergeXXXXObject2Temp.addChildJob(GenNewData);	//将历史累积数据和新数据合并去重
//		GenNewData.addChildJob(New2Db3);	
//		Std2Db3.addChildJob(Temp2Latest);	//备份累积数据
		
		
//		Std2Db3.setConfig("inputHdfsPath", latestDir);
		
		
		JobNode New2TXT = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.New2TXT");
		New2TXT.setConfig("inputHdfsPath", latestDir);
		New2TXT.setConfig("outputHdfsPath", "/user/lqx/output");
		
		JobNode New2Univ = new JobNode("WOSParse", defaultRootDir, 0, 
		"simple.jobstream.mapreduce.site.WOS.New2Univ");
		New2Univ.setConfig("inputHdfsPath", latestDir);
		New2Univ.setConfig("outputHdfsPath", updateStdDir);
		
		JobNode New2018 = new JobNode("WOSParse", defaultRootDir, 0, 
		"simple.jobstream.mapreduce.site.WOS.New2018");
		New2018.setConfig("inputHdfsPath", latestDir);
		New2018.setConfig("outputHdfsPath", updateStdDir);
		
		JobNode NewTXTC = new JobNode("WOSParse", defaultRootDir, 0, 
		"simple.jobstream.mapreduce.site.WOS.New2count");
		NewTXTC.setConfig("inputHdfsPath", latestDir);
		NewTXTC.setConfig("outputHdfsPath", "/user/lqx/output");
		
		JobNode New2Final = new JobNode("WOSParse", defaultRootDir, 0, 
		"simple.jobstream.mapreduce.site.WOS.New2Final");
		New2Final.setConfig("inputHdfsPath", latestDir);
		New2Final.setConfig("outputHdfsPath", updateStdDir);
		
		JobNode Map2XXXXObject = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.Map2XXXXObject");
		Map2XXXXObject.setConfig("inputHdfsPath", latestDir);
		Map2XXXXObject.setConfig("outputHdfsPath", newXXXXObjectDir);
		
		JobNode New2mdb = new JobNode("WOSParse", defaultRootDir, 0, 
		"simple.jobstream.mapreduce.site.WOS.New2mdb");
		New2mdb.setConfig("inputHdfsPath", newXXXXObjectDir);
		New2mdb.setConfig("outputHdfsPath", updateStdDir);
		
		JobNode ER2Db3 = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.ER2Db3");
		ER2Db3.setConfig("inputHdfsPath", latestDir);
		ER2Db3.setConfig("outputHdfsPath", updateStdDir);
		
		JobNode ALL2Db3 = new JobNode("WOSParse", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.WOS.New2Db3as");
		ALL2Db3.setConfig("inputHdfsPath", latestDir);
		ALL2Db3.setConfig("outputHdfsPath", updateStdDir);

		
		
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(New2Db3);
		New2Db3.addChildJob(Temp2Latest);
		
//		result.add(ALL2Db3);
//		Temp2Latest.addChildJob(New2Univ);
		
		
//		Json2XXXXObject.addChildJob(New2Db3);	//备份累积数据
		
		return result;
	}
}
