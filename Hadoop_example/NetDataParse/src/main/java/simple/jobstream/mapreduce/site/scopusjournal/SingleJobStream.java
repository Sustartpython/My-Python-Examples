package simple.jobstream.mapreduce.site.scopusjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;


public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/elsevier/scopus/big_json/2019/20190527";
		String rawXXXXObjectDir = "/RawData/elsevier/scopus/rawXXXXObject";  
		String XXXXObjectDir = "/RawData/elsevier/scopus/XXXXObject";  
		String latest_tempDir = "/RawData/elsevier/scopus/latest_temp"; 
		String latestDir = "/RawData/elsevier/scopus/latest";	 
		String latest_temp_rawDir = "/RawData/elsevier/scopus/latest_temp_raw"; 
		String lates_rawDir = "/RawData/elsevier/scopus/latest_raw";  
		String newXXXXObjectDir = "/RawData/elsevier/scopus/new_data/XXXXObject";		// 去重后得到的新数据
		String stdZhituDir = "/RawData/elsevier/scopus/new_data/StdScopusjournal4zhitu";  // 新数据转换为DB3格式存放目录
		String stdNewDir = "/RawData/elsevier/scopus/new_data/StdScopusjournal4New";  // 新数据转换为DB3格式存放目录
//
//		// 新数据解析后的XXXXObject格式数据路径
//		JobNode Json2RawXXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("scopus.Json2RawXXXXObject", 
//				DateTimeHelper.getNowTimeAsBatch(),
//				"simple.jobstream.mapreduce.site.scopusjournal.Json2RawXXXXObject", 
//				rawHtmlDir, 
//				rawXXXXObjectDir, 
//				200);
//		
//		//合并raw数据
//		JobNode MergeRawXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("scopus.RawMerge", 
//				rawXXXXObjectDir, lates_rawDir, latest_temp_rawDir, 400);
//		
//		// 备份Raw数据
//		JobNode RawTemp2LatestRaw = JobNodeModel.getJobNode4CopyXXXXObject("scopus.RawTemp2LatestRaw", latest_temp_rawDir, lates_rawDir);
//		
		// 将RawObj转成XXObj
		JobNode RawObj2XXObj = JobNodeModel.getJobNode4Parse2XXXXObject("scopus.RawObj2XXObj",  
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.scopusjournal.RawObj2XXXXObject", 
				rawXXXXObjectDir, 
				XXXXObjectDir, 
				200); 
		
//		//合并A层数据
//		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("scopus.Merge", 
//				XXXXObjectDir, latestDir, latest_tempDir, 400);
//		
//		// 生成A层新数据
//		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("scopus.Extract",
//				XXXXObjectDir, latest_tempDir, newXXXXObjectDir, 200);
//
//		// 备份A层数据
//		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("scopus.Copy", XXXXObjectDir, latestDir); 
// 	
//		// 导出数据到 db3 智图
//		JobNode StdZhituDb3 = JobNodeModel.getJobNode4Std2Db3("scopus.StdZhitu","simple.jobstream.mapreduce.site.scopusjournal.StdScopusjournal4zhitu", 
//				newXXXXObjectDir, stdZhituDir, "scopus","/RawData/_rel_file/zt_template.db3",100);
//		
////		// 导出数据到 新字段
//		JobNode StdNewDb3 = JobNodeModel.getJobNode4Std2Db3("scopus.StdNewDb3","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
//				newXXXXObjectDir, stdNewDir, "scopus","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",100);
		
//		// 导出数据到 新字段
//		JobNode StdNewDb3 = JobNodeModel.getJobNode4Std2Db3("scopusjournal.Std","simple.jobstream.mapreduce.common.vip.Std2Db3A", 
//				rawXXXXObjectDir, stdNewDir, "scopusjournal","/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",100);
		
		//*
		// 正常更新
//		result.add(Json2RawXXXXObject); 
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdZhituDb3);
//		GenNewData.addChildJob(StdNewDb3);
		//GenNewData.addChildJob(Temp2Latest);
//		Temp2Latest.addChildJob(LatestRaw2LatestA);
		
		//测试
		
//		RawObj2XXObj.addChildJob(Temp2Latest); 
		
	  JobNode getid =new JobNode("test", defaultRootDir, 0,  "simple.jobstream.mapreduce.site.scopusjournal.RawObj2XXXXObject3");  
	  getid.setConfig("inputHdfsPath", rawXXXXObjectDir); 
	  getid.setConfig("outputHdfsPath",XXXXObjectDir);
	  result.add(getid);
	  return result;
	}
}
