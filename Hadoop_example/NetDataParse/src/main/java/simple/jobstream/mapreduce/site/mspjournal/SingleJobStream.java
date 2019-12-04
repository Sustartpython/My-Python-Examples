package simple.jobstream.mapreduce.site.mspjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/mspjournal/big_json/2019/20190404";
		String rawXXXXObjectDir = "/RawData/mspjournal/XXXXObject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/mspjournal/latest_temp";	// 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/mspjournal/latest";	// 将latest_tempDir数据保存到该目录，以备下次更新使用
		String newXXXXObjectDir = "/RawData/mspjournal/new_data/XXXXObject";		// 去重后得到的新数据
		String stdZhituDir = "/RawData/mspjournal/new_data/StdMspjournal4zhitu";  // 新数据转换为DB3格式存放目录
		String stdNewDir = "/RawData/mspjournal/new_data/StdMspjournal4New";  // 新数据转换为DB3格式存放目录

		// 新数据解析后的XXXXObject格式数据路径
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("mspjournal.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.mspjournal.Json2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				10);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("mspjournal.Merge", 
				rawXXXXObjectDir, latestDir, latest_tempDir, 100);
		
		// 生成新数据
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("mspjournal.Extract",
				rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 100);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("mspjournal.Copy", latest_tempDir, latestDir);

		
		// 导出数据到 db3 智图
		JobNode StdZhituDb3 = JobNodeModel.getJobNode4Std2Db3("mspjournal.Std","simple.jobstream.mapreduce.site.mspjournal.StdMspjournal4zhitu", 
				newXXXXObjectDir, stdZhituDir, "mspjournal","/RawData/_rel_file/zt_template.db3",1);
		
		// 导出数据到 新字段
		JobNode StdNewDb3 = JobNodeModel.getJobNode4Std2Db3("mspjournal.Std","simple.jobstream.mapreduce.site.mspjournal.StdMspjournal4New", 
				newXXXXObjectDir, stdNewDir, "base_obj_meta_a_qk.msp","/RawData/_rel_file/base_obj_meta_a_template_qk.db3",1);
		
		//*
		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdZhituDb3);
		StdZhituDb3.addChildJob(StdNewDb3);
		StdNewDb3.addChildJob(Temp2Latest);
//		StdZhituDb3.addChildJob(Temp2Latest);
		
		// 测试
//		result.add(StdDb3);
		
		return result;
	}
}
