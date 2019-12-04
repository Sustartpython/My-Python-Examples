package simple.jobstream.mapreduce.site.plosjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;


public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/plosjournal/big_json/2019/20190419";
		String rawXXXXObjectDir = "/RawData/plosjournal/XXXXObject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/plosjournal/latest_temp";	// 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/plosjournal/latest";	// 将latest_tempDir数据保存到该目录，以备下次更新使用
		String newXXXXObjectDir = "/RawData/plosjournal/new_data/XXXXObject";		// 去重后得到的新数据
		String stdZhituDir = "/RawData/plosjournal/new_data/StdPlosjournal4zhitu";  // 新数据转换为DB3格式存放目录
		String stdNewDir = "/RawData/plosjournal/new_data/StdPlosjournal4New";  // 新数据转换为DB3格式存放目录

		// 新数据解析后的XXXXObject格式数据路径
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("plosjournal.Json2XXXXObject", 
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.plosjournal.Json2XXXXObject", 
				rawHtmlDir, 
				rawXXXXObjectDir, 
				10);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject("plosjournal.Merge", 
				rawXXXXObjectDir, latestDir, latest_tempDir, 100);
		
		// 生成新数据
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("plosjournal.Extract",
				rawXXXXObjectDir, latest_tempDir, newXXXXObjectDir, 100);

		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("plosjournal.Copy", latest_tempDir, latestDir);

		
		// 导出数据到 db3 智图
		JobNode StdZhituDb3 = JobNodeModel.getJobNode4Std2Db3("plosjournal.Std","simple.jobstream.mapreduce.site.plosjournal.StdPlosjournal4zhitu", 
				newXXXXObjectDir, stdZhituDir, "plosjournal","/RawData/_rel_file/zt_template.db3",1);
		
		// 导出数据到 新字段
		JobNode StdNewDb3 = JobNodeModel.getJobNode4Std2Db3("plosjournal.Std","simple.jobstream.mapreduce.site.plosjournal.StdPlosjournal4New", 
				newXXXXObjectDir, stdNewDir, "plosjournal","/RawData/_rel_file/base_obj_meta_a_template_qk.db3",1);
		
		//*
		// 正常更新
		result.add(Json2XXXXObject);
		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdZhituDb3);
		StdZhituDb3.addChildJob(StdNewDb3);
		StdNewDb3.addChildJob(Temp2Latest);
		
		//测试
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(StdNewDb3);
		
		return result;
	}
}
