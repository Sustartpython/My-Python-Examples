package simple.jobstream.mapreduce.site.bookan_hp;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream()
	{
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/bookan/gaoxiao/big_htm/2019/20190612";
		String rawXXXXObjectDir = "/RawData/bookan/gaoxiao/XXXXObject";
		String latest_tempDir = "/RawData/bookan/gaoxiao/latest_temp";	//临时成品目录
		String latestDir = "/RawData/bookan/gaoxiao/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/bookan/gaoxiao/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/bookan/gaoxiao/new_data/StdBookanQk";
		
		String rawHtmlDir1 = "/RawData/bookan/gaoxiao/big_htm/2019/20190220";
		String rawHtmlDir2 = "/RawData/bookan/gaoxiao/big_htm/2019/20190326";
		String rawHtmlDir3 = "/RawData/bookan/gaoxiao/big_htm/2019/20190328";
		
		String allrawHtml = rawHtmlDir2 +","+rawHtmlDir3; // rawHtmlDir1+","+
		
		
		JobNode Html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"BookanQkHtml2XXXObj",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.bookan_hp.BookanHtml2XXXXObject",
				rawHtmlDir,
				rawXXXXObjectDir,
				100);

		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"BookanQkMergeXXXXObj", 
				rawXXXXObjectDir, 
				latestDir, 
				latest_tempDir,
				100);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"BookanQkGenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3(
				"BookanQkstdDb3",
				"simple.jobstream.mapreduce.site.bookan_hp.StdZTQK", 
				newXXXXObjectDir, 
				stdDir, 
				"ctgubookanjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"BookanQk.Temp2Latest",
				latest_tempDir,
				latestDir);
		
		
		//*正常更新
		
//		result.add(Html2XXXXObject);
//		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob(StdDb3);
//		StdDb3.addChildJob(Temp2Latest);
		
//		result.add(StdDb3);
		
		JobNode StdDb3latest = JobNodeModel.getJobNode4Std2Db3(
				"BookanQkstdDb3",
				"simple.jobstream.mapreduce.site.bookan_hp.StdZTQK", 
				latestDir, 
				stdDir, 
				"ctgubookanjournal",
				"/user/xujiang/zt_template_updatebeginpage.db3",
				1);
		
		
//		result.add(StdDb3latest);
		
		//StdDb3.addChildJob(Temp2Latest);
		
		JobNode zhuanhuan =JobNodeModel.getJobNode4Parse2XXXXObject(
				"zhuanhuan",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.bookan_hp.zhuanhuan",
				latestDir,
				latest_tempDir,
				100);

//		result.add(zhuanhuan);
//		zhuanhuan.addChildJob(Temp2Latest);
		
		JobNode StdDb3update = JobNodeModel.getJobNode4Std2Db3(
				"BookanQkstdDb3",
				"simple.jobstream.mapreduce.site.bookan_hp.StdZTQK", 
				latestDir, 
				stdDir, 
				"ctgubookanjournal",
				"/RawData/bookan/gaoxiao/template/zt_template_update.db3",
				1);
		
		result.add(StdDb3update);

		return result;
	}
	
}
