package simple.jobstream.mapreduce.site.wanfang_bz;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		// 2019年第一批数据进行改版 之前数据作废
		String rawHtmlDir = "/RawData/wanfang/bz/big_htm/2019/20190703";
		String rawXXXXObjectDir = "/RawData/wanfang/bz/XXXXObject";
		String latest_tempDir = "/RawData/wanfang/bz/latest_temp";	//临时成品目录
		String latestDir = "/RawData/wanfang/bz/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/wanfang/bz/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/wanfang/bz/new_data/StdWFBZ";

		
		JobNode Html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"WFBzHtml2XXXObj",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.wanfang_bz.Html2XXXXObject",
				rawHtmlDir,
				rawXXXXObjectDir,
				100);
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"WFBzMergeXXXXObj", 
				rawXXXXObjectDir, 
				latestDir, 
				latest_tempDir,
				100);
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"WFBzGenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3(
				"WFBzstdDb3",
				"simple.jobstream.mapreduce.site.wanfang_bz.StdZLFBZ", 
				newXXXXObjectDir, 
				stdDir, 
				"zlf_bz",
				"/RawData/wanfang/bz/template/wanfang_bz_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"WFBz.Copy.Temp2Latest",
				latest_tempDir,
				latestDir);

		
		//*
		//正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
		
//		JobNode StdDb3test = JobNodeModel.getJobNode4Std2Db3(
//				"WFBzstdDb3",
//				"simple.jobstream.mapreduce.site.wanfang_bz.StdZLFBZ", 
//				latestDir, 
//				stdDir, 
//				"zlf_bz",
//				"/RawData/wanfang/bz/template/wanfang_bz_template.db3",
//				1);
//		result.add(StdDb3test);
		
		return result;
	}
	
}
