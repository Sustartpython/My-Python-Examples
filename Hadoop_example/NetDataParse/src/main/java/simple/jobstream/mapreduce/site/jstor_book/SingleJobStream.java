package simple.jobstream.mapreduce.site.jstor_book;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/jstor/book/big_json/2019/20190627";
		String rawXXXXObjectDir = "/RawData/jstor/book/XXXXObject";
		String latest_tempDir = "/RawData/jstor/book/latest_temp";	//临时成品目录
		String latestDir = "/RawData/jstor/book/latest";	//成品目录
		String newXobjDir = "/RawData/jstor/book/new_data";	
		String db3Dir = "/RawData/jstor/book/StdBk";
		
		/**/
		JobNode html2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject(
				"JstorBKJsonParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.jstor_book.Json2XXXXObject",
				rawHtmlDir,
				rawXXXXObjectDir,
				20);
		
		
		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("jstorbook.MergeXXXXObject2Temp",
				rawXXXXObjectDir, latestDir, latest_tempDir, 200);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("jstorbook.GenNewData", 
				rawXXXXObjectDir, latest_tempDir, newXobjDir, 200);	
		
		JobNode Std2Db3 = JobNodeModel.getJobNode4Std2Db3("jstorbook.StdDb3",
				"simple.jobstream.mapreduce.site.jstor_book.Std2Db3", 
				newXobjDir, 
				db3Dir, 
				"jstorbook",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("jstorbook.Temp2Latest", 
				latest_tempDir, latestDir);
		
		
		
		//正常更新
		result.add(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2Db3);
		Std2Db3.addChildJob(Temp2Latest);
		
//		result.add(Std2Db3);
//		Std2Db3.addChildJob(Temp2Latest);
		
		return result;
	}
	
}
