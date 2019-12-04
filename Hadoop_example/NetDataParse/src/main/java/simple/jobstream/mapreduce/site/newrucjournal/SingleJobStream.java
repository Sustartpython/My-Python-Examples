package simple.jobstream.mapreduce.site.newrucjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String rawjsonDir = "/RawData/ruc/rdfybk/big_json/20190614";
		String rawXXXXObjectDir = "/RawData/ruc/rdfybk/xxxxobject";
		String latest_tempDir_all_1 = "/RawData/ruc/rdfybk/latest_temp_all_1"; //合并
		String latest_tempDir_all = "/RawData/ruc/rdfybk/latest_temp_all";	//临时成品目录
		String latest_tempDir = "/RawData/ruc/rdfybk/latest_temp";
		String latestDir_all = "/RawData/ruc/rdfybk/latest_raw";	//成品目录
		String newXXXXObjectDir = "/RawData/ruc/rdfybk/new_data/XXXXObject";		//本次新增的数据（XXXXObject）
		String stdDir = "/RawData/ruc/rdfybk/new_data/StdQk";
		String stdDirAtable = "/RawData/ruc/rdfybk/new_data/StdQkA";
		String latestDir = "/RawData/ruc/rdfybk/latest"; // a表目录
		
		String latestDir_old = "/RawData/ruc/exuezhe/latest_all";

		JobNode Html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"rdfybkjournalhtml2xxxxobject",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.newrucjournal.Json2XXXXObject",
				rawjsonDir,
				rawXXXXObjectDir,
				100);

		//将历史累积数据和新数据合并去重
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"rdfybkjournalMergeXXXXObj", 
				rawXXXXObjectDir, 
				latestDir_all, 
				latest_tempDir_all_1,
				100);
		
		JobNode MergeXXXXObject2Temp2  = JobNodeModel.getJonNode4MergeXXXXObject(
				"rdfybkjournalMergeXXXXObj2", 
				latest_tempDir_all_1, 
				latestDir_old, 
				latest_tempDir_all,
				100);
		
		JobNode TempClear =JobNodeModel.getJobNode4Parse2XXXXObject(
				"TempClear",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.newrucjournal.Temp2LatestClear",
				latest_tempDir_all,
				latest_tempDir,
				100);
		
		
		//生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"rdfybkjournalGenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
		JobNode StdDb3ZT = JobNodeModel.getJobNode4Std2Db3(
				"rdfybkjournalstdDb3",
				"simple.jobstream.mapreduce.site.newrucjournal.StdZT", 
				newXXXXObjectDir, 
				stdDir, 
				"rdfybkjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"rdfybkjournal.Temp2Latest",
				latest_tempDir,
				latestDir_all);
		
		JobNode XXXXobject2Atable =JobNodeModel.getJobNode4Parse2XXXXObject(
				"rdfybkjournaltoatable",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.newrucjournal.Last2A",
				latestDir_all,
				latestDir,
				100);
		
		
		JobNode StdDb3A = JobNodeModel.getJobNode4Std2Db3(
				"rdfybkjournal.StdDb3A",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				latestDir, 
				stdDirAtable,
				"rdfybkjournal",
				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",
				1);
		
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(MergeXXXXObject2Temp2);
		MergeXXXXObject2Temp2.addChildJob(TempClear);
		TempClear.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3ZT);
		StdDb3ZT.addChildJob(Temp2Latest);
		Temp2Latest.addChildJob(XXXXobject2Atable);
		XXXXobject2Atable.addChildJob(StdDb3A);
		
//		XXXXobject2Atable.addChildJob(StdDb3A);
//		result.add(StdDb3ZT);
		return result;
		
	}

}
