package simple.jobstream.mapreduce.site.intlpressjournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		

		
		String rawDataDir = "/RawData/intlpressjournal/big_json/2019/20190716"; // 带解析新数据路径
		String rawXXXXObjectDir = "/RawData/intlpressjournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/intlpressjournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/intlpressjournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String newXXXXObjectDir = "/RawData/intlpressjournal/new_data/xxxxobject"; // 去重后得到的新数据
		String db3Dir = "/RawData/intlpressjournal/new_data/std2db3"; // 新数据转换为DB3格式存放目录
		String a_db3Dir = "/RawData/intlpressjournal/new_data/a_std2db3"; //a表输出目录
		
		
		JobNode json2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"intlpressjournal.json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.intlpressjournal.Json2XXXXObject",
				rawDataDir,
				rawXXXXObjectDir,
				100);
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"intlpressjournal.Merge", 
				rawXXXXObjectDir, 
				latestDir, 
				latest_tempDir,
				100);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"intlpressjournal.GenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
		JobNode StdDb3A = JobNodeModel.getJobNode4Std2Db3(
				"intlpressjournal.StdDb3A",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A", 
				newXXXXObjectDir, 
				a_db3Dir, 
				"a_intlpressjournal",
				"/RawData/_rel_file/base_obj_meta_a_template_qkwx.db3",
				1);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3(
				"intlpressjournal.stdDb3",
				"simple.jobstream.mapreduce.site.intlpressjournal.Std2Db3", 
				newXXXXObjectDir, 
				db3Dir, 
				"zt_intlpressjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"intlpressjournal.Copy.Temp2Latest",
				latest_tempDir,
				latestDir);
		
		result.add(json2XXXXObject);
		json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3A);
		StdDb3A.addChildJob(StdDb3);
		
//		result.add(StdDb3);
		
		
		return result;
	}
}
