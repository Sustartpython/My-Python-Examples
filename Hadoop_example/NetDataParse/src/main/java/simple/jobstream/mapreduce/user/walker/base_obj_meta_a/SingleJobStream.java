package simple.jobstream.mapreduce.user.walker.base_obj_meta_a;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		
		String db3Dir = "/BaseData/base_obj_meta_a/db3/" + DateTimeHelper.getNowDate();		
		String newXobjDir = "/BaseData/base_obj_meta_a/new_xobj";
		String latestDir = "/BaseData/base_obj_meta_a/latest";
		String latestTempDir = "/BaseData/base_obj_meta_a/latest_temp";
		String hfileDir = "/BaseData/base_obj_meta_a/hfile";
		
		// db3 转 XXXXObject
		JobNode Sqlite2XXXXObject = JobNodeModel.getJobNode4Sqlite2XXXXObject("base_obj_meta_a.Sqlite2XXXXObject", 
				"base_obj_meta_a", 
				"lngid", 
				db3Dir, 
				newXobjDir);
		
		// 合并新数据到临时目录
		JobNode MergeXXXXObject2LatestTemp = new JobNode("base_obj_meta_a.MergeXXXXObject2LatestTemp", "", 0,
				"simple.jobstream.mapreduce.user.walker.base_obj_meta_a.MergeXXXXObject");
		MergeXXXXObject2LatestTemp.setConfig("jobName", "base_obj_meta_a.MergeXXXXObject2LatestTemp");		
		MergeXXXXObject2LatestTemp.setConfig("inputHdfsPath", newXobjDir + "," + latestDir);
		MergeXXXXObject2LatestTemp.setConfig("outputHdfsPath", latestTempDir);
		MergeXXXXObject2LatestTemp.setConfig("reduceNum", "400");
		
		// 将临时目录的内容拷贝到 latest
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("base_obj_meta_a.Temp2Latest", 
				latestTempDir, 
				latestDir);
		
		// 将 XXXXObject 转为 HFile 文件
		JobNode XXXXObject2HFile = JobNodeModel.getJobNode4XXXXObject2HFile("base_obj_meta_a.XXXXObject2HFile", 
				latestTempDir, 
				hfileDir, 
				"base_obj_meta_a", 
				"main", 
				400, 
				true);
		
		// 加载 HFile 到 HBase
		JobNode LoadHFile = JobNodeModel.getJobNode4LoadHFile("base_obj_meta_a.LoadHFile", 
				hfileDir, 
				"base_obj_meta_a", 
				"main");
		
		result.add(Sqlite2XXXXObject);		// db3 转 XXXXObject
		Sqlite2XXXXObject.addChildJob(MergeXXXXObject2LatestTemp);	// 合并新数据到临时目录
		MergeXXXXObject2LatestTemp.addChildJob(Temp2Latest);		// 将临时目录的内容拷贝到 latest
		MergeXXXXObject2LatestTemp.addChildJob(XXXXObject2HFile);	// 将 XXXXObject 转为 HFile 文件
		XXXXObject2HFile.addChildJob(LoadHFile); 					// 加载 HFile 到 HBase

		return result;
	}

}
