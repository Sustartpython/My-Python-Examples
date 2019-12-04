package simple.jobstream.mapreduce.site.musejournal;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public  static LinkedHashSet<JobNode>getJobStream() {
		String rootDir = "";

		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rawDataDir = "/RawData/muse/musejournal/big_json/2019/20190705"; // 带解析新数据路径
		String rawDataXXXXObjectDir = "/RawData/muse/musejournal/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径

		String latest_tempDir = "/RawData/muse/musejournal/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/muse/musejournal/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用

		String new_data_xxxxobject = "/RawData/muse/musejournal/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/muse/musejournal/new_data/stdFile"; // 新数据转换为DB3格式存放目录

		JobNode Html2XXXXObject = new JobNode("MuseJournalParse", rootDir, 0,
				"simple.jobstream.mapreduce.site.musejournal.Htmlxxxxobject");
		Html2XXXXObject.setConfig("inputHdfsPath", rawDataDir);
		Html2XXXXObject.setConfig("outputHdfsPath", rawDataXXXXObjectDir);

		// 将历史累积数据和新数据合并去重
//
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("MuseJournal.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("MuseJournal.GenNewData", 
				rawDataXXXXObjectDir, latest_tempDir,new_data_xxxxobject, 200);		

		// 导出数据到 db3
//		JobNode StdXXXXObject= new JobNode("MuseJournalDB3", rootDir, 0, "simple.jobstream.mapreduce.site.musejournal.StdXXXXObject");
//		StdXXXXObject.setConfig("inputHdfsPath", new_data_xxxxobject);
//		StdXXXXObject.setConfig("outputHdfsPath", new_data_stdDir);
//		StdXXXXObject.setConfig("reduceNum", "1");
		
		// 导出数据到 db3
		JobNode StdXXXXObject = JobNodeModel.getJobNode4Std2Db3("MuseJournal.Std","simple.jobstream.mapreduce.site.musejournal.StdXXXXObject", 
				new_data_xxxxobject, new_data_stdDir, "musejournal","/RawData/_rel_file/zt_template.db3",1);

//		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("MuseJournal.Temp2Latest", latest_tempDir, latestDir);
	

//		 正常更新
		result.add(Html2XXXXObject);
		Html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdXXXXObject);
		StdXXXXObject.addChildJob(Temp2Latest);
		
		// 只导出db3
		// 导出数据到 db3
//		JobNode StdXXXXObjectLast = JobNodeModel.getJobNode4Std2Db3("MuseJournal.Std","simple.jobstream.mapreduce.site.musejournal.StdXXXXObject", 
//				latestDir, new_data_stdDir, "musejournal","/RawData/_rel_file/zt_template.db3",1);
//		result.add(StdXXXXObjectLast);
		return result;

	}
}
