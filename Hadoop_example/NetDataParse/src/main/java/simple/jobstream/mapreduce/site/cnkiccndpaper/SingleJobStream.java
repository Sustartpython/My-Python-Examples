package simple.jobstream.mapreduce.site.cnkiccndpaper;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String rootDir = "";
		String defaultRootDir = "";

		// // 带解析新数据路径test"
		String rawDataDir = "/RawData/cnki/cnkiccndpaper/big_json/20190623";
		String rawDataXXXXObjectDir = "/RawData/cnki/cnkiccndpaper/xxxxobject"; // 新数据解析后的XXXXObject格式数据路径
		String latest_tempDir = "/RawData/cnki/cnkiccndpaper/latest_temp"; // 新数据和旧数据合并去重得到临时数据的存放路径
		String latestDir = "/RawData/cnki/cnkiccndpaper/latest"; // 将latest_tempDir数据保存到该目录，以备下次更新使用
		String new_data_xxxxobject = "/RawData/cnki/cnkiccndpaper/new_data/xxxxobject"; // 去重后得到的新数据
		String new_data_stdDir = "/RawData/cnki/cnkiccndpaper/new_data/stdfile"; // 新数据转换为DB3格式存放目录

		// 第一步：将最近增加的big_htm文件转换为xxxxobjcet文件存放在xxxxobject目录下
		JobNode Json2XXXXObject = JobNodeModel.getJobNode4Parse2XXXXObject("cnkiccndpaper.Json2XXXXObject",
				DateTimeHelper.getNowTimeAsBatch(), "simple.jobstream.mapreduce.site.cnkiccndpaper.Json2XXXXObject",
				rawDataDir, rawDataXXXXObjectDir, 200);

		// 将历史累积数据和新数据合并去重

		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnkiccndpaper.MergeXXXXObject2Temp",
				rawDataXXXXObjectDir, latestDir, latest_tempDir, 200);

//		// 生成新数据。不能是“总数据-老数据”，新数据要全刷一遍。
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnkiccndpaper.GenNewData", rawDataXXXXObjectDir,
				latest_tempDir, new_data_xxxxobject, 200);
//
		// 备份累积数据
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject("cnkiccndpaper.Temp2Latest", latest_tempDir,
				latestDir);

		// 解析生成页面，生成DB两部曲
		// 以A层格式导出db3
		JobNode Std2Db3ZhiTu = JobNodeModel.getJobNode4Std2Db3("cnkiccndpaper.Std2Db3ZhiTu",
				"simple.jobstream.mapreduce.common.vip.Std2Db3A",rawDataXXXXObjectDir, new_data_stdDir,
				"cnkiccndpaper", "/RawData/_rel_file/zt_template.db3", 10);
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(Std2Db3ZhiTu);
//		result.add(Std2Db3ZhiTu);
		
		// 正常更新
//		result.add(Json2XXXXObject);
//		Json2XXXXObject.addChildJob(MergeXXXXObject2Temp);
//		MergeXXXXObject2Temp.addChildJob(GenNewData);
//		GenNewData.addChildJob( Std2Db3ZhiTu);
//		Std2Db3ZhiTu.addChildJob(Temp2Latest);
//		
//		// 解析cnki_bz id	
		JobNode getid =new JobNode("cnki_id", defaultRootDir, 
  				0,  "simple.jobstream.mapreduce.site.cnkiccndpaper.parseCnkiId");	
		getid.setConfig("inputHdfsPath", "/RawData/cnki/cnkiccndpaper/listBigjson/20190711");
		getid.setConfig("outputHdfsPath", "/RawData/cnki/cnkiccndpaper/articleID/20190702");
		result.add(getid);
//			
//

		

	
		return result;
	}

}
