package simple.jobstream.mapreduce.site.cnipapatent;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> cnipaPatent(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawCoverDir = "/RawData/cnipapatent/big_cover/json";
		String coverXXXXObjectDir = "/RawData/cnipapatent/CoverXXXXObject";
		
		String rawJsonDir = "/RawData/cnipapatent/big_json/20190128";
		String rawXXXXObjectDir = "/RawData/cnipapatent/XXXXObject";
		String latest_tempDirCover = "/RawData/cnipapatent/latest_temp_cover";
		String latestDir = "/RawData/cnipapatent/latest";	//成品目录
		String latest_tempDir = "/RawData/cnipapatent/latest_temp";	//临时成品目录
		String newXobjDir = "/RawData/cnipapatent/new_data";	
		String db3Dir = "/RawData/cnipapatent/Stdpatent";
		
		
		//专利检索及分析全新	这个的图片结构有变化
		JobNode Cover2XXXXObject = new JobNode("CnipaPatentCoverParse", defaultRootDir,
				0, "simple.jobstream.mapreduce.site.cnipapatent.sipoCover2XXXXObject");		
		Cover2XXXXObject.setConfig("inputHdfsPath", rawCoverDir);
		Cover2XXXXObject.setConfig("outputHdfsPath", coverXXXXObjectDir);
		
		//专利检索及分析全新		
		JobNode html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"CnipaPatentParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.cnipapatent.sipoJson2XXXXObject",
				rawJsonDir,
				rawXXXXObjectDir,
				100);
		
		JobNode MergeXXXXObject2TempCover  = new JobNode("MergeXXXXObject2TempCover", defaultRootDir, 0, 
				"simple.jobstream.mapreduce.site.cnipapatent.MergeXXXXObject2CoverXobj");
		MergeXXXXObject2TempCover.setConfig("inputHdfsPath", rawXXXXObjectDir + "," + coverXXXXObjectDir);
		MergeXXXXObject2TempCover.setConfig("outputHdfsPath", latest_tempDirCover);
		
		
		JobNode MergeXXXXObject2Temp = JobNodeModel.getJonNode4MergeXXXXObject("cnipa.MergeXXXXObject2Temp",
				latest_tempDirCover,
				latestDir, 
				latest_tempDir, 
				200);
		
		
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject("cnipa.GenNewData", 
				latest_tempDirCover, 
				latest_tempDir, 
				newXobjDir, 
				200);	
		
		
		
		JobNode Std2DbACnipa = JobNodeModel.getJobNode4Std2Db3("CnipaStdDb4new",
				"simple.jobstream.mapreduce.site.cnipapatent.Std2DbACnipa", 
				newXobjDir, 
				db3Dir, 
				"CnipaPatent",
				"/user/xujiang/zt_template_zl.db3",
				1);
		
		JobNode Temp2Latest4 = JobNodeModel.getJobNode4CopyXXXXObject("cnipa.Temp2Latest", 
				latest_tempDir,
				latestDir);
		
		//正常更新
		result.add(Cover2XXXXObject);
		Cover2XXXXObject.addChildJob(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2TempCover);
		MergeXXXXObject2TempCover.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(Std2DbACnipa);
		Std2DbACnipa.addChildJob(Temp2Latest4);
		
		
		return result;
	}
	
}
