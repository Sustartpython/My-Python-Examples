package simple.jobstream.mapreduce.site.siamjournal;

import java.util.LinkedHashSet;

import org.apache.hadoop.mapred.jobcontrol.Job;

import com.cloudera.org.codehaus.jackson.format.InputAccessor.Std;
import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.util.DateTimeHelper;
import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream(){
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();
		String defaultRootDir = "";
		
		String rawHtmlDir = "/RawData/siamjournal/big_json/2019/20190703";
		String rawXXXXObjectDir = "/RawData/siamjournal/XXXXObject";
		String latest_tempDir = "/RawData/siamjournal/latest_temp";	//临时成品目录
		String latestDir = "/RawData/siamjournal/latest";	//成品目录
		String newXXXXObjectDir = "/RawData/siamjournal/new_data";	
		String db3Dir = "/RawData/siamjournal/Stdqk";
		
		/**/
//		JobNode html2XXXXObject = new JobNode("SiamJsonParse", defaultRootDir,
//				0, "simple.jobstream.mapreduce.site.siamjournal.Json2XXXXObject");		
//		html2XXXXObject.setConfig("inputHdfsPath", rawHtmlDir);
//		html2XXXXObject.setConfig("outputHdfsPath", rawXXXXObjectDir);
		
		String rawHtmlDir1 = "/RawData/siamjournal/big_json/2018/20181214";
		String rawHtmlDir2 = "/RawData/siamjournal/big_json/2019/20190122";
		String rawHtmlDir3 = "/RawData/siamjournal/big_json/2019/20190216";
		String rawHtmlDir4 = "/RawData/siamjournal/big_json/2019/20190323";
		String rawHtmlDir5 = "/RawData/siamjournal/big_json/2019/20190508";
		

		JobNode html2XXXXObject =JobNodeModel.getJobNode4Parse2XXXXObject(
				"SiamJsonParse",
				DateTimeHelper.getNowTimeAsBatch(),
				"simple.jobstream.mapreduce.site.siamjournal.Json2XXXXObject",
				rawHtmlDir, //rawHtmlDir1+','+rawHtmlDir2+','+rawHtmlDir3+','+rawHtmlDir4+','+rawHtmlDir5,// rawHtmlDir,
				rawXXXXObjectDir,
				100);
		
		
		JobNode MergeXXXXObject2Temp  = JobNodeModel.getJonNode4MergeXXXXObject(
				"Siamjournal.Merge", 
				rawXXXXObjectDir, 
				latestDir, 
				latest_tempDir,
				100);
		
		JobNode GenNewData = JobNodeModel.getJonNode4ExtractXXXXObject(
				"jamajournal.GenNew",
				rawXXXXObjectDir,
				latest_tempDir,
				newXXXXObjectDir,
				100);
		
		
		JobNode StdDb3A = JobNodeModel.getJobNode4Std2Db3(
				"Siamjournal.StdDb3A",
				"simple.jobstream.mapreduce.site.siamjournal.Std2Db3A", 
				newXXXXObjectDir, 
				db3Dir, 
				"siamjournal",
//				"/RawData/_rel_file/zt_template.db3",
				"/user/xujiang/new_template.db3",
				1);
		
		JobNode StdDb3 = JobNodeModel.getJobNode4Std2Db3(
				"Siamjournal.stdDb3",
				"simple.jobstream.mapreduce.site.siamjournal.Std2Db3", 
				newXXXXObjectDir, 
				db3Dir, 
				"siamjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
		JobNode Temp2Latest = JobNodeModel.getJobNode4CopyXXXXObject(
				"Siamjournal.Copy.Temp2Latest",
				latest_tempDir,
				latestDir);

		
		
		result.add(html2XXXXObject);
		html2XXXXObject.addChildJob(MergeXXXXObject2Temp);
		MergeXXXXObject2Temp.addChildJob(GenNewData);
		GenNewData.addChildJob(StdDb3);
		StdDb3.addChildJob(Temp2Latest);
	
		
		JobNode StdDb3all = JobNodeModel.getJobNode4Std2Db3(
				"Siamjournal.stdDb3",
				"simple.jobstream.mapreduce.site.siamjournal.Std2Db3", 
				latestDir, 
				db3Dir, 
				"siamjournal",
				"/RawData/_rel_file/zt_template.db3",
				1);
		
//		result.add(StdDb3all);
		
		
		return result;
	}
	
}
