package simple.jobstream.mapreduce.user.walker.wf_qk_med;

import java.util.LinkedHashSet;

import com.vipcloud.JobNode.JobNode;

import simple.jobstream.mapreduce.common.vip.JobNodeModel;

public class SingleJobStream {
	public static LinkedHashSet<JobNode> getJobStream() {
		LinkedHashSet<JobNode> result = new LinkedHashSet<JobNode>();

		// 万方医学
		JobNode Std2Db3Med = JobNodeModel.getJobNode4Std2Db3("wf_qk_med.Std2Db3ZT", 
											"simple.jobstream.mapreduce.user.walker.wf_qk_med.Std2Db3Med", 
											"/RawData/wanfang/qk/detail/new_data/XXXXObject", 
//											"/RawData/wanfang/qk/detail/latest", 
											"/RawData/wanfang/qk/detail/med/new_data", 
											"wanfang_qk_med.zt", 
											"/RawData/_rel_file/zt_template.db3", 
											1);		
		
		JobNode CountMed = new JobNode("WFQKMedParse", "", 0,
				"simple.jobstream.mapreduce.user.walker.wf_qk_med.CountMed");
		CountMed.setConfig("inputHdfsPath", "/RawData/wanfang/qk/detail/latest");	
		CountMed.setConfig("outputHdfsPath", "/RawData/wanfang/qk/detail/med/CountMed");

		result.add(Std2Db3Med);
		Std2Db3Med.addChildJob(CountMed);

		return result;
	}
}
