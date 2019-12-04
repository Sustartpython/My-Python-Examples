package simple.jobstream.mapreduce.site.cssci;

import java.util.List;

public class CssciDetail{
	public class Author {
	    public String zzmc;//作者名称
	    public String zzpm;//序号
	    public String jgmc;//机构名称
	    public String bmmc;//部门名称
	}


	public class Catation{
		public String yw_id;//引文ID
		public String ywno;//引文序号
		public String ywzz;//引文作者
		public String ywpm;//引文篇名
		public String ywqk;//引文期刊
		public String ywlx;//引文类型
		public String ywnd;//引文年代
		public String ywym;//引文页码
		public String ywcbd;//引文出版地
		public String ywcbs;//引文出版社
		public String ywcc;//原始网站展示的时间、期
		public String sno;//文章ID
	}

	public class Contents{
		public String blpm;//英文篇名
		public String byc;//关键词
		public String nian;//年
		public String juan;//卷
		public String qi;//期
		public String ym;//页码 9-16,56
		public String lypm;//篇名
		public String qkmc;//期刊名称
		public String wzlx;//文献类型  1论文
		public String xkdm1;//中图类号1
		public String xkdm2;//中图类号2
		public String xkfl1;//学科分类号1
		public String xkfl2;//学科分类号2
		public String xmlb;//基金项目
		public String qkdm;//期刊ID
		public String sno;//文章ID
	}
	public List<Author> author;
	public List<Catation> catation;
	public List<Contents> contents;
}