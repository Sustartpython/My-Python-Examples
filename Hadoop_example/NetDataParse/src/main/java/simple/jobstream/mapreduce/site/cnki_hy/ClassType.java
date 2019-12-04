package simple.jobstream.mapreduce.site.cnki_hy;

import java.util.ArrayList;
import java.util.HashMap;


public class ClassType 
{
	//private String _Class = "";
	
	private HashMap<String, String> FirstClassMap      = null;
	private HashMap<String, String> SecondOnlyClassMap = null;
	private HashMap<String, String> SecondClassMap     = null;
	//private ArrayList<String> classList                = new ArrayList<String>();
	
	public ClassType(HashMap<String, String> FirstClassMap, HashMap<String, String> SecondClassMap,
			HashMap<String, String> SecondOnlyClassMap)
	{
		
		this.FirstClassMap      = FirstClassMap;
		this.SecondClassMap     = SecondClassMap;
		this.SecondOnlyClassMap = SecondOnlyClassMap;
		
		
	}
	
	public static String Full2Half(String full_str)
    {
        String half_str = new String(full_str);
        
        //小写字母,26个
        //大写字母,26个
        //数字,10个
        //符号,英文全角,基本键区第一排,18个
        //符号,英文全角,基本键区其他,14个
        //以上共94个
        //中文符号？
        String strcn = "ａｂｃｄｅｆｇｈｉｊｋｌｍｎｏｐｑｒｓｔｕｖｗｘｙｚ" +
                       "ＡＢＣＤＥＦＧＨＩＪＫＬＭＮＯＰＱＲＳＴＵＶＷＸＹＺ" +
                       "１２３４５６７８９０" +
                       "～！＠＃＄％＾＆＊（）＿＋｜｀－＝＼" +
                       "｛｝［］：＂；＇＜＞？，．／" +
                       "–"      //特别的减号
                       ;
        //小写字母,26个
        //大写字母,26个
        //数字,10个
        //符号,英文半角,基本键区第一排,18个,反斜线需转义
        //符号,英文半角,基本键区其他,14个,双引号需转义
        //以上共94个
        String stren = "abcdefghijklmnopqrstuvwxyz" +
                       "ABCDEFGHIJKLMNOPQRSTUVWXYZ" +
                       "1234567890" +
                       "~!@#$%^&*()_+|`-=\\" +
                       "{}[]:\";'<>?,./" +
                       "-"
                       ;

        for (int i = 0; i < strcn.length(); ++i)
        {
            half_str.replaceAll(strcn.substring(i, i+1), stren.substring(i, i+1));
        }

        return half_str.toString();
    }
	
	public static String UniqLine(String line)
	{
		String[] vec = line.split(";");
        ArrayList<String> lst = new ArrayList<String>();
        for (int i = 0; i < vec.length; ++i )
        {
        	String val = vec[i].trim();
            if (val.length() < 1)
            {
                continue;
            }
            if (!lst.contains(val))
            {
                lst.add(val);
            }
        }
        String newLine = "";
        for(String val:lst)
        {
            newLine += val + ";";
        }

        return newLine;
	}
	
	public String GetClassTypes(String _class)
	{
		_class = Full2Half(_class).toUpperCase().trim();
		ArrayList<String> classList  = new ArrayList<String>();
		String[] arr_class = _class.split("\\s+|;|,");
        for(String val:arr_class)
        {
            if (val.trim().length() > 0)
            {
                classList.add(val.trim());
            }
        }
		
		String classtypes = "";
        
        //处理原本以空格分隔的多个分类号
		for (int i = 0; i < classList.size(); ++i) 
		{
			String tempclass = classList.get(i).trim();
			
			for (int j = tempclass.length(); j > 0; --j)
			{
				tempclass = tempclass.substring(0, j);
				
				if (SecondClassMap.containsKey(tempclass))
                {
					classtypes += SecondClassMap.get(tempclass);
                }
				
				if (FirstClassMap.containsKey(tempclass))
                {
					classtypes += FirstClassMap.get(tempclass);
                }
				
				if (tempclass.length() < 2) //如果已经是单个字符
                {
                    break;
                }
				
				char ch = tempclass.charAt(tempclass.length() - 1);
                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))   //如果最后一个字符是字母
                {
                    break;
                }
			}
		}
		classtypes = UniqLine(classtypes);
		
		return classtypes.replaceAll(";+", ";");
	}
	
	public String GetShowClassTypes(String _class)
	{
		_class = Full2Half(_class).toUpperCase().trim();
		ArrayList<String> classList  = new ArrayList<String>();
		String[] arr_class = _class.split("\\s+|;|,");
        for(String val:arr_class)
        {
            if (val.trim().length() > 0)
            {
                classList.add(val.trim());
            }
        }
		
		String showclasstypes = "";
        
        //处理原本以空格分隔的多个分类号
		for (int i = 0; i < classList.size(); ++i) 
		{
			String tempclass = classList.get(i).trim();
			String part      = "";       //对应一个分类号的结果
			
			System.out.println(tempclass);
			
			for (int j = tempclass.length(); j > 0; --j)
			{
				tempclass = tempclass.substring(0, j);
				
				System.out.println(tempclass);
				
				if (SecondOnlyClassMap.containsKey(tempclass))
                {
					part += SecondOnlyClassMap.get(tempclass).replaceAll(";", ",");
                }
				
				System.out.println(part);
				
				if (tempclass.length() < 2) //如果已经是单个字符
                {
                    break;
                }
				
				char ch = tempclass.charAt(tempclass.length() - 1);
                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))   //如果最后一个字符是字母
                {
                    break;
                }
			}
			
			System.out.println(part);
			
			if (part.length() < 1)    //在二级分类中未找到
            {
				tempclass = classList.get(i).trim();
				for (int j = tempclass.length(); j > 0; --j)
                {
					tempclass = tempclass.substring(0, j);
					if (FirstClassMap.containsKey(tempclass))
	                {
						part += FirstClassMap.get(tempclass).replaceAll(";", ",");
	                }
					if (tempclass.length() < 2) //如果已经是单个字符
	                {
	                    break;
	                }
					char ch = tempclass.charAt(tempclass.length() - 1);
	                if ((ch >= 'a' && ch <= 'z') || (ch >= 'A' && ch <= 'Z'))   //如果最后一个字符是字母
	                {
	                    break;
	                }
                }
            }
			
            if (part.length() > 0)
            {
            	part = part.trim();
            	if (part.substring(part.length()-1, part.length()).equals(","))
            	{
            		part = part.substring(0, part.length()-1);
				}
            	showclasstypes += part + ";";
            }
            
		}
		
		return showclasstypes.replaceAll(";+", ";");
	}
	
	public static void main(String[] args)
	{/*
		HashMap<String, String> FirstClassMap      = new HashMap<>();
		HashMap<String, String> SecondOnlyClassMap = new HashMap<>();
		HashMap<String, String> SecondClassMap     = new HashMap<>();
		
		try
        {
        	File file=new File("E:/class_info.txt");
        	if(file.isFile() && file.exists())
        	{ //判断文件是否存在
        		InputStreamReader read = new InputStreamReader(new FileInputStream(file),"utf-8");
        		BufferedReader bufferedReader = new BufferedReader(read);

        		String lineTxt = null;
        		while((lineTxt = bufferedReader.readLine()) != null)
        		{
        			String[] arrTempLine = lineTxt.toString().split("\t");
					
					String classid       = arrTempLine[0].trim();
					String classtypename = arrTempLine[1].trim();
					String classcode     = arrTempLine[2].trim();
					String classidlevel  = arrTempLine[3].trim();
					String classlevelname= arrTempLine[4].trim();
					
					String[] arr_class = classcode.split(";");
					
					if (classidlevel.split(";").length == 1) 
					{
						for (int i = 0; i < arr_class.length; i++) 
						{
							String tempclass = arr_class[i].trim();
							
							if (FirstClassMap.containsKey(tempclass)) 
							{
								String templine = FirstClassMap.get(tempclass);
								
								templine += "[" + classid + "]" + classtypename + ";";
								FirstClassMap.put(tempclass, templine);
								
							}else {
								
								String templine = "[" + classid + "]" + classtypename + ";";
								FirstClassMap.put(tempclass, templine);
							}
						}
					}else if (classidlevel.split(";").length == 2) {
						
						for (int i = 0; i < arr_class.length; i++) 
						{
							String tempclass = arr_class[i].trim();
							
							if (SecondOnlyClassMap.containsKey(tempclass)) 
							{
								String templine = SecondOnlyClassMap.get(tempclass);
								
								templine += "[" + classid + "]" + classtypename + ";";
								SecondOnlyClassMap.put(tempclass, templine);
								
							}else {
								
								String templine = "[" + classid + "]" + classtypename + ";";
								SecondOnlyClassMap.put(tempclass, templine);
							}
							
							if (SecondClassMap.containsKey(tempclass)) 
							{
								String templine = SecondClassMap.get(tempclass);
								
								templine += "[" + classidlevel.split(";")[0].trim() + "]" + 
										classlevelname.split(";")[0].trim() + ";" + 
										"[" + classidlevel.split(";")[1].trim() + "]" + 
										classlevelname.split(";")[1].trim() + ";";
								
								SecondClassMap.put(tempclass, templine);
								
							}else {
								
								String templine = "[" + classidlevel.split(";")[0].trim() + "]" + 
										classlevelname.split(";")[0].trim() + ";" + 
										"[" + classidlevel.split(";")[1].trim() + "]" + 
										classlevelname.split(";")[1].trim() + ";";
								SecondClassMap.put(tempclass, templine);
							}
						}
					}
        		}
        		read.close();
        	}else{
        		throw new RuntimeException("找不到指定的文件");
        	}
        }catch (Exception e) {
        	e.printStackTrace();
        	throw new RuntimeException("读取文件内容出错");
        }
		
		Iterator iter1 = FirstClassMap.entrySet().iterator();
		while (iter1.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter1.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			FirstClassMap.put(key, UniqLine(val));
		}
		
		Iterator iter2 = SecondClassMap.entrySet().iterator();
		while (iter2.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter2.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			SecondClassMap.put(key, UniqLine(val));
		}
		
		Iterator iter3 = SecondOnlyClassMap.entrySet().iterator();
		while (iter3.hasNext()) 
		{
			Map.Entry entry = (Map.Entry) iter3.next();
			String key = (String) entry.getKey();
			String val = (String) entry.getValue();
			
			SecondOnlyClassMap.put(key, UniqLine(val));
		}
		
		String _class = "TP3 C93";
		
		ClassType c = new ClassType(FirstClassMap, SecondClassMap, SecondOnlyClassMap, _class);
		
		String ClassTypes = c.GetClassTypes();
		String ShowClassTypes = c.GetShowClassTypes();
		
		System.out.println("ClassTypes: " + ClassTypes);
		System.out.println("ShowClassTypes: " + ShowClassTypes);
		
		System.out.println(FirstClassMap.containsKey("C"));
	*/
	}
}
