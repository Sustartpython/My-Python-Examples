package simple.jobstream.mapreduce.common.vip;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.LinkedHashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class AuthorOrgan {
	/**
	 * @author liuqingxin 2018年10月11日 上午11:24:02
	 * @param creator
	 *            形如xxx[a,b];yyy[b,c];zzz的字符串，作者以";"分割，机构序号用"[]"括起来并以","分割
	 * @param insitution
	 *            形如[a]cqu;[b]cqjtu;[c]xnu的字符串
	 * @return 包含两个String的String[],前一个为作者，后一个为机构，将机构序号符号编为数字，xxx[1,2];yyy[2,3];zzz
	 *         [1]cqu;[2]cqjtu;[3]xnu
	 *         ";"只能用于分割作者机构，如在其他地方出现，函数会出错
	 */
	public static String[] renumber(String creator, String insitution) throws Exception {
		if (creator.equals("") || insitution.equals("")) {
			String result[] = new String[2];
			result[0] = creator;
			result[1] = insitution;
			return result;
		}
		if (creator.contains("]") && !insitution.contains("]")) {
			String result[] = new String[2];
			result[0] = creator;
			result[1] = insitution;
			return result;
		}
		if (!creator.contains("]") && insitution.contains("]")) {
			String result[] = new String[2];
			result[0] = creator;
			result[1] = insitution;
			return result;
		}
		
		if (creator.contains("]") && insitution.contains("]")) {
			Set<String> setall = new HashSet<String>();
	        Set<String> set1 = new HashSet<String>();
	        Set<String> set2 = new HashSet<String>();
			Pattern rePattern = Pattern.compile("\\[(.*?)\\]");
			Matcher mcreator = rePattern.matcher(creator);
			String down_creator = "";
			while (mcreator.find()) {
				down_creator = mcreator.group(1);
				for (String strdown_creator : down_creator.split(",")) {
		    		set1.add(strdown_creator);
		        }
			}
			Matcher minsitution = rePattern.matcher(insitution);
			String down_insitution = "";
			while (minsitution.find()) {
				down_insitution = minsitution.group(1);
				for (String strdown_insitution : down_insitution.split(",")) {
		    		set2.add(strdown_insitution);
		        }
			}										    	
	    		    
	        setall.clear();
	        setall.addAll(set1);
	        setall.retainAll(set2);
	        
	        if (setall.size()==0) {
	        	String result[] = new String[2];
				result[0] = creator;
				result[1] = insitution;
				return result;
			}
			
		}
		
		if (!insitution.contains(";") && !insitution.contains("]")) {
			String result[] = new String[2];
			result[1] = "[1]" + insitution;
			if (!creator.contains(";")) {
				result[0] = creator + "[1]";
				return result;
			} else if (!creator.contains("]")) {
				String author = "";
				for (String strcre : creator.split(";")) {
					author = author + strcre + "[1];";
				}
				author = author.substring(0, author.length() - 1);
				result[0] = author;
				return result;
			} else {
				result[0] = creator;
				result[1] = insitution;
				return result;
			}
		}
		Map<String, String> mapinsitution = new HashMap<String, String>();
		Map<String, String> mapinsitutioncopy = new HashMap<String, String>();
		String spInstitution = "";
		for (String strins : insitution.split(";")) {
			if (strins.contains("]")) {
				if (strins.split("\\]").length > 1) {
					mapinsitution.put(strins.split("\\]")[0].replace("[", ""), strins.split("\\]")[1]);
					mapinsitutioncopy.put(strins.split("\\]")[0].replace("[", ""), strins.split("\\]")[1]);
				}
			} else {
				spInstitution = spInstitution + strins + ";";
			}
		}
		LinkedHashMap<String, String> mapcreator = new LinkedHashMap<String, String>();
		LinkedHashMap<String, String> mapcre_ins = new LinkedHashMap<String, String>();
		for (String strcre : creator.split(";")) {
			if (strcre.contains("[")) {
				mapcreator.put(strcre.split("\\[")[0], strcre.split("\\[")[1].replace("]", ""));
			} else {
				if (!mapcreator.containsKey(strcre)) {
					mapcreator.put(strcre, "");
				}
			}
		}

		for (String author : mapcreator.keySet()) {
			String ins = "";
			if (mapcreator.get(author).equals("")) {
				mapcre_ins.put(author, "");
				continue;
			}
			for (String str : mapcreator.get(author).split(",")) {
				if (mapinsitution.containsKey(str)) {
					ins = ins + mapinsitution.get(str) + ";";
					mapinsitutioncopy.remove(str);
				}

			}
			if (ins.length() >= 1) {
				ins = ins.substring(0, ins.length() - 1);
				mapcre_ins.put(author, ins);
			} else {
				mapcre_ins.put(author, "");
			}
		}
		String[] result = numberByMap(mapcre_ins);
		if (mapinsitutioncopy.size() > 0) {
			String leftins = "";
			int startnum = mapinsitution.size() - mapinsitutioncopy.size() + 1;
			for (String string : mapinsitutioncopy.values()) {
				leftins = leftins + "[" + startnum + "]" + string + ";";
				startnum++;
			}
			spInstitution = leftins + spInstitution;

		}
		if (!spInstitution.equals("")) {
			if (result[1].length() > 0) {
				result[1] = result[1] + ";" + spInstitution.substring(0, spInstitution.length() - 1);
			} else {
				result[1] = spInstitution.substring(0, spInstitution.length() - 1);
			}
		}

		if (!result[0].contains("[") && !result[0].contains(";") && !result[1].contains("[")
				&& !result[1].contains(";")) {
			result[0] = result[0] + "[1]";
			result[1] = "[1]" + result[1];
		}
		return result;

	}

	/**
	 * @author liuqingxin 2018年10月11日 上午11:39:21
	 * @param mapcre_ins
	 *            key为单个作者，value为多个机构名称，机构以";"分割
	 *            {"xxx":"cqu;cqjtu","yyy":"cqjtu;xnu","zzz":""}
	 * @tips 作者不能包含"["或"]"否则会出错
	 * @return 包含两个String的String[],前一个为作者，后一个为机构，编好了序号 xxx[1,2];yyy[2,3];zzz
	 *         [1]cqu;[2]cqjtu;[3]xnu
	 */
	public static String[] numberByMap(LinkedHashMap<String, String> mapcre_ins) {
		String[] result = new String[2];
		Map<String, String> ins_num = new HashMap<String, String>();
		int num = 1;
		String creator = "";
		String institution = "";
		for (String author : mapcre_ins.keySet()) {
			if (mapcre_ins.get(author).equals("")) {
				creator = creator + author + ";";
			} else {
				creator = creator + author + "[";
				for (String ins : mapcre_ins.get(author).split(";")) {
					if (!ins_num.containsKey(ins)) {
						ins_num.put(ins, String.valueOf(num));
						institution = institution + "[" + String.valueOf(num) + "]" + ins + ";";
						num++;
					}
					creator = creator + ins_num.get(ins) + ",";
				}
				creator = creator.substring(0, creator.length() - 1) + "];";
			}
		}
		if (creator.length() > 0) {
			creator = creator.substring(0, creator.length() - 1);
		}
		if (institution.length() > 0) {
			institution = institution.substring(0, institution.length() - 1);
		}
		creator = sortCreater(creator);
		result[0] = creator;
		result[1] = institution;
		return result;
	}

	/**
	 * @author liuqingxin 2019年2月14日 上午9:47:49
	 * @tips 主要供numberByMap使用
	 * @param creator
	 *            [5,3,1,2]xxx;[2,4,1]yyy
	 * @return 对[]内的数字从新排序 [1,2,3,5]xxx;[1,2,4]yyy
	 */
	public static String sortCreater(String creator) {
		Matcher m1 = Pattern.compile("\\[(.*?)\\]").matcher(creator);
		while (m1.find()) {
			String rp = m1.group(1);
			List<String> list = new ArrayList<String>();
			for (String num : rp.split(",")) {
				list.add(num);
			}
			Collections.sort(list);
			String rpString = "";
			for (String num : list) {
				rpString = rpString + num + ",";
			}
			rpString = rpString.substring(0, rpString.length() - 1);
			creator = creator.replaceFirst(rp, rpString);
		}
		return creator;
	}

}