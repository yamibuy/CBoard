package org.cboard.dataprovider.util;

import org.apache.commons.lang.StringUtils;
import org.cboard.dataprovider.config.*;
import org.stringtemplate.v4.compiler.STParser.element_return;

import com.facebook.presto.jdbc.internal.spi.predicate.Domain;

import javolution.io.Struct.Bool;

import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.StringJoiner;
import java.util.function.Function;
import java.util.stream.Collectors;
import java.util.stream.IntStream;
import java.util.stream.Stream;

import static org.cboard.dataprovider.DataProvider.NULL_STRING;
import static org.cboard.dataprovider.DataProvider.separateNull;


/**
 * Created by zyong on 2017/9/15.
 */
public class SqlHelper {

    private String tableName;
    private StringBuffer whereStr = new StringBuffer();
    private boolean isSubquery;
    private SqlSyntaxHelper sqlSyntaxHelper = new SqlSyntaxHelper();

    public SqlHelper() {}

    public SqlHelper(String tableName, boolean isSubquery) {
        this.tableName = tableName;
        this.isSubquery = isSubquery;
    }

    public String assembleFilterSql(AggConfig config) {
        String whereStr = null;
        if (config != null) {
            Stream<DimensionConfig> c = config.getColumns().stream();
            Stream<DimensionConfig> r = config.getRows().stream();
            Stream<ConfigComponent> f = config.getFilters().stream();
            Stream<ConfigComponent> filters = Stream.concat(Stream.concat(c, r), f);
            whereStr = filterSql(filters, "WHERE");
        }
        return whereStr;
    }

    public String assembleFilterSql(Stream<ConfigComponent> filters) {
        return filterSql(filters, "WHERE");
    }

    public String assembleAggDataSql(AggConfig config) throws Exception {
        Stream<DimensionConfig> c = config.getColumns().stream();
        Stream<DimensionConfig> r = config.getRows().stream();
        Stream<ConfigComponent> f = config.getFilters().stream();
        Stream<ConfigComponent> filters = Stream.concat(Stream.concat(c, r), f);
        Stream<DimensionConfig> dimStream = Stream.concat(config.getColumns().stream(), config.getRows().stream());

        String dimColsStr = assembleDimColumns(dimStream);
        String aggColsStr = assembleAggValColumns(config.getValues().stream());

        String whereStr = filterSql(filters, "WHERE");
        String groupByStr = StringUtils.isBlank(dimColsStr) ? "" : "GROUP BY " + dimColsStr;

        StringJoiner selectColsStr = new StringJoiner(",");
        if (!StringUtils.isBlank(dimColsStr)) {
            selectColsStr.add(dimColsStr);
        }
        if (!StringUtils.isBlank(aggColsStr)) {
            selectColsStr.add(aggColsStr);
        }
        String fsql = null;
        if (isSubquery) {
            fsql = "\nSELECT %s \n FROM (\n%s\n) cb_view \n %s \n %s";
        } else {
            fsql = "\nSELECT %s \n FROM %s \n %s \n %s";
        }
        String exec = String.format(fsql, selectColsStr, tableName, whereStr, groupByStr);
        return exec;
    }
    
    public String assembleAggDataSqlv2(AggConfig config) throws Exception {
        Stream<DimensionConfig> c = config.getColumns().stream();
        Stream<DimensionConfig> r = config.getRows().stream();
        Stream<ConfigComponent> f = config.getFilters().stream();
        Stream<ConfigComponent> filters = Stream.concat(Stream.concat(c, r), f);
        Stream<DimensionConfig> dimStream = Stream.concat(config.getColumns().stream(), config.getRows().stream());

        String dimColsStr = assembleDimColumns(dimStream);
        String aggColsStr = assembleAggValColumns(config.getValues().stream());

        String whereStr = filterSqlV3(config);
        String groupByStr = StringUtils.isBlank(dimColsStr) ? "" : "GROUP BY " + dimColsStr;

        StringJoiner selectColsStr = new StringJoiner(",");
        if (!StringUtils.isBlank(dimColsStr)) {
            selectColsStr.add(dimColsStr);
        }
        if (!StringUtils.isBlank(aggColsStr)) {
            selectColsStr.add(aggColsStr);
        }
        String fsql = null;
        if (isSubquery) {
            fsql = "\nSELECT %s \n FROM (\n%s\n) cb_view \n %s \n %s";
        } else {
            fsql = "\nSELECT %s \n FROM %s \n %s \n %s";
        }
        String exec = String.format(fsql, selectColsStr, tableName, whereStr, groupByStr);
        if (config.getLimit() != null) {
        	exec += "LIMIT "+config.getLimit()+"";
		}
        return exec;
    }

    private String filterSql(Stream<ConfigComponent> filterStream, String prefix) {
        StringJoiner where = new StringJoiner("\nAND ", prefix + " ", "");
        where.setEmptyValue("");
        filterStream.map(e -> separateNull(e))
                .map(e -> configComponentToSql(e))
                .filter(e -> e != null) 
                .forEach(where::add);
        return where.toString();
    }
    
    private String filterSqlV3(AggConfig config){
    	List<DimensionConfig> filterList = new ArrayList<DimensionConfig>();
    	for (ConfigComponent component : config.getFilters()) {
			filterList.add((DimensionConfig) component);
		}
    	String whereStr = whereCheck(filterList);
    	return whereStr;
    }
    
    private String whereCheck(List<DimensionConfig> filterList){
    	for(int i = 0; i < filterList.size(); i++){
    		if (filterList.size()>1) {
    			for (int j = i+1; j < filterList.size(); j++) {
    				if (filterList.get(i).getColumnName().equals(filterList.get(j).getColumnName())) {
    					if ((filterList.get(i).getFilterType().equals("=")&&(filterList.get(j).getFilterType().equals("≥")||filterList.get(j).getFilterType().equals("≤")))||
    						(filterList.get(j).getFilterType().equals("=")&&(filterList.get(i).getFilterType().equals("≥")||filterList.get(i).getFilterType().equals("≤")))) {
    						if ((!filterList.get(i).getFilterType().equals("eq")&&filterList.get(i).getValues().size()>0)&&
    								(!filterList.get(j).getFilterType().equals("eq")&&filterList.get(j).getValues().size()>0)) {
    							String biao1 = "";
								String cloumn1 = "";
								if (filterList.get(i).getColumnName().indexOf(".") != -1) {
									biao1 = ""+filterList.get(i).getColumnName().substring(0, filterList.get(i).getColumnName().indexOf("."))+".";
									cloumn1 = ""+filterList.get(i).getColumnName().substring(filterList.get(i).getColumnName().indexOf(".")+1, filterList.get(i).getColumnName().length())+" ";
								}else {
									cloumn1 = filterList.get(i).getColumnName()+" ";
								}
								String biao2 = "";
								String cloumn2 = "";
								if (filterList.get(j).getColumnName().indexOf(".") != -1) {
									biao2 = ""+filterList.get(j).getColumnName().substring(0, filterList.get(j).getColumnName().indexOf("."))+".";
            						cloumn2 = ""+filterList.get(j).getColumnName().substring(filterList.get(j).getColumnName().indexOf(".")+1, filterList.get(j).getColumnName().length())+" ";
								}else {
									cloumn2 = filterList.get(j).getColumnName()+" ";
								}
        						String values1 = "('"+filterList.get(i).getValues().get(0)+"')";
        						String values2 = "('"+filterList.get(j).getValues().get(0)+"')";
        						if (whereStr.toString().equals("")) {
        							whereStr.append("WHERE ("+biao1+cloumn1+">= "+values1+" AND "+biao2+cloumn2+"<= "+values2+") ");
        						}else {
        							whereStr.append(" AND ("+biao1+cloumn1+">= "+values1+" AND "+biao2+cloumn2+"<= "+values2+")");
        						}
        						filterList.remove(j);
        						filterList.remove(i);
        						whereCheck(filterList);
							}
    					}else {
    						if ((!filterList.get(i).getFilterType().equals("eq")&&filterList.get(i).getValues().size()>0)&&(!filterList.get(j).getFilterType().equals("eq")&&filterList.get(j).getValues().size()>0)) {
    							String biao1 = "";
    							String cloumn1 = "";
    							if (filterList.get(i).getColumnName().indexOf(".") != -1) {
    								biao1 = ""+filterList.get(i).getColumnName().substring(0, filterList.get(i).getColumnName().indexOf("."))+".";
    								cloumn1 = ""+filterList.get(i).getColumnName().substring(filterList.get(i).getColumnName().indexOf(".")+1, filterList.get(i).getColumnName().length())+" ";
								}else {
									cloumn1 = filterList.get(i).getColumnName()+" ";
								}
    							String biao2 = "";
    							String cloumn2 = "";
    							if (filterList.get(j).getColumnName().indexOf(".") != -1) {
    								biao2 = ""+filterList.get(j).getColumnName().substring(0, filterList.get(j).getColumnName().indexOf("."))+".";
    								cloumn2 = ""+filterList.get(j).getColumnName().substring(filterList.get(j).getColumnName().indexOf(".")+1, filterList.get(j).getColumnName().length())+" ";
								}else {
									cloumn2 = filterList.get(j).getColumnName()+" ";
								}
        						String filterType1 = "";
        						String filterType2 = "";
        						List<String> valueList1 = new ArrayList<String>();
        						List<String> valueList2 = new ArrayList<String>();
        						for (String str : filterList.get(i).getValues()) {
        							valueList1.add("'"+str+"'");
        						}
        						for (String str : filterList.get(j).getValues()) {
        							valueList2.add("'"+str+"'");
        						}
        						String values1 = "("+valueList1.toString().replace("[", "").replace("]", "")+")";
        						String values2 = "("+valueList2.toString().replace("[", "").replace("]", "")+")";
        						if (filterList.get(i).getFilterType().equals("=")) {
        							filterType1 = "IN ";
        						}else if(filterList.get(i).getFilterType().equals("≠")){
        							filterType1 = "NOT IN ";
        						}else if (filterList.get(i).getFilterType().equals("≥")) {
        							filterType1 = ">= ";
        						}else if (filterList.get(i).getFilterType().equals("≤")) {
        							filterType1 = "<= ";
        						}else {
        							filterType1 = filterList.get(i).getFilterType();
        						}
        						
        						if (filterList.get(j).getFilterType().equals("=")) {
        							filterType2 = "IN ";
        						}else if(filterList.get(j).getFilterType().equals("≠")){
        							filterType2 = "NOT IN ";
        						}else if (filterList.get(j).getFilterType().equals("≥")) {
        							filterType2 = ">= ";
        						}else if (filterList.get(j).getFilterType().equals("≤")) {
        							filterType2 = "<= ";
        						}else {
        							filterType2 = filterList.get(j).getFilterType();
        						}
        						if (whereStr.toString().equals("")) {
        							whereStr.append("WHERE ("+biao1+cloumn1+filterType1+values1+" OR "+biao2+cloumn2+filterType2+values2+")");
        						}else {
        							whereStr.append(" AND ("+biao1+cloumn1+filterType1+values1+" OR "+biao2+cloumn2+filterType2+values2+")");
        						}
        						filterList.remove(j);
        						filterList.remove(i);
        						whereCheck(filterList);
							}
    					}
    				}
    			}
    			if (filterList.size()>0) {
    				if (!filterList.get(i).getFilterType().equals("eq")&&filterList.get(i).getValues().size()>0) {
    					String biao = "";
    					String cloumn = "";
    					if (filterList.get(i).getColumnName().indexOf(".") != -1) {
    						biao = ""+filterList.get(i).getColumnName().substring(0, filterList.get(i).getColumnName().indexOf("."))+".";
    						cloumn = ""+filterList.get(i).getColumnName().substring(filterList.get(i).getColumnName().indexOf(".")+1, filterList.get(i).getColumnName().length())+" ";
    					}else {
    						cloumn = filterList.get(i).getColumnName()+" ";
    					}
    					String filterType = "";
    					List<String> valueList = new ArrayList<String>();
    					for (String str : filterList.get(i).getValues()) {
    						valueList.add("'"+str+"'");
    					}
    					String values = "("+valueList.toString().replace("[", "").replace("]", "")+")";
    					String abString = "";
    					if (filterList.get(i).getFilterType().equals("=")) {
    						filterType = "IN ";
    					}else if(filterList.get(i).getFilterType().equals("≠")){
    						filterType = "NOT IN ";
    					}else if (filterList.get(i).getFilterType().equals("≥")) {
    						filterType = ">= ";
    					}else if (filterList.get(i).getFilterType().equals("≤")) {
    						filterType = "<= ";
    					}else if (filterList.get(i).getFilterType().equals("[a,b]")||filterList.get(i).getFilterType().equals("(a,b)")||
    							filterList.get(i).getFilterType().equals("(a,b]")||filterList.get(i).getFilterType().equals("[a,b)")) {
    						abString = biao+cloumn +" >= "+"("+valueList.get(0)+")"+" AND "+biao+cloumn+" <= "+"("+valueList.get(1)+")";
    						if (whereStr.toString().equals("")) {
    							whereStr.append("WHERE "+abString);
    						}else {
    							whereStr.append(" AND ("+abString+")");
    						}
    						break;
    					}
    					if (whereStr.toString().equals("")) {
    						whereStr.append("WHERE "+biao+cloumn+filterType+values);
    					}else {
    						whereStr.append(" AND "+biao+cloumn+filterType+values);
    					}
    					filterList.remove(i);
    					whereCheck(filterList);
    				}
				}
			}else {
				if (!filterList.get(i).getFilterType().equals("eq")&&filterList.get(i).getValues().size()>0) {
					String biao = "";
					String cloumn = "";
					if (filterList.get(i).getColumnName().indexOf(".") != -1) {
						biao = ""+filterList.get(i).getColumnName().substring(0, filterList.get(i).getColumnName().indexOf("."))+".";
						cloumn = ""+filterList.get(i).getColumnName().substring(filterList.get(i).getColumnName().indexOf(".")+1, filterList.get(i).getColumnName().length())+" ";
					}else {
						cloumn = filterList.get(i).getColumnName()+" ";
					}
					String filterType = "";
					List<String> valueList = new ArrayList<String>();
					for (String str : filterList.get(i).getValues()) {
						valueList.add("'"+str+"'");
					}
					String values = "("+valueList.toString().replace("[", "").replace("]", "")+")";
					String abString = "";
					if (filterList.get(i).getFilterType().equals("=")) {
						filterType = "IN ";
					}else if(filterList.get(i).getFilterType().equals("≠")){
						filterType = "NOT IN ";
					}else if (filterList.get(i).getFilterType().equals("≥")) {
						filterType = ">= ";
					}else if (filterList.get(i).getFilterType().equals("≤")) {
						filterType = "<= ";
					}else if (filterList.get(i).getFilterType().equals("[a,b]")||filterList.get(i).getFilterType().equals("(a,b)")||
							filterList.get(i).getFilterType().equals("(a,b]")||filterList.get(i).getFilterType().equals("[a,b)")) {
						abString = biao+cloumn +" >= "+"("+valueList.get(0)+")"+" AND "+biao+cloumn+" <= "+"("+valueList.get(1)+")";
						if (whereStr.toString().equals("")) {
							whereStr.append("WHERE "+abString);
						}else {
							whereStr.append(" AND ("+abString+")");
						}
						break;
					}
					if (whereStr.toString().equals("")) {
						whereStr.append("WHERE "+biao+cloumn+filterType+values);
					}else {
						whereStr.append(" AND "+biao+cloumn+filterType+values);
					}
					filterList.remove(i);
					whereCheck(filterList);
				}
			}
    	}
    	return whereStr.toString();
    }
    
//    private String filterSqlV2(List<ConfigComponent> filters, String prefix) {
//    	StringBuffer whereStr = new StringBuffer();
//    	Boolean hasNow = false;
//    	List<DimensionConfig> filterList = new ArrayList<DimensionConfig>();
//    	List<DimensionConfig> sameFilter = new ArrayList<DimensionConfig>();
//    	List<DimensionConfig> orderFilte = new ArrayList<DimensionConfig>();
//        for (ConfigComponent configComponent : filters) {
//			filterList.add((DimensionConfig) configComponent);
//		}
//        
//        //判断是否含有相同字段，将相同的放入sameFilter中，不同的在orderFilte
//        if (filterList.size()>0) {
//        	for(int i=0;i<filterList.size();i++){
//        		for (int j = i+1; j < filterList.size(); j++) {
//    				if (filterList.get(i).getColumnName().equals(filterList.get(j).getColumnName())) {
//    					sameFilter.add(filterList.get(i));
//    					sameFilter.add(filterList.get(j));
//    					hasNow = true;
//					}else {
//						Boolean isHas = false;
//						for (DimensionConfig dimensionConfig : orderFilte) {
//							if (dimensionConfig.getColumnName().equals(filterList.get(j).getColumnName())) {
//								isHas = true;
//							}
//						}
//						if (!isHas) {
//							orderFilte.add(filterList.get(j));
//						}
//					}
//    			}
//        	}
//			
//		}
//        
//        
//        if (hasNow) {
//        	if (sameFilter.size() == 2) {
//				if ((sameFilter.get(0).getFilterType().equals("=")&&(sameFilter.get(1).getFilterType().equals("≥")||sameFilter.get(1).getFilterType().equals("≤")))||
//						(sameFilter.get(1).getFilterType().equals("=")&&(sameFilter.get(0).getFilterType().equals("≥")||sameFilter.get(0).getFilterType().equals("≤")))) {
//					String biao1 = "\""+sameFilter.get(0).getColumnName().substring(0, sameFilter.get(0).getColumnName().indexOf("."))+"\"";
//					String biao2 = "\""+sameFilter.get(1).getColumnName().substring(0, sameFilter.get(1).getColumnName().indexOf("."))+"\"";
//					String cloumn1 = "\""+sameFilter.get(0).getColumnName().substring(sameFilter.get(0).getColumnName().indexOf(".")+1, sameFilter.get(0).getColumnName().length())+"\" ";
//					String cloumn2 = "\""+sameFilter.get(1).getColumnName().substring(sameFilter.get(1).getColumnName().indexOf(".")+1, sameFilter.get(1).getColumnName().length())+"\" ";
//					String values1 = "('"+sameFilter.get(0).getValues().get(0)+"')";
//					String values2 = "('"+sameFilter.get(1).getValues().get(0)+"')";
//					if (whereStr.toString().equals("")) {
//						whereStr.append("WHERE ("+biao1+"."+cloumn1+">= "+values1+" AND "+biao2+"."+cloumn2+"<= "+values2+") ");
//					}else {
//						whereStr.append("AND ("+biao1+"."+cloumn1+">="+values1+"AND"+biao2+"."+cloumn2+"<="+values2+")");
//					}
//				}else {
//					String biao1 = "\""+sameFilter.get(0).getColumnName().substring(0, sameFilter.get(0).getColumnName().indexOf("."))+"\"";
//					String biao2 = "\""+sameFilter.get(1).getColumnName().substring(0, sameFilter.get(1).getColumnName().indexOf("."))+"\"";
//					String cloumn1 = "\""+sameFilter.get(0).getColumnName().substring(sameFilter.get(0).getColumnName().indexOf(".")+1, sameFilter.get(0).getColumnName().length())+"\" ";
//					String cloumn2 = "\""+sameFilter.get(1).getColumnName().substring(sameFilter.get(1).getColumnName().indexOf(".")+1, sameFilter.get(1).getColumnName().length())+"\" ";
//					String filterType1 = "";
//					String filterType2 = "";
//					List<String> valueList1 = new ArrayList<String>();
//					List<String> valueList2 = new ArrayList<String>();
//					for (String str : sameFilter.get(0).getValues()) {
//						valueList1.add("'"+str+"'");
//					}
//					for (String str : sameFilter.get(1).getValues()) {
//						valueList2.add("'"+str+"'");
//					}
//					String values1 = "("+valueList1.toString().replace("[", "").replace("]", "")+")";
//					String values2 = "("+valueList2.toString().replace("[", "").replace("]", "")+")";
//					if (sameFilter.get(0).getFilterType().equals("=")) {
//						filterType1 = "IN ";
//					}else if(sameFilter.get(0).getFilterType().equals("≠")){
//						filterType1 = "NOT IN ";
//					}else if (sameFilter.get(0).getFilterType().equals("≥")) {
//						filterType1 = ">= ";
//					}else if (sameFilter.get(0).getFilterType().equals("≤")) {
//						filterType1 = "<= ";
//					}else {
//						filterType1 = sameFilter.get(0).getFilterType();
//					}
//					
//					if (sameFilter.get(1).getFilterType().equals("=")) {
//						filterType2 = "IN ";
//					}else if(sameFilter.get(1).getFilterType().equals("≠")){
//						filterType2 = "NOT IN ";
//					}else if (sameFilter.get(1).getFilterType().equals("≥")) {
//						filterType2 = ">= ";
//					}else if (sameFilter.get(1).getFilterType().equals("≤")) {
//						filterType2 = "<= ";
//					}else {
//						filterType2 = sameFilter.get(1).getFilterType();
//					}
//					if (whereStr.toString().equals("")) {
//						whereStr.append("WHERE ("+biao1+"."+cloumn1+filterType1+values1+"OR"+biao2+"."+cloumn2+filterType2+values2+")");
//					}else {
//						whereStr.append("AND ("+biao1+"."+cloumn1+filterType1+values1+"OR"+biao2+"."+cloumn2+filterType2+values2+")");
//					}
//				}
//			}
//			for (DimensionConfig dimensionConfig : orderFilte) {
//				String biao = "\""+dimensionConfig.getColumnName().substring(0, dimensionConfig.getColumnName().indexOf("."))+"\"";
//				String cloumn = "\""+dimensionConfig.getColumnName().substring(dimensionConfig.getColumnName().indexOf(".")+1, dimensionConfig.getColumnName().length())+"\" ";
//				String filterType = "";
//				List<String> valueList = new ArrayList<String>();
//				for (String str : dimensionConfig.getValues()) {
//					valueList.add("'"+str+"'");
//				}
//				String values = "("+valueList.toString().replace("[", "").replace("]", "")+")";
//				String abString = "";
//				if (dimensionConfig.getFilterType().equals("=")) {
//					filterType = "IN ";
//				}else if(dimensionConfig.getFilterType().equals("≠")){
//					filterType = "NOT IN ";
//				}else if (dimensionConfig.getFilterType().equals("≥")) {
//					filterType = ">= ";
//				}else if (dimensionConfig.getFilterType().equals("≤")) {
//					filterType = "<= ";
//				}else if (dimensionConfig.getFilterType().equals("[a,b]")) {
//					abString = biao+"."+cloumn +">= "+"("+valueList.get(0)+")"+" AND "+biao+"."+cloumn+"<= "+"("+valueList.get(1)+")";
//					if (whereStr.toString().equals("")) {
//						whereStr.append("WHERE "+abString);
//					}else {
//						whereStr.append(" OR ("+abString+")");
//					}
//					break;
//				}
//				if (whereStr.toString().equals("")) {
//					whereStr.append("WHERE "+biao+"."+cloumn+filterType+values);
//				}else {
//					whereStr.append("AND "+biao+"."+cloumn+filterType+values);
//				}
//			}
//		}else {
//			for (DimensionConfig dimensionConfig : filterList) {
//				String biao = "\""+dimensionConfig.getColumnName().substring(0, dimensionConfig.getColumnName().indexOf("."))+"\"";
//				String cloumn = "\""+dimensionConfig.getColumnName().substring(dimensionConfig.getColumnName().indexOf(".")+1, dimensionConfig.getColumnName().length())+"\" ";
//				String filterType = "";
//				List<String> valueList = new ArrayList<String>();
//				for (String str : dimensionConfig.getValues()) {
//					valueList.add("'"+str+"'");
//				}
//				String values = "("+valueList.toString().replace("[", "").replace("]", "")+")";
//				String abString = "";
//				if (dimensionConfig.getFilterType().equals("=")) {
//					filterType = "IN ";
//				}else if(dimensionConfig.getFilterType().equals("≠")){
//					filterType = "NOT IN ";
//				}else if (dimensionConfig.getFilterType().equals("≥")) {
//					filterType = ">= ";
//				}else if (dimensionConfig.getFilterType().equals("≤")) {
//					filterType = "<= ";
//				}else if (dimensionConfig.getFilterType().equals("[a,b]")) {
//					abString = biao+"."+cloumn +">= "+"("+valueList.get(0)+")"+" AND "+biao+"."+cloumn+"<= "+"("+valueList.get(1)+")";
//					if (whereStr.toString().equals("")) {
//						whereStr.append("WHERE "+abString);
//					}else {
//						whereStr.append(" OR ("+abString+")");
//					}
//					break;
//				}
//				if (whereStr.toString().equals("")) {
//					whereStr.append("WHERE "+biao+"."+cloumn+filterType+values);
//				}else {
//					whereStr.append(" AND "+biao+"."+cloumn+filterType+values);
//				}
//			}
//		}
//        return whereStr.toString();
//    }

    private String configComponentToSql(ConfigComponent cc) {
        if (cc instanceof DimensionConfig) {
            return filter2SqlCondtion.apply((DimensionConfig) cc);
        } else if (cc instanceof CompositeConfig) {
            CompositeConfig compositeConfig = (CompositeConfig) cc;
            String sql = compositeConfig.getConfigComponents().stream()
                    .map(e -> separateNull(e))
                    .map(e -> configComponentToSql(e))
                    .collect(Collectors.joining(" " + compositeConfig.getType() + " "));
            return "(" + sql + ")";
        }
        return null;
    }

    /**
     * Parser a single filter configuration to sql syntax
     */
    private Function<DimensionConfig, String> filter2SqlCondtion = (config) -> {
        if (config.getValues().size() == 0) {
            return null;
        }

        String fieldName = sqlSyntaxHelper.getProjectStr(config);
        String v0 = sqlSyntaxHelper.getDimMemberStr(config, 0);
        String v1 = null;
        if (config.getValues().size() == 2) {
            v1 = sqlSyntaxHelper.getDimMemberStr(config, 1);
        }

        if (NULL_STRING.equals(config.getValues().get(0))) {
            switch (config.getFilterType()) {
                case "=":
                case "≠":
                    return config.getColumnName() + ("=".equals(config.getFilterType()) ? " IS NULL" : " IS NOT NULL");
            }
        }

        switch (config.getFilterType()) {
            case "=":
            case "eq":
                return fieldName + " IN (" + valueList(config) + ")";
            case "≠":
            case "ne":
                return fieldName + " NOT IN (" + valueList(config) + ")";
            case ">":
                return rangeQuery(fieldName, v0, null);
            case "<":
                return rangeQuery(fieldName, null, v0);
            case "≥":
                return rangeQuery(fieldName, v0, null, true, true);
            case "≤":
                return rangeQuery(fieldName, null, v0, true, true);
            case "(a,b]":
                return rangeQuery(fieldName, v0, v1, false, true);
            case "[a,b)":
                return rangeQuery(fieldName, v0, v1, true, false);
            case "(a,b)":
                return rangeQuery(fieldName, v0, v1, false, false);
            case "[a,b]":
                return rangeQuery(fieldName, v0, v1, true, true);
        }
        return null;
    };

    private String valueList(DimensionConfig config) {
        String resultList = IntStream.range(0, config.getValues().size())
                .boxed()
                .map(i -> sqlSyntaxHelper.getDimMemberStr(config, i))
                .collect(Collectors
                .joining(","));
        return resultList;
    }

    private String rangeQuery(String fieldName, Object from, Object to, boolean includeLower, boolean includeUpper) {
        StringBuffer result = new StringBuffer();
        result.append("(");
        final String gt = ">",
                gte = ">=",
                lt = "<",
                lte = "<=";
        if (from != null) {
            String op = includeLower ? gte : gt;
            result.append(fieldName + op + from);
        }
        if (to != null) {
            if (from != null) {
                result.append(" AND ");
            }
            String op = includeUpper ? lte : lt;
            result.append(fieldName + op + to);
        }
        result.append(")");
        return result.toString();
    }

    private String rangeQuery(String fieldName, Object from, Object to) {
        return rangeQuery(fieldName, from, to, false, false);
    }

    public static String surround(String text, String quta) {
        return quta + text + quta;
    }

    private String assembleAggValColumns(Stream<ValueConfig> selectStream) {
        StringJoiner columns = new StringJoiner(", ", "", " ");
        columns.setEmptyValue("");
        selectStream.map(vc -> sqlSyntaxHelper.getAggStr(vc)).filter(e -> e != null).forEach(columns::add);
        return columns.toString();
    }

    private String assembleDimColumns(Stream<DimensionConfig> columnsStream) {
        StringJoiner columns = new StringJoiner(", ", "", " ");
        columns.setEmptyValue("");
        columnsStream.map(g -> sqlSyntaxHelper.getProjectStr(g)).distinct().filter(e -> e != null).forEach(columns::add);
        return columns.toString();
    }

    public SqlHelper setSqlSyntaxHelper(SqlSyntaxHelper sqlSyntaxHelper) {
        this.sqlSyntaxHelper = sqlSyntaxHelper;
        return this;
    }

    public SqlSyntaxHelper getSqlSyntaxHelper() {
        return this.sqlSyntaxHelper;
    }
}
