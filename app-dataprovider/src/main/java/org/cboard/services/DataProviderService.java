package org.cboard.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.*;
import java.util.function.Consumer;

import com.alibaba.fastjson.JSON;
import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.list_privileges;
import org.cboard.dao.DatasetDao;
import org.cboard.dao.DatasourceDao;
import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.DataProviderManager;
import org.cboard.dataprovider.config.AggConfig;
import org.cboard.dataprovider.config.ConfigComponent;
import org.cboard.dataprovider.config.DimensionConfig;
import org.cboard.dataprovider.config.ValueConfig;
import org.cboard.dataprovider.result.AggregateResult;
import org.cboard.dataprovider.util.DataUtils;
import org.cboard.dto.DataProviderResult;
import org.cboard.exception.CBoardException;
import org.cboard.pojo.DashboardDataset;
import org.cboard.pojo.DashboardDatasource;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.stereotype.Repository;

import com.alibaba.fastjson.JSONObject;
import com.alibaba.fastjson.JSONPath;
import com.google.common.base.Functions;
import com.google.common.collect.Maps;
import com.googlecode.aviator.AviatorEvaluator;

/**
 * Created by yfyuan on 2016/8/15.
 */
@Repository
public class DataProviderService {

    private final Logger LOG = LoggerFactory.getLogger(this.getClass());

    @Autowired
    private DatasourceDao datasourceDao;

    @Autowired
    private DatasetDao datasetDao;

    private DataProvider getDataProvider(Long datasourceId, Map<String, String> query, Dataset dataset) throws Exception {
        if (dataset != null) {
            datasourceId = dataset.getDatasourceId();
            query = dataset.getQuery();
        }
        DashboardDatasource datasource = datasourceDao.getDatasource(datasourceId);
        JSONObject datasourceConfig = JSONObject.parseObject(datasource.getConfig());
        Map<String, String> dataSource = Maps.transformValues(datasourceConfig, Functions.toStringFunction());
        DataProvider dataProvider = DataProviderManager.getDataProvider(datasource.getType(), dataSource, query);
        if (dataset != null && dataset.getInterval() != null && dataset.getInterval() > 0) {
            dataProvider.setInterval(dataset.getInterval());
        }
        return dataProvider;
    }

    public Map<String, String> getDataSource(Long datasourceId){
        DashboardDatasource datasource = datasourceDao.getDatasource(datasourceId);
        JSONObject datasourceConfig = JSONObject.parseObject(datasource.getConfig());
        return Maps.transformValues(datasourceConfig, Functions.toStringFunction());
    }

    public AggregateResult queryAggData(Long datasourceId, Map<String, String> query, Long datasetId, AggConfig config, boolean reload) {
        try {
            Dataset dataset = getDataset(datasetId);
            attachCustom(dataset, config);
            DataProvider dataProvider = getDataProvider(datasourceId, query, dataset);
//            AggConfig aggConfig = filterCheck(config);

//            buildQuarter(config)
            AggConfig aggConfig = filterCheckV4(buildQuarter(config));
            AggregateResult aggData = dataProvider.getAggData(aggConfig, reload);
            return aggData;
        } catch (Exception e) {
            LOG.error("", e);
            throw new CBoardException("系统异常,请联系亚米网IT人员");
        }
    }

    private AggConfig filterCheckV4(AggConfig config) throws ParseException {
        Calendar calendar = Calendar.getInstance();
        List<ConfigComponent> filters = config.getFilters();

        List<DimensionConfig> filterList1 = new ArrayList<DimensionConfig>();//原始的
        DimensionConfig timeDimensionConfig = null;//看板的时期时间
        for(ConfigComponent  filter:filters){
            filterList1.add((DimensionConfig) filter);
        }
        int i = 0;//如果只有一个看板的时间条件则保留

        //不能单纯合并相同字段  因为字段相同但是条件不同
        //合并看板和模块内的时间  以看板时间为准定位近几天的时间
        //先判断是不是看板
        for(DimensionConfig info :filterList1){
            Boolean isBoard = info.getBoard();
            Boolean board  = null != isBoard && true == isBoard && info.getColumnName().contains("DIM_CAL_DATE");
            if(board){
                timeDimensionConfig = info;
            }
            if(info.getColumnName().contains("DIM_CAL_DATE") && null != info.getValues() && info.getValues().size() > 0){
                i++;
//                if(info.getValues().get(0).startsWith("{") && info.getValues().get(0).endsWith("}")){
//                    if(!board){
//                        info.setFilterType("≥");
//                    }
//                }
            }
        }

        if(null == timeDimensionConfig){//判断是否有看板时间  有则需要重新计算
            for(DimensionConfig info :filterList1){
                if(info.getColumnName().contains("DIM_CAL_DATE") && null != info.getValues() && info.getValues().size() > 0){
                    List<String> valuesList = new ArrayList<>();
                    for(String vs:info.getValues()){
                        if(vs.startsWith("{") && vs.endsWith("}")){
                            String[] split = vs.split("'");
                            String timeType = split[1];
                            String cha = split[2].replace(",", "");
                            String timeTypeS = split[3].replace("\"", "");

                            calendar.setTime(new Date());
                            if ("M".equals(timeType)) {
                                calendar.add(Calendar.MONTH, Integer.parseInt(cha));
                            }else if ("D".equals(timeType)) {
                                calendar.add(Calendar.DATE, Integer.parseInt(cha));
                            }else if ("Y".equals(timeType)) {
                                calendar.add(Calendar.YEAR, Integer.parseInt(cha));
                            }else if ("W".equals(timeType)) {
                                timeTypeS = "yyyy-ww";
                                calendar.add(Calendar.WEEK_OF_YEAR, Integer.parseInt(cha));
                            }
                            SimpleDateFormat sFilterFormat = new SimpleDateFormat(timeTypeS);
                            String format = sFilterFormat.format(calendar.getTime());
                            valuesList.add(format);
                        }else{
                            valuesList.add(vs);
                        }
                    }
                    info.getValues().clear();
                    info.getValues().addAll(valuesList);

                }
            }
            return config;
        }
        config.getFilters().clear();
        String s1 ="";
        String s2 = timeDimensionConfig.getValues().get(0);//看板时间


        if(s2.startsWith("{") && s2.endsWith("}")){//需要计算
            String[] split = s2.split("'");
            String broadTimeType = split[1];
            SimpleDateFormat sBroadFormat = new SimpleDateFormat("yyyy-MM-dd");
            calendar.setTime(sBroadFormat.parse(sBroadFormat.format(new Date())));
            String cha = split[2].replace(",", "");
            calendar.add(Calendar.DAY_OF_YEAR,Integer.parseInt(cha));
            String format = sBroadFormat.format(calendar.getTime());
            s1 = format;
            timeDimensionConfig.getValues().clear();
            timeDimensionConfig.getValues().add(s1);
        }else {
            s1 = s2;
        }

        for(DimensionConfig info :filterList1){
            if(info.getColumnName().contains("DIM_CAL_DATE")){
                if(null != info.getValues() && info.getValues().size() > 0){
                    List<String> valuesList = new ArrayList<>();
                    for(String  vs:info.getValues()){
                        if(vs.startsWith("{") && vs.endsWith("}")){
                            String[] split = vs.split("'");
                            String timeType = split[1];
                            String cha = split[2].replace(",", "");
                            String timeTypeS = split[3].replace("\"", "");
                            if(timeTypeS.equals("yyyy-W")){
                                timeTypeS = "yyyy-ww";
                            }
                            SimpleDateFormat sFilterFormat = new SimpleDateFormat(timeTypeS);
                            calendar.setTime(sFilterFormat.parse(s1));
                            if ("M".equals(timeType)) {
                                calendar.add(Calendar.MONTH, Integer.parseInt(cha));
                            }else if ("D".equals(timeType)) {
                                calendar.add(Calendar.DATE, Integer.parseInt(cha));
                            }else if ("Y".equals(timeType)) {
                                calendar.add(Calendar.YEAR, Integer.parseInt(cha));
                            }else if ("W".equals(timeType)) {
                                calendar.add(Calendar.WEEK_OF_YEAR, Integer.parseInt(cha));
                            }
//                        calendar.add(Calendar.DAY_OF_YEAR,Integer.parseInt(cha));
                            String format = sFilterFormat.format(calendar.getTime());
                            valuesList.add(format);
                            info.setFilterType("≥");
                        }else {
                            valuesList.add(vs);
                        }
                    }
                    info.getValues().clear();
                    info.getValues().addAll(valuesList);
                }
            }
        }

        if(i > 1){
//            filterList1.remove(timeDimensionConfig);
            timeDimensionConfig.setFilterType("≤");
        }

        config.getFilters().addAll(filterList1);
        return config;
    }

    //检验是否含有相同字段条件，用看板条件替换图表条件的值
    private AggConfig filterCheck(AggConfig config) throws ParseException{
    	List<DimensionConfig> filterList = new ArrayList<DimensionConfig>();
    	List<DimensionConfig> filterList2 = new ArrayList<DimensionConfig>();
		String clName="";
    	for (ConfigComponent configComponent : config.getFilters()) {
			filterList.add((DimensionConfig) configComponent);
			filterList2.add((DimensionConfig) configComponent);
		}
    	for (DimensionConfig dimensionConfig : config.getRows()) {
    		if (!dimensionConfig.getFilterType().equals("eq")) {
    			filterList.add(dimensionConfig);
			}
			filterList2.add(dimensionConfig);
		}
    	
    	//检验是否含有相同字段
    	Boolean hasNow = false;
    	for(int i=0;i<filterList2.size();i++){
    		for (int j = i+1; j < filterList2.size(); j++) {
				if (filterList2.get(i).getColumnName().equals(filterList2.get(j).getColumnName())) {
					if (filterList2.get(i).getBoard()==true) {
						clName = filterList2.get(i).getValues().get(0);
					}else if (filterList2.get(j).getBoard()==true) {
						clName = filterList2.get(j).getValues().get(0);
					}
					hasNow = true;
				}
			}
    	}
    	
    	if (hasNow) {
    		List<String> clNameList = new ArrayList<String>();
    		for (int i=0;i<filterList.size();i++) {
    			for (int j = i+1; j < filterList.size(); j++) {
    				if (filterList.get(i).getColumnName().equals(filterList.get(j).getColumnName())) {
    					if (filterList.get(i).getBoard() == null) {
        					for (String cloumn : filterList.get(i).getValues()) {
        						if (!"".equals(clName)) {
        							if (cloumn.indexOf("now") != -1) {
            							String [] strings = cloumn.split(",");
            							if (strings.length == 3) {
            								SimpleDateFormat sFormat = null;
            								String dataType = cloumn.substring(6,7);
            								if ("M".equals(dataType)) {
            									sFormat = new SimpleDateFormat("yyyy-MM");
    										}else if ("D".equals(dataType)) {
    											sFormat = new SimpleDateFormat("yyyy-MM-dd");
    										}else if ("Y".equals(dataType)) {
    											sFormat = new SimpleDateFormat("yyyy");
    										}else if ("W".equals(dataType)) {
												sFormat = new SimpleDateFormat("yyyy-WW");
											}
            								Calendar calendar = Calendar.getInstance();
            								if (clName.indexOf("now") != -1) {
            									clName = AviatorEvaluator.compile(clName.substring(1, clName.length() - 1), true).execute().toString();
            								}
            								calendar.setTime(sFormat.parse(clName));
            								String num = strings[1].replace(" ", "");
            								if ("M".equals(dataType)) {
            									calendar.add(Calendar.MONTH, Integer.parseInt(num));
    										}else if ("D".equals(dataType)) {
    											calendar.add(Calendar.DATE, Integer.parseInt(num));
    										}else if ("Y".equals(dataType)) {
    											calendar.add(Calendar.YEAR, Integer.parseInt(num));
    										}else if ("W".equals(dataType)) {
    											calendar.add(Calendar.DAY_OF_WEEK, Integer.parseInt(num));
											}
            								String newClName = sFormat.format(calendar.getTime());
            								clNameList.add(newClName);
            							}else {
            								clNameList.add(clName);
            							}
            						}else {
            							clNameList.add(cloumn);
            						}
								}
        					}
        					filterList.get(i).setValues(new ArrayList<String>());
        					filterList.get(i).setValues(clNameList);
        				}else {
        					for (String cloumn : filterList.get(j).getValues()) {
        						if (!"".equals(clName)) {
        							if (cloumn.indexOf("now") != -1) {
            							String [] strings = cloumn.split(",");
            							if (strings.length == 3) {
            								SimpleDateFormat sFormat = null;
            								String dataType = cloumn.substring(6,7);
            								if ("M".equals(dataType)) {
            									sFormat = new SimpleDateFormat("yyyy-MM");
    										}else if ("D".equals(dataType)) {
    											sFormat = new SimpleDateFormat("yyyy-MM-dd");
    										}else if ("Y".equals(dataType)) {
    											sFormat = new SimpleDateFormat("yyyy");
    										}else if ("W".equals(dataType)) {
												sFormat = new SimpleDateFormat("yyyy-WW");
											}
            								Calendar calendar = Calendar.getInstance();
            								if (clName.indexOf("now") != -1) {
            									clName = AviatorEvaluator.compile(clName.substring(1, clName.length() - 1), true).execute().toString();
            								}
            								calendar.setTime(sFormat.parse(clName));
            								String num = strings[1].replace(" ", "");
            								if ("M".equals(dataType)) {
            									calendar.add(Calendar.MONTH, Integer.parseInt(num));
    										}else if ("D".equals(dataType)) {
    											calendar.add(Calendar.DATE, Integer.parseInt(num));
    										}else if ("Y".equals(dataType)) {
    											calendar.add(Calendar.YEAR, Integer.parseInt(num));
    										}else if ("W".equals(dataType)) {
    											calendar.add(Calendar.DAY_OF_WEEK, Integer.parseInt(num));
											}
            								String newClName = sFormat.format(calendar.getTime());
            								clNameList.add(newClName);
            							}else {
            								clNameList.add(clName);
            							}
            						}else {
            							clNameList.add(cloumn);
            						}
								}
        					}
        					filterList.get(j).setValues(new ArrayList<String>());
        					filterList.get(j).setValues(clNameList);
        				}
    				}
				}
			}
		}
    	if (filterList.size()>0) {
    		List<DimensionConfig> filterList3 = new ArrayList<DimensionConfig>();
    		config.setFilters(new ArrayList<ConfigComponent>());
			for (DimensionConfig dimensionConfig : filterList) {
				if (dimensionConfig.getBoard() == null) {
					config.getFilters().add(dimensionConfig);
				}else {
					filterList3.add(dimensionConfig);
				}
			}
			for (DimensionConfig dimensionConfig : filterList3) {
				config.getFilters().add(dimensionConfig);
			}
		}

		return config;
    }

    //检验是否含有相同字段条件，用看板条件替换图表条件的值  过滤相同的valuse里的exp表达式
    private AggConfig filterCheckV2(AggConfig config){

        List<ConfigComponent> filters = config.getFilters();
        List<DimensionConfig> filterList1 = new ArrayList<DimensionConfig>();
        Map<String,DimensionConfig> map = new HashMap<>();
        for(ConfigComponent  filter:filters){
            filterList1.add((DimensionConfig) filter);
        }
        filters.clear();
        //合并filter集合中的values
        for(DimensionConfig dimensionConfig1 :filterList1){
            for(DimensionConfig dimensionConfig2 :filterList1){
                //有相同的filter
                if(dimensionConfig1.getColumnName().equals(dimensionConfig2.getColumnName())){
                    List<String> values1 = dimensionConfig1.getValues();
                    List<String> values2 = dimensionConfig2.getValues();
                    //合并相同的filter中values的值
                    for(String value :values2){
                        if(!values1.contains(value)){
                            values1.add(value);
                        }
                    }
                }
            }
            if(dimensionConfig1.getValues().size() > 0){
                map.put(dimensionConfig1.getColumnName(),dimensionConfig1);
            }
        }

        for(DimensionConfig info:map.values()){
            filters.add(info);
        }
        return config;
    }

    public DataProviderResult getColumns(Long datasourceId, Map<String, String> query, Long datasetId, boolean reload) {
        DataProviderResult dps = new DataProviderResult();
        try {
            Dataset dataset = getDataset(datasetId);
            DataProvider dataProvider = getDataProvider(datasourceId, query, dataset);
            String[] result = dataProvider.invokeGetColumn(reload);
            dps.setColumns(result);
            dps.setMsg("1");
        } catch (Exception e) {
            LOG.error("", e);
            dps.setMsg(e.getMessage());
        }
        return dps;
    }

    public String[] getDimensionValues(Long datasourceId, Map<String, String> query, Long datasetId, String columnName, AggConfig config, boolean reload) {
        try {
            Dataset dataset = getDataset(datasetId);
            attachCustom(dataset, config);
            DataProvider dataProvider = getDataProvider(datasourceId, query, dataset);
            String[] result = dataProvider.getDimVals(columnName, config, reload);
            return result;
        } catch (Exception e) {
            LOG.error("", e);
        }
        return null;
    }

    public String viewAggDataQuery(Long datasourceId, Map<String, String> query, Long datasetId, AggConfig config) {
        try {
            Dataset dataset = getDataset(datasetId);
            attachCustom(dataset, config);
            DataProvider dataProvider = getDataProvider(datasourceId, query, dataset);
            return dataProvider.getViewAggDataQuery(config);
        } catch (Exception e) {
            LOG.error("", e);
            throw new CBoardException(e.getMessage());
        }
    }

    public ServiceStatus test(JSONObject dataSource, Map<String, String> query) {
        try {
            DataProvider dataProvider = DataProviderManager.getDataProvider(
                    dataSource.getString("type"),
                    Maps.transformValues(dataSource.getJSONObject("config"), Functions.toStringFunction()),
                    query, true);
            dataProvider.test();
            return new ServiceStatus(ServiceStatus.Status.Success, null);
        } catch (Exception e) {
            LOG.error("", e);
            return new ServiceStatus(ServiceStatus.Status.Fail, e.getMessage());
        }
    }

    public boolean isDataSourceAggInstance(Long datasourceId, Map<String, String> query, Long datasetId) {
        try {
            Dataset dataset = getDataset(datasetId);
            DataProvider dataProvider = getDataProvider(datasourceId, query, dataset);
            return dataProvider.isDataSourceAggInstance();
        } catch (Exception e) {
            LOG.error("", e);
            throw new CBoardException(e.getMessage());
        }
    }

    private void attachCustom(Dataset dataset, AggConfig aggConfig) {
        if (dataset == null || aggConfig == null) {
            return;
        }
        Consumer<DimensionConfig> predicate = (config) -> {
            if (StringUtils.isNotEmpty(config.getId())) {
                String custom = (String) JSONPath.eval(dataset.getSchema(), "$.dimension[id='" + config.getId() + "'][0].custom");
                if (custom == null) {
                    custom = (String) JSONPath.eval(dataset.getSchema(), "$.dimension[type='level'].columns[id='" + config.getId() + "'][0].custom");
                }
                config.setCustom(custom);
            }
        };
        aggConfig.getRows().forEach(predicate);
        aggConfig.getColumns().forEach(predicate);
    }

    protected Dataset getDataset(Long datasetId) {
        if (datasetId == null) {
            return null;
        }
        return new Dataset(datasetDao.getDataset(datasetId));
    }

    protected class Dataset {
        private Long datasourceId;
        private Map<String, String> query;
        private Long interval;
        private JSONObject schema;

        public Dataset(DashboardDataset dataset) {
            JSONObject data = JSONObject.parseObject(dataset.getData());
            this.query = Maps.transformValues(data.getJSONObject("query"), Functions.toStringFunction());
            this.datasourceId = data.getLong("datasource");
            this.interval = data.getLong("interval");
            this.schema = data.getJSONObject("schema");
        }

        public JSONObject getSchema() {
            return schema;
        }

        public void setSchema(JSONObject schema) {
            this.schema = schema;
        }

        public Long getDatasourceId() {
            return datasourceId;
        }

        public void setDatasourceId(Long datasourceId) {
            this.datasourceId = datasourceId;
        }

        public Map<String, String> getQuery() {
            return query;
        }

        public void setQuery(Map<String, String> query) {
            this.query = query;
        }

        public Long getInterval() {
            return interval;
        }

        public void setInterval(Long interval) {
            this.interval = interval;
        }
    }

    //构建季度时间
    private AggConfig buildQuarter(AggConfig config) {
        for(int i = 0 ;i<config.getFilters().size();i++){
            DimensionConfig info = (DimensionConfig) config.getFilters().get(i);
            if(info.getColumnName().contains("DIM_CAL_DATE.QUARTER_ID") && null != info.getValues() && info.getValues().size() > 0) {
                String[] split = info.getValues().get(0).split("'");
//                boolean q = config.getValues().get(0).equals("Q");
                String cha = split[2].replace(",", "");
//                int chaYear = Integer.parseInt(cha) / 4;
//                int chaQuarter = Integer.parseInt(cha) % 4;
//                int year = Integer.parseInt(DataUtils.getSysYear()) - chaYear;
//                int quarter = DataUtils.getSeason(new Date()) - chaQuarter;
                int year = Integer.parseInt(DataUtils.getSysYear());
                int quarter = DataUtils.getSeason(new Date()) + Integer.parseInt(cha);
                if(quarter < 0){
                    year = year - 1;
                    quarter = 4 - quarter;
                }
                if(quarter == 0){
                    quarter = 1;
                }
                info.getValues().clear();
                info.getValues().add(year + "-" +quarter);
            }
        }
        return config;
    }
}
