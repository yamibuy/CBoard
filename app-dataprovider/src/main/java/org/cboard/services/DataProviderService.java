package org.cboard.services;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.ArrayList;
import java.util.Calendar;
import java.util.Date;
import java.util.List;
import java.util.Map;
import java.util.function.Consumer;

import org.apache.commons.lang3.StringUtils;
import org.apache.hadoop.hive.metastore.api.ThriftHiveMetastore.Processor.list_privileges;
import org.cboard.dao.DatasetDao;
import org.cboard.dao.DatasourceDao;
import org.cboard.dataprovider.DataProvider;
import org.cboard.dataprovider.DataProviderManager;
import org.cboard.dataprovider.config.AggConfig;
import org.cboard.dataprovider.config.ConfigComponent;
import org.cboard.dataprovider.config.DimensionConfig;
import org.cboard.dataprovider.result.AggregateResult;
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
            filterCheck(config);
            return dataProvider.getAggData(config, reload);
        } catch (Exception e) {
            LOG.error("", e);
            throw new CBoardException("系统异常,请联系亚米网IT人员");
        }
    }
    
    //检验是否含有相同字段条件，用看板条件替换图表条件的值
    private void filterCheck(AggConfig config) throws ParseException{
    	List<DimensionConfig> filterList = new ArrayList<DimensionConfig>();
    	List<DimensionConfig> filterList2 = new ArrayList<DimensionConfig>();
		String clName="";
    	for (ConfigComponent configComponent : config.getFilters()) {
			filterList.add((DimensionConfig) configComponent);
			filterList2.add((DimensionConfig) configComponent);
		}
    	for (DimensionConfig dimensionConfig : config.getRows()) {
			filterList2.add(dimensionConfig);
		}
    	
    	//检验是否含有相同字段
    	Boolean hasNow = false;
    	for(int i=0;i<filterList2.size();i++){
    		for (int j = i+1; j < filterList2.size(); j++) {
				if (filterList2.get(i).getColumnName().equals(filterList2.get(j).getColumnName())) {
					if (filterList2.get(i).getIsBoard()=="true") {
						clName = filterList2.get(i).getValues().get(0);
					}else if (filterList2.get(j).getIsBoard()=="true") {
						clName = filterList2.get(j).getValues().get(0);
					}
					hasNow = true;
				}
			}
    	}
    	
    	if (hasNow) {
    		for (DimensionConfig dimensionConfig : config.getRows()) {
        		if (!dimensionConfig.getFilterType().equals("eq")) {
        			filterList.add(dimensionConfig);
    			}
    		}
    		List<String> clNameList = new ArrayList<String>();
    		for (int i=0;i<filterList.size();i++) {
    			for (int j = i+1; j < filterList.size(); j++) {
    				if (filterList.get(i).getColumnName().equals(filterList.get(j).getColumnName())) {
    					if (filterList.get(i).getIsBoard() == null) {
        					for (String cloumn : filterList.get(i).getValues()) {
        						if (cloumn.indexOf("now") != -1) {
        							String [] strings = cloumn.split(",");
        							if (strings.length == 3) {
        								SimpleDateFormat sFormat = new SimpleDateFormat("yyyy-MM-dd");
        								Calendar calendar = Calendar.getInstance();
        								if (clName.indexOf("now") != -1) {
        									clName = AviatorEvaluator.compile(clName.substring(1, clName.length() - 1), true).execute().toString();
        								}
        								calendar.setTime(sFormat.parse(clName));
        								String num = strings[1].replace(" ", "");
        								calendar.add(Calendar.DATE, Integer.parseInt(num));
        								String newClName = sFormat.format(calendar.getTime());
        								clNameList.add(newClName);
        							}else {
        								clNameList.add(clName);
        							}
        						}else {
        							clNameList.add(cloumn);
        						}
        					}
        					filterList.get(i).setValues(new ArrayList<String>());
        					filterList.get(i).setValues(clNameList);
        				}else {
        					for (String cloumn : filterList.get(j).getValues()) {
        						if (cloumn.indexOf("now") != -1) {
        							String [] strings = cloumn.split(",");
        							if (strings.length == 3) {
        								SimpleDateFormat sFormat = new SimpleDateFormat("yyyy-MM-dd");
        								Calendar calendar = Calendar.getInstance();
        								if (clName.indexOf("now") != -1) {
        									clName = AviatorEvaluator.compile(clName.substring(1, clName.length() - 1), true).execute().toString();
        								}
        								calendar.setTime(sFormat.parse(clName));
        								String num = strings[1].replace(" ", "");
        								calendar.add(Calendar.DATE, Integer.parseInt(num));
        								String newClName = sFormat.format(calendar.getTime());
        								clNameList.add(newClName);
        							}else {
        								clNameList.add(clName);
        							}
        						}else {
        							clNameList.add(cloumn);
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
				if (dimensionConfig.getIsBoard() == null) {
					config.getFilters().add(dimensionConfig);
				}else {
					filterList3.add(dimensionConfig);
				}
			}
			for (DimensionConfig dimensionConfig : filterList3) {
				config.getFilters().add(dimensionConfig);
			}
		}
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
}
