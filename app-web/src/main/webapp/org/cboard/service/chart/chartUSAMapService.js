/**
 * Created by jintian on 2017/8/22.
 */
'use strict';
cBoard.service('chartUSAMapService', function () {
    this.render = function (containerDom, option, scope, persist,drill) {
        if (option == null) {
            containerDom.html('<div style="min-height:300px;line-height:300px;" >No Data!</div>');
            return;
        }
        var height;
        scope ? height = scope.myheight - 20 : null;
        return new CBoardHeatMapRender(containerDom, option).chart(height, persist);
    };

    this.parseOption = function (data) {
        var optionData = [];
        var seriesData = [];
        var data_keys = data.keys;
        var data_series = data.series;
        var chartConfig = data.chartConfig;
        var code = 'usa';
        // if (chartConfig.city && chartConfig.city.code) {
        //     code = chartConfig.city.code;
        // } else if (chartConfig.province && chartConfig.province.code) {
        //     code = chartConfig.province.code;
        // }

        var url;
        if (code == 'usa') {
            url = 'plugins/FineMap/mapdata/usa.json';
        } else if (code.length > 2) {
            url = 'plugins/FineMap/mapdata/geometryCouties/' + code + '.json';
        } else {
            url = 'plugins/FineMap/mapdata/geometryProvince/' + code + '.json';
        }

        var fromName;
        var fromN;
        var fromL;
        var toName;
        var toN;
        var toL;
        var max;
        var min;
        var j = 0;
        var maxScatter;
        for(var serieConfig in data.seriesConfig){
            var serieType = data.seriesConfig[serieConfig].type;
            //重置为null，防止脏数据
            fromName = null;
            fromN = null;
            fromL = null;
            if(data_series[j].length > 3){
                fromName = data_series[j][2];
                fromN = parseFloat(data_series[j][0]);
                fromL = parseFloat(data_series[j][1]);
            }else if(data_series[j].length == 3){
                fromName = data_series[j][1];
                fromN = parseFloat(data_series[j][0].split(",")[0]);
                fromL = parseFloat(data_series[j][0].split(",")[1]);
            }

            //根据不同的地图类型获取不同的series
            switch (serieType){
                case "markLine" :
                    var lineData = [];
                    if(fromN && fromL){
                        for(var i = 0; data_keys[0] && i < data_keys.length; i++){
                            toName = null;
                            toN = null;
                            toL = null;
                            if(data_keys[i].length > 2){
                                toName = data_keys[i][2];
                                toN = parseFloat(data_keys[i][0]);
                                toL = parseFloat(data_keys[i][1]);
                            }else if(data_keys[i].length == 2){
                                toName = data_keys[i][1];
                                toN = parseFloat(data_keys[i][0].split(",")[0]);
                                toL = parseFloat(data_keys[i][0].split(",")[1]);
                            }

                            if(data.data[j][i] && toN && toL){
                                lineData.push({fromName: fromName,
                                    toName: toName,
                                    coords: [[fromN,fromL],
                                        [toN, toL]]
                                });

                                if(max == null || max <= parseFloat(data.data[j][i])){
                                    max = parseFloat(data.data[j][i]) + 10;
                                }
                                if(min == null || min >= parseFloat(data.data[j][i])){
                                    min = parseFloat(data.data[j][i]) - 10;
                                }
                            }
                        }

                        if(lineData.length > 0){
                            seriesData.push(
                                {
                                    name:fromName,
                                    type: 'lines',
                                    coordinateSystem: 'geo',
                                    symbol: ['none', 'arrow'],
                                    symbolSize: 6,
                                    effect: {
                                        show: true,
                                        period: 6,
                                        trailLength: 0,
                                        symbol: 'arrow',
                                        symbolSize: 4
                                    },
                                    lineStyle: {
                                        normal: {
                                            width: 1,
                                            opacity: 0.6,
                                            curveness: 0.2
                                        }
                                    },
                                    data: lineData
                                }
                            );
                            optionData.push(fromName);
                        }
                    }
                    break;

                case "heat" :
                    var heatmapData = [];
                    for(var i = 0; data_keys[0] && i < data_keys.length; i++){
                        toName = null;
                        toN = null;
                        toL = null;
                        if(data_keys[i].length > 2){
                            toName = data_keys[i][2];
                            toN = parseFloat(data_keys[i][0]);
                            toL = parseFloat(data_keys[i][1]);
                        }else if(data_keys[i].length == 2){
                            toName = data_keys[i][1];
                            toN = parseFloat(data_keys[i][0].split(",")[0]);
                            toL = parseFloat(data_keys[i][0].split(",")[1]);
                        }

                        if(data.data[j][i]){
                            heatmapData.push([toN,toL,parseFloat(data.data[j][i])]);

                            if(max == null || max <= parseFloat(data.data[j][i])){
                                max = parseFloat(data.data[j][i]) + 10;
                            }
                            if(min == null || min >= parseFloat(data.data[j][i])){
                                min = parseFloat(data.data[j][i]) - 10;
                            }
                        }
                    }

                    if(heatmapData.length > 0){
                        seriesData.push(
                            {
                                name: serieConfig,
                                type: 'heatmap',
                                mapType: code,
                                coordinateSystem: 'geo',
                                data: heatmapData
                            }
                        );
                        optionData.push(serieConfig);
                    }
                    break;

                case "scatter" :
                    var scatterData = [];
                    for(var i = 0; data_keys[0] && i < data_keys.length; i++){
                        toName = null;
                        toN = null;
                        toL = null;
                        if(data_keys[i][0].length > 2){
                            toName = data_keys[i][0];
                            toN = parseFloat(data_keys[i][0]);
                            toL = parseFloat(data_keys[i][1]);
                        }

                        if(data.data[j][i]){
                            scatterData.push({
                                name:toName,
                                value: parseFloat(data.data[j][i])
                            });
                            if(maxScatter == null || maxScatter < parseFloat(data.data[j][i])){
                                maxScatter = parseFloat(data.data[j][i]);
                            }
                            max = max>1000?1000:max;
                            if(max == null || max <= parseFloat(data.data[j][i])){
                                max = parseFloat(data.data[j][i]) + 10;
                            }
                            if(min == null || min >= parseFloat(data.data[j][i])){
                                min = parseFloat(data.data[j][i]) - 10;
                            }
                            min = min>0?min:0;
                        }
                    }

                    if(scatterData.length > 0){

                        seriesData.push(
                            {
                                name: serieConfig,
                                coordinateSystem:"geo",
                                type: 'map',
                                roam: false,
                                map: 'usa',
                                itemStyle:{
                                    emphasis:{label:{show:true}}
                                },
                                // 文本位置修正
                                textFixed: {
                                    Alaska: [20, -20]
                                },
                                data: scatterData,
                                showLegendSymbol: false,
                                label: {
                                    normal: {
                                        formatter: '{b}',
                                        position: 'right',
                                        show: false
                                    },
                                    emphasis: {
                                        show: true
                                    }
                                }
                            }
                        );
                        optionData.push(serieConfig);
                    }
            }
            j++;
        }

        var mapOption;

        $.ajax({
            type: "get",
            url: url,
            async: false,
            //type:'json',
            success: function (cityJson) {
                // cityJson = seriesData.length>0?cityJson:'';
                echarts.registerMap(code, cityJson,{
                    'Alaska': {              // 把阿拉斯加移到美国主大陆左下方
                        left: -131,
                        top: 25,
                        width: 15
                    },
                    'Hawaii': {
                        left: -110,        // 夏威夷
                        top: 28,
                        width: 5
                    },
                    'Puerto Rico': {       // 波多黎各
                        left: -76,
                        top: 26,
                        width: 2
                    }
                });
                mapOption = {
                    legend: {
                        orient: 'vertical',
                        top: 'top',
                        left: 'left',
                        selectedMode: 'multiple',
                        data: optionData
                    },
                    visualMap: {
                        show:seriesData.length>0?true:false,
                        min: min,
                        max: max,
                        left: 'right',
                        top: 'bottom',
                        //text: ['High', 'Low'],
                        inRange: {
                            // color: ['#d94e5d','#eac736','#50a3ba'].reverse()
                            color: ['orangered','yellow','lightskyblue'].reverse()
                        },
                        calculable : true,
                        textStyle: {
                            color: '#d94e5d'
                        }
                    },
                    geo: {
                        map: code,
                        show:true,
                        label: {
                            emphasis: {
                                show: false
                            }
                        },
                        itemStyle: {
                            normal: {
                                areaColor: '#EFF0F0',
                                borderColor: '#B5B5B5',
                                borderWidth: 1
                            }
                        }
                    },
                    tooltip: {
                        trigger: 'item',
                        formatter: '{b0}: {c0}'
                    },
                    series:seriesData
                };
                if(seriesData.length==0){
                    mapOption.title = {
                        show: true,
                        textStyle:{
                            color:'rgba(0,0,0,.4)',
                            fontSize:12,
                            fontFamily: 'Arial, Verdana, sans-serif',
                            fontStyle: 'normal'
                        },
                        text: 'No Data!',
                        left: 'center',
                        top: 'bottom',
                    };
                    mapOption.xAxis = {show : false};
                    mapOption.yAxis= {show : false};
                }else{
                    //单独处理{name: 'Puerto Rico', value: 3667084}{name: 'Alaska', value: 731449},
                    var  allCityList = [{name: 'Alabama', value: 0},
                        {name: 'Alaska', value: 0},
                        {name: 'Arizona', value: 0},
                        {name: 'Arkansas', value: 0},
                        {name: 'California', value: 0},
                        {name: 'Colorado', value: 0},
                        {name: 'Connecticut', value: 0},
                        {name: 'Delaware', value: 0},
                        {name: 'District of Columbia', value: 0},
                        {name: 'Florida', value: 0},
                        {name: 'Georgia', value: 0},
                        {name: 'Hawaii', value: 0},
                        {name: 'Idaho', value: 0},
                        {name: 'Illinois', value: 0},
                        {name: 'Indiana', value: 0},
                        {name: 'Iowa', value: 0},
                        {name: 'Kansas', value: 0},
                        {name: 'Kentucky', value: 0},
                        {name: 'Louisiana', value: 0},
                        {name: 'Maine', value: 0},
                        {name: 'Maryland', value: 0},
                        {name: 'Massachusetts', value: 0},
                        {name: 'Michigan', value: 0},
                        {name: 'Minnesota', value: 0},
                        {name: 'Mississippi', value: 0},
                        {name: 'Missouri', value: 0},
                        {name: 'Montana', value: 0},
                        {name: 'Nebraska', value: 0},
                        {name: 'Nevada', value: 0},
                        {name: 'New Hampshire', value: 0},
                        {name: 'New Jersey', value: 0},
                        {name: 'New Mexico', value: 0},
                        {name: 'New York', value: 0},
                        {name: 'North Carolina', value: 0},
                        {name: 'North Dakota', value: 0},
                        {name: 'Ohio', value: 0},
                        {name: 'Oklahoma', value: 0},
                        {name: 'Oregon', value: 0},
                        {name: 'Pennsylvania', value: 0},
                        {name: 'Rhode Island', value: 0},
                        {name: 'South Carolina', value: 0},
                        {name: 'South Dakota', value: 0},
                        {name: 'Tennessee', value: 0},
                        {name: 'Texas', value: 0},
                        {name: 'Utah', value: 0},
                        {name: 'Vermont', value: 0},
                        {name: 'Virginia', value: 0},
                        {name: 'Washington', value: 0},
                        {name: 'West Virginia', value: 0},
                        {name: 'Wisconsin', value: 0},
                        {name: 'Wyoming', value: 0},
                        {name: 'Puerto Rico', value: 0}];
                    var allCityLength = allCityList.length;
                    var mapData = mapOption.series[0].data;
                    var mapLength = mapData.length;
                    for(var p = 0; p<allCityLength;p++){
                        for(var q=0;q<mapLength;q++){
                            if(allCityList[p].name == mapData[q].name ){
                                allCityList[p].value = mapData[q].value;
                                break;
                            }
                        }
                    }
                    mapOption.series[0].data = allCityList;
                    if(chartConfig.option.visualMap && chartConfig.option.visualMap.max && chartConfig.option.visualMap.min){
                        if(chartConfig.option.visualMap.max>chartConfig.option.visualMap.min){
                            mapOption.visualMap.max = Math.floor(chartConfig.option.visualMap.max);
                            mapOption.visualMap.min = Math.floor(chartConfig.option.visualMap.min);
                        }
                    }else{
                        if( chartConfig.option.visualMap &&  chartConfig.option.visualMap.max && chartConfig.option.visualMap.max>=0){
                            mapOption.visualMap.max = Math.floor(chartConfig.option.visualMap.max);
                        }
                        if( chartConfig.option.visualMap &&  chartConfig.option.visualMap.min && chartConfig.option.visualMap.min>=0){
                            mapOption.visualMap.min = Math.floor(chartConfig.option.visualMap.min);
                        }
                    }
                }
            }
        });
        return mapOption;
    };
});
