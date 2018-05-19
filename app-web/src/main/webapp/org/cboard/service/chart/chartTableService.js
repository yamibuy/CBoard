/**
 * Created by yfyuan on 2016/10/28.
 */
'use strict';
cBoard.service('chartTableService', function () {

    this.render = function (containerDom, option, scope, persist, drill) {
        if (option == null) {
            containerDom.html('<div style="min-height:300px;line-height:300px;" >No Data!</div>');
            return;
        }
        var height;
        scope ? height = scope.myheight : null;
        console.log(option);
        return new CBoardTableRender(containerDom, option, drill).do(height, persist);
    };

    this.parseOption = function (data) {
        var tableOption = chartDataProcess(data.chartConfig, data.keys, data.series, data.data, data.seriesConfig);
        return tableOption;
    };
});