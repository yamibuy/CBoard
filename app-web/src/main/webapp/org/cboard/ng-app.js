/**
 * Created by Peter on 2016/10/22.
 */

var cBoard = angular.module('cBoard', ['ui.router', 'angular-md5', 'dndLists', 'treeControl',
    'ui.bootstrap', 'ngSanitize', 'ui.select', 'pascalprecht.translate', 'ui.ace', 'ngJsTree', 'daterangepicker', 'angular-cron-jobs', 'rzModule','uuid4']);
cBoard.provider('userService',function(){
    var data = {
        datasetList: []
    };
    var f = function (datasetList) {
        if (datasetList.length > 0)
        {
            data.datasetList = datasetList
        }
        return data;
    };
    this.$get = function () {
        return f;
    };
});