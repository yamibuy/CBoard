/**
 * Created by yfyuan on 2016/8/2.
 */

cBoard.controller('dashboardViewCtrl', function ($timeout, $rootScope, $scope, $state, $stateParams, $http, ModalUtils, chartService, $interval, $uibModal, dataService, userService) {
    $scope.loading = true;
    $rootScope.loadingData = true;
    $scope.paramInit = 0;
    $scope.relations = JSON.stringify([]);
    $http.get("dashboard/getDatasetList.do").success(function (response) {
        $scope.datasetList = response;
        userService(response);
        $scope.realtimeDataset = {};
        $scope.datasetMeta = {};
        $scope.intervals = [];
        $scope.datasetFilters = {};
        $scope.widgetFilters = {};
        $scope.relationFilters = {};
        $scope.load(false);
    });

    $scope.timelineColor = ['bg-light-blue', 'bg-red', 'bg-aqua', 'bg-green', 'bg-yellow', 'bg-gray', 'bg-navy', 'bg-teal', 'bg-purple', 'bg-orange', 'bg-maroon', 'bg-black'];

    var wLength = 0;

    var groupTimeline = function () {
        $scope.timeline = [];
        var group = undefined;
        _.each($scope.board.layout.rows, function (row, idx) {
            if (idx == 0) {
                $scope.timelineFilter = row;
                return;
            }
            row.show = false;
            if (row.node == 'parent') {
                if (group) {
                    $scope.timeline.push(group);
                }
                group = [];
                row.show = true;
            }
            group.push(row);
        });
        $scope.timeline.push(group);
    };

    $scope.openCloseParentNode = function (group) {
        var find = _.find(group, function (row) {
            return row.node != 'parent' && row.show;
        });
        if (find) {
            _.each(group, function (row) {
                if (row.node != 'parent') {
                    row.show = false;
                    _.each(row.widgets, function (widget) {
                        widget.show = false;
                    });
                }
            });
        } else {
            _.each(group, function (row) {
                if (row.node != 'parent') {
                    row.show = true;
                    _.each(row.widgets, function (widget) {
                        widget.show = true;
                    });
                }
            });
        }
    };

    $scope.openCloseNode = function (row) {
        if (row.show) {
            row.show = false;
            _.each(row.widgets, function (widget) {
                widget.show = false;
            });
        } else {
            row.show = true;
            _.each(row.widgets, function (widget) {
                widget.show = true;
            });
        }
    };

    $http.post("admin/isConfig.do", {type: 'widget'}).success(function (response) {
        $scope.widgetCfg = response;
    });

    var buildRender = function (widget, reload) {
        var widgetConfig = injectFilter(widget.widget).data;
        widget.render = function (container, optionFilter, scope) {
            // 百度地图特殊处理
            var charType = injectFilter(widget.widget).data.config.chart_type;
            if(charType == 'chinaMapBmap'){
                chartService.renderChart(container, widgetConfig, {
                    optionFilter: optionFilter,
                    scope: scope,
                    reload: reload
                }).then(
                    function() {
                        widget.loading = false;
                    }
                );
            } else {
                chartService.renderChart(container, widgetConfig, {
                    optionFilter: optionFilter,
                    scope: scope,
                    reload: reload,
                    relations: widget.relations
                }).then(function (d) {
                    widget.realTimeTicket = d;
                    widget.loading = false;
                    $rootScope.loadingData = false;
                });
            }
            widget.realTimeOption = {optionFilter: optionFilter, scope: scope};
        };
        widget.modalRender = function (container, optionFilter, scope) {
            widget.modalLoading = true;
            widget.modalRealTimeTicket = chartService.renderChart(container, widgetConfig, {
                optionFilter: optionFilter,
                scope: scope
            }).then(function () {
                widget.modalLoading = false;
            });
            widget.modalRealTimeOption = {optionFilter: optionFilter, scope: scope};
        };
    };

    $rootScope.$on('$stateChangeSuccess', function (event, toState, toParams, fromState) {
            if (fromState.controller == 'dashboardViewCtrl') {
                _.each($scope.intervals, function (i) {
                    $interval.cancel(i);
                })
            }
        }
    );

    $scope.export = function () {
        if ($scope.exportStatus) {
            return;
        }
        $scope.exportStatus = true;
        var filters = [];
        _.forEach($scope.board.layout.rows,function(v,i){
            if(v.type === 'param'){
                var par = {};
                _.forEach(v.params,function(val,j){
                    par.values = val.values;
                    par.type = val.type;
                    par.column = [];
                    _.forEach(val.col,function(value,k){
                        par.column.push(value.column);
                    });
                });
                filters.push(par);
            }
        })
        $http({
            url: "dashboard/exportBoardV1.do",
            method: "POST",
            headers: {
                'Content-type': 'application/json'
            },
            params: {
                id: $stateParams.id,
                filters: JSON.stringify(filters)
            },
            responseType: 'arraybuffer'
        }).success(function (data) {
            var blob = new Blob([data], {type: "application/vnd.ms-excel"});
            var objectUrl = URL.createObjectURL(blob);
            var aForExcel = $("<a><span class='forExcel'>下载excel</span></a>").attr("href", objectUrl);
            aForExcel.attr("download", $scope.board.name);
            $("body").append(aForExcel);
            $(".forExcel").click();
            aForExcel.remove();
            $scope.exportStatus = false;
        }).error(function (data, status, headers, config, statusText) {
            $scope.exportStatus = false;
            ModalUtils.alert("Export error, please ask admin to check server side log.", "modal-warning", "lg");
        });
    };

    var refreshParam = function () {
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.params, function (param) {
                if (param.refresh) {
                    param.refresh();
                }
            });
        });
        paramToFilter();
    };

    var initDsReloadStatus = function(reload) {
        var dsReloadStatus = new Map();
        _.each($scope.board.layout.rows, function(row) {
            _.each(row.widgets, function (widget) {
                var dataSetId = widget.widget.data.datasetId;
                if (dataSetId != undefined) {
                    dsReloadStatus.set(dataSetId, reload);
                }
            });
        });
        return dsReloadStatus;
    };
    var loadWidget = function (reload) {
        paramToFilter();
        var dsReloadStatus = initDsReloadStatus(reload);
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.widgets, function (widget) {
                wLength++;
                if (!_.isUndefined(widget.hasRole) && !widget.hasRole) {
                    return;
                }
                var dataSetId = widget.widget.data.datasetId;
                var needReload = reload;
                // avoid repeat load offline dataset data
                if (dataSetId != undefined && reload) {
                    var needReload = dsReloadStatus.get(dataSetId) ? true : false;
                    dsReloadStatus.set(dataSetId, false);
                }
                buildRender(widget, needReload);
                widget.loading = true;
                if ($scope.board.layout.type == 'timeline') {
                    if (row.show) {
                        widget.show = true;
                    }
                } else {
                    widget.show = true;
                }

                //real time load task
                var w = widget.widget.data;
                var ds = _.find($scope.datasetList, function (e) {
                    return e.id == w.datasetId;
                });
                if (ds && ds.data.interval && ds.data.interval > 0) {
                    if (!$scope.intervalGroup[w.datasetId] && !widget.sourceId) {
                        $scope.intervalGroup[w.datasetId] = [];
                        $scope.intervals.push($interval(function () {
                            refreshParam();
                            _.each($scope.intervalGroup[w.datasetId], function (e) {
                                e();
                            });
                        }, ds.data.interval * 1000));
                    }
                    $scope.intervalGroup[w.datasetId].push(function () {
                        try {
                            if (widget.show) {
                                chartService.realTimeRender(widget.realTimeTicket, injectFilter(widget.widget).data);
                                if (widget.modalRealTimeTicket) {
                                    chartService.realTimeRender(widget.modalRealTimeTicket, injectFilter(widget.widget).data, widget.modalRealTimeOption.optionFilter, null);
                                }
                            }
                        } catch (e) {
                            console.error(e);
                        }
                    });
                }

            });
        });
    };

    var paramInitListener;

    $scope.load = function (reload) {
        $scope.paramInit = 0;
        $scope.loading = true;
        $rootScope.loadingData = true;
        $("#relations").val(JSON.stringify([]));
        _.each($scope.intervals, function (e) {
            $interval.cancel(e);
        });
        $scope.intervals = [];

        if ($scope.board) {
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.widgets, function (widget) {
                    widget.show = false;
                });
            });
        }
        $http.get("dashboard/getBoardData.do?id=" + $stateParams.id).success(function (response) {
            $scope.intervalGroup = {};
            $scope.loading = false;
            $scope.board = response;
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.params, function (param) {
                    if (!param.paramType) {
                        param.paramType = 'selector';
                    }
                });
            });
            if (paramInitListener) {
                paramInitListener(reload);
            }
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.params, function (param) {
                    $scope.paramInit++;
                });
            });
            if ($scope.board.layout.type == 'timeline') {
                groupTimeline();
            }
            if ($scope.paramInit == 0) {
                loadWidget(reload);
            }
            paramInitListener = $scope.$on('paramInitFinish', function (e, d) {
                $scope.paramInit--;
                if ($scope.paramInit == 0) {
                    loadWidget(reload)
                }
            });
        });
    };

    var injectFilter = function (widget) {
        var boardFilters = [];
        if(!_.isUndefined($scope.widgetFilters[widget.id])){
            _.each($scope.widgetFilters[widget.id], function(e){
                boardFilters.push(e);
            });
        }
        if(!_.isUndefined($scope.datasetFilters[widget.data.datasetId])){
            _.each($scope.datasetFilters[widget.data.datasetId], function(e){
                boardFilters.push(e);
            });
        }
        if(!_.isUndefined($scope.relationFilters[widget.id])){
            _.each($scope.relationFilters[widget.id], function(e){
                boardFilters.push(e);
            });
        }
        widget.data.config.boardFilters = boardFilters;
        return widget;
    };

    var paramToFilter = function () {
        $scope.widgetFilters = [];
        $scope.datasetFilters = [];
        $scope.relationFilters = [];

        //将点击的参数赋值到看板上的参数中
        //"{"targetId":3,"params":[{"targetField":"logo","value":"iphone"},{"targetField":"logo1","value":"上海市"}]}" targetField==param.name
        if(location.href.split("?")[1]) {
            var urlParam = JSON.parse(decodeURI(location.href.split("?")[1]));
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.params, function (param) {
                    var p = _.find(urlParam.params, function (e) {
                        return e.targetField == param.name;
                    });
                    if(p){
                        param.values.push(p.value);
                    }
                });
            });
            location.href = location.href.split("?")[0];
        }

        _.each($scope.board.layout.rows, function (row) {
            _.each(row.params, function (param) {
                if (param.values.length <= 0) {
                    return;
                }
                _.each(param.col, function (col) {
                    var p = {
                        col: col.column,
                        type: param.type,
                        values: param.values
                    };
                    if (_.isUndefined(col.datasetId)) {
                        if (!$scope.widgetFilters[col.widgetId]) {
                            $scope.widgetFilters[col.widgetId] = [];
                        }
                        $scope.widgetFilters[col.widgetId].push(p);
                    } else {
                        if (!$scope.datasetFilters[col.datasetId]) {
                            $scope.datasetFilters[col.datasetId] = [];
                        }
                        $scope.datasetFilters[col.datasetId].push(p);
                    }
                });
            });
        });
        updateParamTitle();
        //将点击的参数赋值到relationFilters中
        if(_.isUndefined($("#relations").val())){
            return;
        }
        var relations = JSON.parse($("#relations").val());
        for(var i=0;i<relations.length;i++){
            if(relations[i].targetId && relations[i].params && relations[i].params.length>0){
                for(var j=0;j<relations[i].params.length;j++) {
                    var p = {
                        col: relations[i].params[j].targetField,
                        type: "=",
                        values: [relations[i].params[j].value]
                    };
                    if (!$scope.relationFilters[relations[i].targetId]) {
                        $scope.relationFilters[relations[i].targetId] = [];
                    }
                    $scope.relationFilters[relations[i].targetId].push(p); //relation.targetId == widgetId
                }
            }
        }
    };

    $scope.applyParamFilter = function () {
        $rootScope.loadingData = true;
        paramToFilter();
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.widgets, function (w) {
                try {
                    chartService.realTimeRender(w.realTimeTicket, injectFilter(w.widget).data, null, $scope, w, true);
                } catch (e) {
                    $rootScope.loadingData = false;
                    console.error(e);
                }
            });
        });
    };

    $scope.paramToString = function (row) {
        return _.filter(_.map(row.params, function (e) {
            return e.title;
        }), function (e) {
            return e && e.length > 0;
        }).join('; ');
    };

    $scope.modalChart = function (widget) {
        $uibModal.open({
            templateUrl: 'org/cboard/view/util/modal/chart.html',
            windowTemplateUrl: 'org/cboard/view/util/modal/window.html',
            windowClass: 'modal-fit',
            backdrop: false,
            controller: function ($scope, $uibModalInstance, chartService) {
                $scope.widget = widget;
                $scope.close = function () {
                    $uibModalInstance.close();
                    delete widget.modalRealTimeTicket;
                    delete widget.modalRealTimeOption;
                };
                $scope.render1 = function () {
                    widget.modalRender($('#modal_chart'), function (option) {
                        option.toolbox = {
                            feature: {
                                //saveAsImage: {},
                                dataView: {
                                    show: true,
                                    readOnly: true
                                },
                                magicType: {
                                    type: ['line', 'bar', 'stack', 'tiled']
                                },
                                dataZoom: {
                                    show: true
                                },
                                restore: {
                                    show: true
                                }
                            }
                        };
                    }, null);
                };
            }
        });
    };

    $scope.modalTable = function (widget) {
        $uibModal.open({
            templateUrl: 'org/cboard/view/util/modal/chart.html',
            windowTemplateUrl: 'org/cboard/view/util/modal/window.html',
            windowClass: 'modal-fit',
            backdrop: false,
            controller: function ($scope, $uibModalInstance, chartService) {
                $scope.widget = widget;
                $scope.close = function () {
                    $uibModalInstance.close();
                };
                $scope.render1 = function () {
                    widget.modalRender($('#modal_chart'), null, null);
                };
            }
        });
    };

    $scope.config = function (widget) {
        $state.go('config.widget', {id: widget.widget.id});
    };

    $scope.reload = function (widget) {
        $rootScope.loadingData = true;
        paramToFilter();
        widget.widget.data = injectFilter(widget.widget).data;
        widget.show = false;
        widget.showDiv = true;
        widget.render = function (content, optionFilter, scope) {
            //百度地图特殊处理
            var charType = widget.widget.data.config.chart_type;
            var widgetConfig = widget.widget.data;
            if(charType == 'chinaMapBmap'){
                chartService.renderChart(content, widgetConfig, {
                    optionFilter: optionFilter,
                    scope: scope,
                    reload: true
                });
                widget.loading = false;
                $rootScope.loadingData = false;
            } else {
                chartService.renderChart(content, widgetConfig, {
                    optionFilter: optionFilter,
                    scope: scope,
                    reload: true,
                    relations: widget.relations
                }).then(function (d) {
                    widget.realTimeTicket = d;
                    widget.loading = false;
                    $rootScope.loadingData = false;
                });
            }
            widget.realTimeOption = {optionFilter: optionFilter, scope: scope};
        };
        $timeout(function () {
            widget.loading = true;
            widget.show = true;
        });
    };

    $http.get("dashboard/getBoardParam.do?boardId=" + $stateParams.id).success(function (response) {
        if (response) {
            $scope.boardParams = JSON.parse(response.config);
        } else {
            $scope.boardParams = [];
        }
    });

    $scope.newBoardParam = function (name) {
        if (name == '') {
            return;
        }
        var params = {};
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.params, function (param) {
                if ('slider' != param.paramType) {
                    params[param.name] = {type: param.type, values: param.values};
                }
            });
        });
        $scope.boardParams.unshift({name: name, params: params});
        $http.post("dashboard/saveBoardParam.do", {
            boardId: $stateParams.id,
            config: angular.toJson($scope.boardParams)
        }).success(function (response) {
        });
    };

    $scope.editBoard = function() {
        $state.go('config.board', {boardId: $stateParams.id});
    };

    $scope.deleteBoardParam = function (index) {
        $scope.boardParams.splice(index,1);
        $http.post("dashboard/saveBoardParam.do", {
            boardId: $stateParams.id,
            config: angular.toJson($scope.boardParams)
        }).success(function (response) {
        });
    };

    $scope.applyBoardParam = function (param) {
        for (var name in param) {
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.params, function (p) {
                    if (p.name == name) {
                        p.type = param[name].type;
                        p.values = param[name].values;
                    }
                });
            });
        }
        $scope.applyParamFilter();
    };
    var dropList = {
        Y: [
            {
                key: "{now('Y',0,'yyyy')}",
                distance: 0,
                title: '本年',
                format: '',
            },
            {
                key: "{now('Y',-1,'yyyy')}",
                distance: -1,
                title: '去年',
                format: '',
            },
        ],
        M: [
            {
                key: "{now('M',0,'yyyy-MM')}",
                distance: 0,
                title: '本月',
                format: '',
            },
            {
                key: "{now('M',-1,'yyyy-MM')}",
                distance: -1,
                title: '上月',
                format: '',
            },
            {
                key: "{now('M',-3,'yyyy-MM')}",
                distance: -3,
                title: '最近第3月',
                format: '',
            },
            {
                key: "{now('M',-6,'yyyy-MM')}",
                distance: -6,
                title: '最近第6月',
                format: '',
            },
            {
                key: "{now('M',-12,'yyyy-MM')}",
                distance: -12,
                title: '最近第12月',
                format: '',
            },
        ],
        D: [
            {
                key: "{now('D',-1,'yyyy-MM-dd')}",
                distance: -1,
                title: '最近第1天',
                format: '',
            },
            {
                key: "{now('D',-7,'yyyy-MM-dd')}",
                distance: -7,
                title: '最近第7天',
                format: '',
            },
            {
                key: "{now('D',-15,'yyyy-MM-dd')}",
                distance: -15,
                title: '最近第15天',
                format: '',
            },
            {
                key: "{now('D',-30,'yyyy-MM-dd')}",
                distance: -30,
                title: '最近第30天',
                format: '',
            },
            {
                key: "{now('D',-60,'yyyy-MM-dd')}",
                distance: -60,
                title: '最近第60天',
                format: '',
            },
            {
                key: "{now('D',-90,'yyyy-MM-dd')}",
                distance: -90,
                title: '最近第90天',
                format: '',
            },
            {
                key: "{now('D',-180,'yyyy-MM-dd')}",
                distance: -180,
                title: '最近180天',
                format: '',
            },
        ],
        Q: [
            {
                key: "{now('Q',0,'yyyy-Q')}",
                distance: 0,
                title: '本季度',
                format: '',
            },
            {
                key: "{now('Q',-1,'yyyy-Q')}",
                distance: -1,
                title: '上季度',
                format: '',
            }
        ],
        W: [
            {
                key: "{now('W',0,'yyyy-W')}",
                distance: 0,
                title: '本周',
                format: '',
            },
            {
                key: "{now('W',-1,'yyyy-W')}",
                distance: -1,
                title: '上周',
                format: '',
            },
        ]
    };
    var getTimeByTimeZone = function(timeZone) {
        var d = new Date();
        localTime = d.getTime(),
            localOffset=d.getTimezoneOffset()*60000, //获得当地时间偏移的毫秒数,这里可能是负数
            utc = localTime + localOffset, //utc即GMT时间
            offset = timeZone, //时区，北京市+8  美国华盛顿为 -5
            localSecondTime = utc + (3600000*offset);  //本地对应的毫秒数
        return new Date(localSecondTime);
    };
    // 获取若干天前（后）的日
    var getDateStr = function (AddDayCount) {
        var dd = getTimeByTimeZone(-8);
        dd.setDate(dd.getDate() + AddDayCount);
        var y = dd.getFullYear();
        var m = dd.getMonth() + 1;
        var d = dd.getDate();
        return y+"-"+m+"-"+d;
    };
    // 获取若干月前（后）的月
    var getMonthStr = function (AddMonthCount) {
        var dd = getTimeByTimeZone(-8);
        dd.setMonth(dd.getMonth() + AddMonthCount);
        var y = dd.getFullYear();
        var m = dd.getMonth()+1;
        return y+"-"+m
    };
    // 获取若干年前（后）的年
    var getYearStr = function (AddYearCount) {
        var dd = getTimeByTimeZone(-8);
        dd.setYear(dd.getFullYear() + AddYearCount);//获取AddDayCount天后的日期
        var y = dd.getFullYear();
        return y
    };
    // 获取若干周前（后）的周
    var getWeekStr = function (AddWeekCount) {
        var today = getTimeByTimeZone(-8);
        var firstDay = new Date(today.getFullYear(),0, 1);
        var dayOfWeek = firstDay.getDay();
        var spendDay= 1;
        if (dayOfWeek !=0) {
            spendDay=7-dayOfWeek+1;
        }
        firstDay = new Date(today.getFullYear(),0, 1+spendDay);
        var d =Math.ceil((today.valueOf()- firstDay.valueOf())/ 86400000);
        var result =Math.ceil(d/7);
        var w = (result + 1) + AddWeekCount;
        if(w <= 0){
            return today.getFullYear()-1 + '-' + (52+w);
        }else {
            return today.getFullYear() + '-' + w;
        }
    };
    // 获取若干季前（后）的季
    var getQuarterlyStr = function (AddQuarterlyCount) {
        var dd = getTimeByTimeZone(-8);
        var y = dd.getFullYear();
        var m = dd.getMonth() + 1;
        var q;
        if(m <= 3){
            q = 1;
        }else if(m <= 6){
            q = 2;
        }else if(m <= 9){
            q = 3;
        }else if(m <= 12){
            q = 4;
        };
        if((q + AddQuarterlyCount) <= 0){
            q = 4;
            y = y - 1;
        }
        return y + '-' + q;
    };

    _.each(dropList.D,function(v,i){
        v.format = getDateStr(v.distance);
    });
    _.each(dropList.M,function(v,i){
        v.format = getMonthStr(v.distance);
    });
    _.each(dropList.Y,function(v,i){
        v.format = getYearStr(v.distance);
    });
    _.each(dropList.Q,function(v,i){
        v.format = getQuarterlyStr(v.distance);
    });
    _.each(dropList.W,function(v,i){
        v.format = getWeekStr(v.distance);
    });

    var updateParamTitle = function () {
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.params, function (param) {
                if ('slider' == param.paramType) {
                    return;
                }
                var paramObj;
                switch (param.type) {
                    case '=':
                    case '≠':
                        var arr = angular.copy(param.values);
                        _.forEach(arr,function(v,i){
                            for(var j in dropList){
                                for(var k in dropList[j]){
                                    if(dropList[j][k].key == v){
                                        arr[i] = dropList[j][k].format
                                    }
                                }
                            };
                        });
                        paramObj = param.name + ' ' + param.type + ' (' + arr + ')';
                        break;
                    // case '>':
                    // case '<':
                    case '≥':
                    case '≤':
                        var arr = angular.copy(param.values);
                        _.forEach(arr,function(v,i){
                            for(var j in dropList){
                                for(var k in dropList[j]){
                                    if(dropList[j][k].key == v){
                                        arr[i] = dropList[j][k].format
                                    }
                                }
                            };
                        });
                        paramObj = param.name + ' ' + param.type + ' ' + arr;
                        break;
                    // case '(a,b]':
                    // case '[a,b)':
                    // case '(a,b)':
                    case '[a,b]':
                        var leftBrackets = param.type.split('a')[0];
                        var rightBrackets = param.type.split('b')[1];
                        paramObj = param.name + ' between ' + leftBrackets + param.values[0] + ',' + param.values[1] + rightBrackets;
                        break;
                }
                param.title = param.values.length > 0 ? paramObj : undefined;
            });
        });
    };

});