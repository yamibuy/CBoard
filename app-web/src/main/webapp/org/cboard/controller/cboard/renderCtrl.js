cBoard.controller('renderCtrl', function ($timeout, $rootScope, $scope, $state, $location, $http, ModalUtils, chartService) {

    $scope.loading = true;
    $scope.l = 1;
    $scope.persistFinish = false;

    var buildRender = function (w, reload) {
        w.render = function (content, optionFilter, scope) {
            w.persist = {};
            var chartType = w.widget.data.config.chart_type;
            try {
                if (chartType == 'chinaMapBmap') {
                    chartService.renderChart(content, w.widget.data, {
                        optionFilter: optionFilter,
                        scope: scope,
                        reload: reload,
                        persist: w.persist
                    });
                    w.loading = false;
                    $scope.l--;
                } else {
                    chartService.renderChart(content, w.widget.data, {
                        optionFilter: optionFilter,
                        scope: scope,
                        reload: reload,
                        persist: w.persist
                    }).then(function (d) {
                        w.realTimeTicket = d;
                        w.loading = false;
                        $scope.l--;
                    }, function (error) {
                        $scope.l--;
                    });
                }
            } catch (e) {
                console.error(e);
            }
        };
    };

    $scope.$watch('l', function (newValue) {
        if (newValue == 0) {
            $timeout(function () {
                runTask();
            }, 3000);
        }
    });

    var runTask = function () {
        var result = {};
        _.each($scope.board.layout.rows, function (row) {
            _.each(row.widgets, function (widget) {
                result[widget.widgetId] = widget.persist;
            });
        });

        html2canvas($('body')[0], {
            onrendered: function (canvas) {
                result['img'] = canvas.toDataURL("image/jpeg");
                var obj = {
                    persistId: $location.search().pid,
                    data: result
                };
                var xmlhttp = new XMLHttpRequest();
                xmlhttp.open("POST", "commons/persist.do", false);
                xmlhttp.send(angular.toJson(obj));
                $scope.$apply(function () {
                    $scope.persistFinish = true;
                });
            }
        });
    };
    var GetRequest = function () {
        var url = location.search; //获取url中"?"符后的字串
        var theRequest = new Object();
        if (url.indexOf("?") != -1) {
            var str = url.substr(1);
            strs = str.split("&");
            for(var i = 0; i < strs.length; i ++) {
                theRequest[strs[i].split("=")[0]]=unescape(strs[i].split("=")[1]);
            }
        }
        return theRequest;
    }
    $scope.load = function (reload) {
        $scope.loading = true;

        if ($scope.board) {
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.widgets, function (widget) {
                    widget.show = false;
                });
            });
        }
        $http.get("dashboard/getBoardData.do?id=" + $location.search().id).success(function (response) {
            $scope.loading = false;

            // 获取Url的参数
            var params = GetRequest();
            var filterParams = null;
            if(params.hasOwnProperty('filters') && params.filters){
                _.forEach(function(value,index){
                    if(value.type === 'widget'){
                        _.forEach(value.widgets,function(val,idx){
                            _.forEach(val.widget.data.config.filters,function(v,i){
                                v.filters.push(
                                    {
                                        col: 'NNNNN',
                                        type: '=',
                                        values: []
                                    }
                                );
                            })
                        })
                    }
                });
            }
            console.log(response);
            $scope.board = response;
            _.each($scope.board.layout.rows, function (row) {
                _.each(row.widgets, function (widget) {
                    if (!_.isUndefined(widget.hasRole) && !widget.hasRole) {
                        return;
                    }
                    buildRender(widget, reload);
                    widget.loading = true;
                    widget.show = true;
                    $scope.l++;
                });
            });
            $scope.l--;
        });
    };

    $scope.load(false);
});