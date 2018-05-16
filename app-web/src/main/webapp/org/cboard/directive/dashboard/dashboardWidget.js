/**
 * Created by yfyuan on 2016/8/8.
 */

cBoard.directive('dashboardWidget', function ($compile, $templateCache, dataService, chartService) {

    var renderEchart = function (scope, element, attrs) {
        var template = $templateCache.get("echartContent");
        scope.myheight = scope.row.height ? (scope.row.height - 44) : 400;
        var link = $compile(template);
        element.append(link(scope));
        var ndWrapper = $(element).find('.box-body');
        scope.widget.render(ndWrapper, null, scope);
    };

    var renderMap = function (scope, element, attrs) {
        var template = $templateCache.get("chartContent");
        scope.myheight = scope.row.height ? (scope.row.height - 44 ) : 400;
        var link = $compile(template);
        element.append(link(scope));
        var ndWrapper = $(element).find('.box-body');
        scope.widget.render(ndWrapper, null, scope);
    };

    // var renderUSAMap = function (scope, element, attrs) {
    //     var template = $templateCache.get("chartContent");
    //     scope.myheight = scope.row.height ? (scope.row.height - 44 ) : 400;
    //     var link = $compile(template);
    //     element.append(link(scope));
    //     var ndWrapper = $(element).find('.box-body');
    //     scope.widget.render(ndWrapper, null, scope);
    // };

    var renderKpi = function (scope, element, attrs) {
        var template = $templateCache.get("kpiContent");
        var aa = $compile(template)(scope);
        element.append(aa);
        var ndWrapper = $(element).find('.kpi-body');
        scope.widget.render(ndWrapper, null, scope);
    };

    var renderTable = function (scope, element, attrs) {
        var template = $templateCache.get("chartContent");
        scope.myheight = scope.row.height ? (scope.row.height - 44 ) : 400;
        var aa = $compile(template)(scope);
        element.append(aa);
        var ndWrapper = $(element).find('.box-body');
        scope.widget.render(ndWrapper, null, scope);
    };

    return {
        restrict: 'E',
        scope: true,
        compile: function (element, attrs) {
            return {
                pre: function (scope, element, attrs) {
                },
                post: function (scope, element, attrs) {
                    switch (scope.widget.widget.data.config.chart_type) {
                        case 'map':
                            renderMap(scope, element, attrs);
                            break;
                        case 'kpi':
                            renderKpi(scope, element, attrs);
                            break;
                        case 'table':
                            renderTable(scope, element, attrs);
                            break;
                        // case 'usaMap':
                        //     renderUSAMap(scope, element, attrs);
                        //     break;
                        default:
                            renderEchart(scope, element, attrs);
                    }
                }
            }
        }
    };
});

cBoard.directive('ngdatepicker', function ($timeout,$filter,$rootScope){
    return {
        require: '?ngModel',
        restrict: 'A',
        scope: {
            ngModel: '=',
            minDate: '='
        },
        link: function(scope, element, attr, ngModel) {
            // 当数据在AngularJS内部改变, 把model的值更新到view上
            ngModel.$render = function() {
                $timeout(function(){
                    element.val(ngModel.$viewValue ? ngModel.$viewValue : '');
                });
            };
            // 当数据在AngularJS外部改变
            element.on('blur', function() {
                // 通知AngularJS更新UI, 并把值转成时间戳
                $timeout(function(){
                    var pickedTime = element.val();
                    ngModel.$setViewValue(pickedTime ? pickedTime : '');
                });
            });

            var vm = {
                events: {
                    init: function(){
                        vm.events.dateTimePickerInit('zh-cn');
                    },
                    dateTimePickerInit: function(language){
                        $timeout(function () {
                            var headerFormat = 'yyyy-mm-dd'
                            switch (attr.minview) {
                                case '2':
                                    headerFormat = 'yyyy';
                                    break;
                                case '1':
                                    headerFormat = 'yyyy-mm';
                                    break;
                                case '0':
                                    headerFormat = 'yyyy-mm-dd';
                                    break;
                            }
                            element.datepicker({
                                format: headerFormat,
                                language: 'zh-CN',
                                endDate : new Date(),
                                minViewMode: attr.minview - 0
                            });
                        })
                    }
                }
            };

            vm.events.init();
        }
    }
});