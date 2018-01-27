/**
 * Created by yfyuan on 2017/5/2.
 */
cBoard.controller('paramSelector', function ($timeout, $scope, $uibModalInstance, dataService, param, filter, getSelects, ok) {
    $scope.type = [
        {
            title: '等于',
            value: '='
        },
        {
            title: '不等于',
            value: '≠'
        },
        {
            title: '大于',
            value: '>'
        },
        {
            title: '小于',
            value: '<'
        },
        {
            title: '大于等于',
            value: '≥'
        },
        {
            title: '小于等于',
            value: '≤'
        },
        {
            title: '大于下限小于等于上限',
            value: '(a,b]'
        },
        {
            title: '大于等于下限小于上限',
            value: '[a,b)'
        },
        {
            title: '大于下限小于上限',
            value: '(a,b)'
        },
        {
            title: '大于等于下限小于等于上限',
            value: '[a,b]'
        },
    ];
    $scope.param = param;
    $scope.operate = {};
    $scope.filter = filter;
    $scope.byFilter = {a: false};
    $scope.loadSelect = true;
    $scope.showRange = false;
    var filterFlag = null;
    $scope.rangeItem = {
        selected: '',
        capped: '',
        lowerLimit: ''
    };
    // 初始进来需要设置默认值 TODO （PS：属性类型一行需要隐藏）
    $scope.selectedAttrKey = null;

    $scope.getSelects = function () {
        $scope.loading = true;
        getSelects($scope.byFilter.a, $scope.param.col, function (d) {
            $scope.selects = d;
            $scope.loading = false;
        });
    };
    var showValues = function () {
        var equal = ['=', '≠'];
        var openInterval = ['>', '<', '≥', '≤'];
        var closeInterval = ['(a,b]', '[a,b)', '(a,b)', '[a,b]'];
        $scope.operate.equal = $.inArray($scope.param.type, equal) > -1 ? true : false;
        $scope.operate.openInterval = $.inArray($scope.param.type, openInterval) > -1 ? true : false;
        $scope.operate.closeInterval = $.inArray($scope.param.type, closeInterval) > -1 ? true : false;
    };
    showValues();
    $scope.dbclickPush = function (o) {
        // = ≠
        if ($scope.operate.equal) {
            if($scope.param.values.length == 1 && (_.isUndefined($scope.param.values[0]) || $scope.param.values[0]=='')){
                $scope.param.values.length = 0;
            }
            $scope.param.values.push(o);
        }
        // > < ≥ ≤
        if ($scope.operate.openInterval) {
            $scope.param.values[0] = o;
        }
        // (a,b]...
        if ($scope.operate.closeInterval) {
            if ($scope.param.values[0] == undefined || $scope.param.values[0] == '') {
                $scope.param.values[0] = o;
            } else {
                $scope.param.values[1] = o;
            }
        }
    };
    $scope.deleteValues = function (array) {
        if ($scope.operate.equal) {
            $scope.param.values = _.difference($scope.param.values, array);
        }
    };
    $scope.pushValues = function (array) {
        if($scope.param.values.length == 1 && (_.isUndefined($scope.param.values[0]) || $scope.param.values[0]=='')){
            $scope.param.values.length = 0;
        }
        if ($scope.operate.openInterval) {
            array.splice(1, array.length - 1);
        }
        if ($scope.operate.closeInterval) {
            array.splice(2, array.length - 2);
        }
        _.each(array, function (e) {
            $scope.param.values.push(e);
        });
    };
    $scope.selected = function (v) {
        return _.indexOf($scope.param.values, v) == -1;
    };
    $scope.filterType = function () {
        var rang = ['(a,b]', '[a,b)', '(a,b)', '[a,b]'];
        if($.inArray(param.type,rang) != -1){
            // 显示上下限选择框
            $scope.showRange = true;
        }else {
            $scope.showRange = false;
        }

        $scope.rangeItem.capped = '';
        $scope.rangeItem.lowerLimit = '';

        $scope.param.values = [];
        $scope.param.values.length = 0;
        showValues();
    };
    $scope.close = function () {
        $uibModalInstance.close();
    };
    $scope.ok = function () {
        $uibModalInstance.close();
        $scope.param.values = _.filter($scope.param.values, function(e){
                return e != null && !_.isUndefined(e);
            }
        );
        ok($scope.param);
    };

    $scope.initValues = function () {
        if($scope.param.values.length==0){
            $scope.param.values.length = 1;
        }
    };
    // 选定日期类型
    $scope.addToSelected = function () {
        console.log(param.values);
    }
    // 选定时间范围
    $scope.selectRange = function (type) {
        if(type =='equal'){
            if($.inArray($scope.rangeItem.selected,param.values) > -1){
                $scope.exist_o = true;
                $timeout(function(){
                    $scope.exist_o = false;
                },1500);
                return;
            }
            param.values.push($scope.rangeItem.selected);
        }else if(type == 'openInterval'){
            param.values[0] = $scope.rangeItem.selected;
        }else if(type == 'closeInterval'){
            if(param.values[0] && (param.values[0] == $scope.rangeItem.capped)){
                $scope.exist_t = true;
                $timeout(function() {
                    $scope.exist_t = false;
                }, 1500);
            }
            if(param.values[1] && (param.values[1] == $scope.rangeItem.lowerLimit)){
                $scope.exist_e = true;
                $timeout(function() {
                    $scope.exist_e = false;
                }, 1500);
            }

            param.values = [];
            param.values.push($scope.rangeItem.capped);
            param.values.push($scope.rangeItem.lowerLimit);
        }else if(type == 'qorw'){
            if(param.type == '=' || param.type == '≠'){
                param.values.push($scope.rangeItem.selected);
            }else if(param.type == '>' || param.type == '<' || param.type == '≥' || param.type == '≤'){
                param.values = [];
                param.values[0] = $scope.rangeItem.selected;
            }else {
                param.values[0] = $scope.rangeItem.capped;
                param.values[1] = $scope.rangeItem.lowerLimit;
            }
        }
    }
    //
    $scope.clearValue = function (flag) {
        if(flag == 1){
            $scope.type = [
                {
                    title: '等于',
                    value: '='
                },
                {
                    title: '不等于',
                    value: '≠'
                },
                {
                    title: '大于',
                    value: '>'
                },
                {
                    title: '小于',
                    value: '<'
                },
                {
                    title: '大于等于',
                    value: '≥'
                },
                {
                    title: '小于等于',
                    value: '≤'
                },
                {
                    title: '大于下限小于等于上限',
                    value: '(a,b]'
                },
                {
                    title: '大于等于下限小于上限',
                    value: '[a,b)'
                },
                {
                    title: '大于下限小于上限',
                    value: '(a,b)'
                },
                {
                    title: '大于等于下限小于等于上限',
                    value: '[a,b]'
                },
            ];
        }else {
            $scope.type = [
                {
                    title: '等于',
                    value: '='
                },
                {
                    title: '不等于',
                    value: '≠'
                }
            ];
        }
        if(flag != filterFlag){
            param.values = [];
        }
        filterFlag = flag;
    }
    // 删除当前选定值
    $scope.clearSelected = function (index) {
        if($scope.showRange){
            param.values = [];
        }else {
            param.values.splice(index, 1);
        }
    }
});