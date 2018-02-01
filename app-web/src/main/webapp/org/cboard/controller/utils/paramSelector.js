/**
 * Created by yfyuan on 2017/5/2.
 */
cBoard.controller('paramSelector', function ($timeout, $scope, $uibModalInstance, dataService, param, filter, getSelects, ok,disabled) {
    // 初始化部分数据
    //
    console.log(param);
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
            title: '大于等于',
            value: '≥'
        },
        {
            title: '小于等于',
            value: '≤'
        },
        {
            title: '范围[上限,下限]',
            value: '[a,b]'
        },
    ];
    $scope.dropList = {
        Y: [
            {
                key: "{now('Y',0,'yyyy')}",
                title: '本年',
            },
            {
                key: "{now('Y',-1,'yyyy')}",
                title: '去年',
            },
        ],
        M: [
            {
                key: "{now('M',0,'yyyy-MM')}",
                title: '本月',
            },
            {
                key: "{now('M',-1,'yyyy-MM')}",
                title: '上月',
            },
            {
                key: "{now('M',-3,'yyyy-MM')}",
                title: '最近第3月',
            },
            {
                key: "{now('M',-6,'yyyy-MM')}",
                title: '最近第6月',
            },
            {
                key: "{now('M',-12,'yyyy-MM')}",
                title: '最近第12月',
            },
        ],
        D: [
            {
                key: "{now('D',-7,'yyyy-MM-dd')}",
                title: '最近第7天',
            },
            {
                key: "{now('D',-15,'yyyy-MM-dd')}",
                title: '最近第15天',
            },
            {
                key: "{now('D',-30,'yyyy-MM-dd')}",
                title: '最近第30天',
            },
            {
                key: "{now('D',-60,'yyyy-MM-dd')}",
                title: '最近第60天',
            },
            {
                key: "{now('D',-90,'yyyy-MM-dd')}",
                title: '最近第90天',
            },
            {
                key: "{now('D',-180,'yyyy-MM-dd')}",
                title: '最近半年',
            },
        ],
        Q: [
            {
                key: "{now('Q',0,'yyyy-Q')}",
                title: '本季度',
            },
            {
                key: "{now('Q',-1,'yyyy-Q')}",
                title: '上季度'
            }
        ],
        W: [
            {
                key: "{now('W',0,'yyyy-W')}",
                title: '本周',
            },
            {
                key: "{now('W',-1,'yyyy-W')}",
                title: '上周'
            },
        ]
    };

    $scope.disabled = disabled;
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
    // 初始进来需要设置默认值
    if(param.hasOwnProperty('fileType')){
        $scope.selectedAttrKey = param.fileType;
    }else if(param.col && param.col.length == 1){
        $scope.selectedAttrKey = param.col[0].fileType;
    }else {
        $scope.selectedAttrKey = 'other'
    }


    if(param.fileType !== 'other'){
        $scope.currentDropList = $scope.dropList[param.fileType];
    }

    $scope.onClickDropList = function(key){
        console.log(key);
        // 判断类型

    };

    $scope.getSelects = function () {
        $scope.loading = true;
        getSelects($scope.byFilter.a, $scope.param.col, function (d) {
            $scope.selects = d;
            $scope.loading = false;
        });
    };
    var showValues = function () {
        var equal = ['=', '≠'];
        var openInterval = ['≥', '≤'];
        var closeInterval = ['[a,b]'];
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
        var rang = ['[a,b]'];
        if($.inArray(param.type,rang) != -1){
            // 显示上下限选择框
            $scope.showRange = true;
        }else {
            $scope.showRange = false;
        }

        $scope.rangeItem.selected = '';
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
        // = ≠
        if(type =='equal'){
            if($.inArray($scope.rangeItem.selected,param.values) > -1){
                $scope.exist_o = true;
                $timeout(function(){
                    $scope.exist_o = false;
                },1500);
                return;
            }
            param.values.push($scope.rangeItem.selected);
            // ≤ ≥
        }else if(type == 'openInterval'){
            param.values[0] = $scope.rangeItem.selected;
            // 范围
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
            // 周/季度
        }else if(type == 'qorw'){
            if(param.type == '=' || param.type == '≠'){
                param.values.push($scope.rangeItem.selected);
            }else if(param.type == '≥' || param.type == '≤'){
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
        $scope.rangeItem = {
            selected: '',
            capped: '',
            lowerLimit: ''
        };
        param.type = '=';
        $scope.operate.equal = true;
        $scope.operate.openInterval = false;
        $scope.operate.closeInterval = false;
        $scope.param.values = [];
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
                    title: '大于等于',
                    value: '≥'
                },
                {
                    title: '小于等于',
                    value: '≤'
                },
                {
                    title: '范围[上限,下限]',
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
    };
    // 删除当前选定值
    $scope.clearSelected = function (index) {
        if($scope.showRange){
            param.values = [];
        }else {
            param.values.splice(index, 1);
        }
    };
});