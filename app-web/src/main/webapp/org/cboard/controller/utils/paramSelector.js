/**
 * Created by yfyuan on 2017/5/2.
 */
cBoard.controller('paramSelector', function ($timeout, $scope, $uibModalInstance, dataService, param, filter, getSelects, ok,disabled) {
    // 初始化部分数据
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
    $scope.param = angular.copy(param);
    $scope.param.cloneValue = angular.copy(param.values);

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


    if($scope.selectedAttrKey !== 'other'){
        $scope.currentDropList = $scope.dropList[$scope.selectedAttrKey];
        _.forEach($scope.param.cloneValue,function(v,i){
            _.forEach($scope.currentDropList,function(val){
                if(v == val.key){
                    $scope.param.cloneValue[i] = val;
                }
            });
        })
    }
    $scope.findTypeOf = function(param){
        return typeof(param)
    };
    $scope.onClickDropList = function(key){
        // 判断类型
        var obj = $scope.dropList[$scope.selectedAttrKey];
        for(var i in obj){
            if(obj[i].key == key){
                $scope.param.cloneValue.push(obj[i]);
            }
        };
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
            if($scope.param.cloneValue.length == 1 && (_.isUndefined($scope.param.cloneValue[0]) || $scope.param.cloneValue[0]=='')){
                $scope.param.cloneValue.length = 0;
            }
            $scope.param.cloneValue.push(o);
        }
        // > < ≥ ≤
        if ($scope.operate.openInterval) {
            $scope.param.cloneValue[0] = o;
        }
        // (a,b]...
        if ($scope.operate.closeInterval) {
            if ($scope.param.cloneValue[0] == undefined || $scope.param.cloneValue[0] == '') {
                $scope.param.cloneValue[0] = o;
            } else {
                $scope.param.cloneValue[1] = o;
            }
        }
    };
    $scope.deleteValues = function (array) {
        if ($scope.operate.equal) {
            $scope.param.cloneValue = _.difference($scope.param.cloneValue, array);
        }
    };
    $scope.pushValues = function (array) {
        if($scope.param.cloneValue.length == 1 && (_.isUndefined($scope.param.cloneValue[0]) || $scope.param.cloneValue[0]=='')){
            $scope.param.cloneValue.length = 0;
        }
        if ($scope.operate.openInterval) {
            array.splice(1, array.length - 1);
        }
        if ($scope.operate.closeInterval) {
            array.splice(2, array.length - 2);
        }
        _.each(array, function (e) {
            $scope.param.cloneValue.push(e);
        });
    };
    $scope.selected = function (v) {
        return _.indexOf($scope.param.cloneValue, v) == -1;
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

        $scope.param.cloneValue = [];
        $scope.param.cloneValue.length = 0;
        showValues();
    };
    $scope.close = function () {
        $uibModalInstance.close();
    };
    $scope.ok = function () {
        $uibModalInstance.close();
        $scope.param.values = [];
        $scope.param.values = _.filter($scope.param.cloneValue, function(e){
                return e != null && !_.isUndefined(e);
            }
        );
        _.forEach($scope.param.values,function(v,i){
            if(typeof v != 'string'){
                $scope.param.values[i] = $scope.param.values[i].key;
            }
        });
        ok($scope.param);
    };

    $scope.initValues = function () {
        if($scope.param.cloneValue.length==0){
            $scope.param.cloneValue.length = 1;
        }
    };
    // 选定时间范围
    $scope.selectRange = function (type) {
        // = ≠
        if(type =='equal'){
            if($.inArray($scope.rangeItem.selected,$scope.param.cloneValue) > -1){
                $scope.exist_o = true;
                $timeout(function(){
                    $scope.exist_o = false;
                },1500);
                return;
            }
            $scope.param.cloneValue.push($scope.rangeItem.selected);
            // ≤ ≥
        }else if(type == 'openInterval'){
            $scope.param.cloneValue[0] = $scope.rangeItem.selected;
            // 范围
        }else if(type == 'closeInterval'){
            if($scope.param.cloneValue[0] && ($scope.param.cloneValue[0] == $scope.rangeItem.capped)){
                $scope.exist_t = true;
                $timeout(function() {
                    $scope.exist_t = false;
                }, 1500);
            }
            if($scope.param.cloneValue[1] && ($scope.param.cloneValue[1] == $scope.rangeItem.lowerLimit)){
                $scope.exist_e = true;
                $timeout(function() {
                    $scope.exist_e = false;
                }, 1500);
            }

            $scope.param.cloneValue = [];
            $scope.param.cloneValue.push($scope.rangeItem.capped);
            $scope.param.cloneValue.push($scope.rangeItem.lowerLimit);
            // 周/季度
        }else if(type == 'qorw'){
            if(param.type == '=' || param.type == '≠'){
                $scope.param.cloneValue.push($scope.rangeItem.selected);
            }else if(param.type == '≥' || param.type == '≤'){
                $scope.param.cloneValue = [];
                $scope.param.cloneValue[0] = $scope.rangeItem.selected;
            }else {
                $scope.param.cloneValue[0] = $scope.rangeItem.capped;
                $scope.param.cloneValue[1] = $scope.rangeItem.lowerLimit;
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
        $scope.param.cloneValue = [];
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
            $scope.param.cloneValue = [];
        }
        filterFlag = flag;
    };
    // 删除当前选定值
    $scope.clearSelected = function (index) {
        if($scope.showRange){
            $scope.param.cloneValue = [];
        }else {
            $scope.param.cloneValue.splice(index, 1);
        }
    };
});