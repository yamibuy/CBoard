var CBoardTableRender = function (jqContainer, options, drill) {
    this.container = jqContainer; // jquery object
    this.options = options;
    this.tall;
    this.drill = drill;
    var _this = this;
    $(this.container).resize(function (e) {
        _this.resize(e.target);
    });
};

CBoardTableRender.prototype.resize = function (container) {
    var wrapper = $(container).find('.table_wrapper');
    wrapper.css('width', 'auto');
    if (wrapper.width() < $(container).width()) {
        wrapper.css('width', '100%');
    }
};

CBoardTableRender.prototype.do = function (tall, persist) {
    this.tall = tall;
    tall = _.isUndefined(tall) ? 500 : tall;
    var divHeight = tall - 90;
    var _this = this;
    var render = function (o, drillConfig) {
        _this.options = o;
        if (_this.drill) {
            _this.drill.config = drillConfig;
        }
        _this.do(_this.tall);
    };
    _.forEach(this.options.data,function(val){
        if(Array.isArray(val)){
            _.forEach(val,function(value){
                if(value.property === 'data' && isNaN(parseFloat(value.data))){
                    value.data = 'N/A';
                }
            })
        }
    });

    var args = {
        tall: divHeight,
        chartConfig: this.options.chartConfig,
        data: this.options.data,
        container: this.container,
        drill: this.drill,
        render: render
    };
    console.log(args);
    new CBCrossTable(args).table();
    $(this.container).css({
        height: tall + "px"
    });
    this.resize(this.container);
    if (persist) {
        persist.data = this.options.data;
        persist.type = "table"
    }
    return render;
};

