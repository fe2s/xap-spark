var wordCountCloud;

var wordCountCloudHasFocus = false;

function initWordCountCloud() {
    $("#wordcount_cloud_div").mouseenter(function() {
        wordCountCloudHasFocus = true;
    });
    $("#wordcount_cloud_div").mouseleave(function() {
        wordCountCloudHasFocus = false;
    });
}

function drawWordCountCloud(data) {
//    if(wordCountCloudHasFocus){
//        return;
//    }

    var coef = findCoef(data)

    var list = new Array()
    for(var i in data){
        var row = data[i];
        list.push([ row.word, row.count/coef ])
    }
    WordCloud(document.getElementById('wordcount_cloud_div'), { list: list} );
}

function findCoef(data){
    var max = 0;
    for(var i in data){
        var value = data[i].count;
        if(value > max){
            max = value;
        }
    }
    return max/100;
}