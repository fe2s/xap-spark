var wordCountTable;

var wordCountOption = {showRowNumber: true, is3D: true, title: 'Current word count'};

var wordCountHasFocus = false;

function initWordCountTable() {

    var block = document.getElementById('wordcount_table_div');
    wordCountOption.width = block.clientWidth;
    wordCountTable = new google.visualization.Table(block);

    $("#wordcount_table_div").mouseenter(function() {
        wordCountHasFocus = true;
    });
    $("#wordcount_table_div").mouseleave(function() {
        wordCountHasFocus = false;
    });
}

function drawWordCountTable(data) {
    if(wordCountHasFocus){
        return;
    }
    var tableData = new google.visualization.DataTable();
    tableData.addColumn('string', 'Word');
    tableData.addColumn('number', 'Count');

    for(var i in data){
        var row = data[i];
        tableData.addRow([row.word,row.count]);
    }

    tableData.sort([{column: 1, desc:true}, {column: 0}])
    wordCountTable.draw(tableData, wordCountOption);
}