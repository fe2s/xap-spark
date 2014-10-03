google.load('visualization', '1.0', {'packages':['corechart','table','geochart']});

google.setOnLoadCallback(drawCharts);

function drawCharts(){

    initWordCountTable();
    initWordCountCloud();

    drawChartsAJAX()

    setInterval(drawChartsAJAX, 1000)
}

function drawChartsAJAX(){
    $.get( "wordCountChartData", function( data ) {
      successCharts(data)
    });
}

function successCharts(data) {
    drawWordCountTable(data);
    drawWordCountCloud(data)
}