#!/usr/bin/env node
var INFLUX_DB = process.env.INFLUX_DB ? process.env.INFLUX_DB : 'http://localhost:8086/hyperflow_tests';
var HfId = process.env.HfId ? process.env.HfId : "";
var WfId = process.env.WfId ? process.env.WfId : "";

const Influx = require('influxdb-nodejs');
var fs = require('fs');
const toCSV = require('array-to-csv')
var mkdirp = require('mkdirp');

var client = new Influx(INFLUX_DB);

function writeDataToFile(columns,values,file_name)
{
    var stream = fs.createWriteStream(file_name+".csv");
    var result =toCSV([columns].concat(values));

    //console.log([columns].concat(values));
    stream.once('open', function(fd) {
    stream.write(result);
    stream.end();
    });
}


function getStartEndOfExperiment(hfId,wfId,cb)
{
    client.query('execution_log_sub_stage')
    .where('hfId',HfId)
    .then((data )=> {
        console.log("data returned");

        var first = data.results[0].series[0].values[0];
        var last  = data.results[0].series[0].values[data.results[0].series[0].values.length-1];

        console.log(first);
        console.log(last);

        var startTime=first[0];
        var endTime=last[0];

        var commonData={start:startTime,end:endTime,hfId:hfId,wfId:wfId};

        cb(commonData );
    })
    .catch(console.error);
}

function getDataHfIdWfIdBased(commonData,metric,file_name)
{
    client.query(metric)
    .where('hfId',HfId)
    .then((data )=> {
        writeDataToFile(data.results[0].series[0].columns,data.results[0].series[0].values,file_name);
    })
    .catch(console.error);
}


function getDataTimeBased(commonData,metric,file_name)
{

    client.query(metric)
    .where('time',commonData.start, '>=')
    .where('time',commonData.end , '<=')
    .then((data )=> {
        writeDataToFile(data.results[0].series[0].columns,data.results[0].series[0].values,file_name)
    })
    .catch(console.error);
}

function main()
{
    console.log("main start");

    getStartEndOfExperiment(HfId,WfId,function(commonData){
        mkdirp(HfId, function(err) { 
            getDataTimeBased(commonData,'hyperflow_cpu_usage_ec2',HfId+"/"+"cpuUsage")
            
            getDataTimeBased(commonData,'hyperflow_connection_transferred',HfId+"/"+"connectionEth0Send");

            getDataTimeBased(commonData,'hyperflow_connection_received',HfId+"/"+"connectionEth0Receive");

            getDataTimeBased(commonData,'hyperflow_memory_usage_ec2',HfId+"/"+"memoryUsageEC2");

            getDataHfIdWfIdBased(commonData,'execution_log_sub_stage',HfId+"/"+"executionStages");

            getDataHfIdWfIdBased(commonData,'execution_times',HfId+"/"+"executionWorkerTime");

            getDataTimeBased(commonData,'hyperflow_rabbitmq_monitor',HfId+"/"+"rabbitmq_queue");

            getDataTimeBased(commonData,'hyperflow_monitor',HfId+"/"+"ec2AndContainerInstances");

            getDataTimeBased(commonData,'hyperflow_alarms',HfId+"/"+"alarmsState");
        });
    });
}

main();
