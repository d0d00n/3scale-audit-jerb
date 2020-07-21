const mysql = require('mysql');
const fs = require('fs');
const zlib = require('zlib');
const aws = require('aws-sdk');


const mysqlConnectionString = process.env.MYSQL_CONNECTION_STRING;
const awsAccessKeyId = process.env.AWS_ACCESS_KEY_ID;
const awsSecretAccessKey = process.env.AWS_SECRET_ACCESS_KEY;
const awsBucket = process.env.AWS_BUCKET;
const fileNamePrefix = process.env.FILE_NAME_PREFIX || '3scale_audit_log_';
const s3AuditFolder = process.env.FOLDER_NAME || 'audit';

if (mysqlConnectionString == null || awsAccessKeyId == null || awsSecretAccessKey == null || awsBucket == null){
    console.log('Error in cron jerb. Make sure MYSQL_CONNECTION_STRING, AWS_ACCESS_KEY_ID, AWS_SECRET_ACCESS_KEY and AWS_BUCKET are set.');
}

//swap protocol from mysql2 (Ruby library) to mysql
var connection = mysql.createConnection(mysqlConnectionString.replace('mysql2://','mysql://'));

try{
    connection.connect();

    connection.query('SELECT * FROM audits WHERE YEAR(created_at) = YEAR(CURRENT_DATE - INTERVAL 1 MONTH) AND MONTH(created_at) = MONTH(CURRENT_DATE - INTERVAL 1 MONTH);',
        function (error, results, fields) {
        if (error) throw error;
        var d = new Date();
        var dateForFile = d.getFullYear();
        if (d.getMonth() === 0){
            dateForFile += '-12';
        }
        else{
            dateForFile += '-' + d.getMonth(); // 0-based month digit works well since we don't actually have to subtract anything...
        }
        const buffer = new Buffer(JSON.stringify(results), 'utf-8');
        zlib.gzip(buffer, function (error,result){
            // take the file and shove it into AWS
            var objectParams = {Bucket : awsBucket, Key: s3AuditFolder+'/'+fileNamePrefix+dateForFile+'.gz', Body: result };
            var uploadPromise = new aws.S3({apiVersion: '2006-03-01'}).putObject(objectParams).promise();
            uploadPromise.then(function(data){
                console.log('Successfully uploaded '+fileNamePrefix+dateForFile+'.gz'+' to '+awsBucket+'/'+s3AuditFolder);
            })

        });
      });
    connection.end();
} catch (err){
    console.log('Error in jerb: '+ err.stack);
}


