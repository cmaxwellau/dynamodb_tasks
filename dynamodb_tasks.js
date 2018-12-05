'use strict';

const AWS = require('aws-sdk');
const meow = require('meow');
const JSONStream = require('JSONStream');
const streamToPromise = require('stream-to-promise');
const fs = require('fs');
const bluebird = require('bluebird');
const debug = require('debug')('dynamo_tasks');
const _ = require('lodash');  

bluebird.promisifyAll(fs);

const cli = meow(`
	Usage
    $ node dynamodb_tasks.js <action> <options> 

  Actions:
    list-tables         List all tables available for export
    export-schema       Export table schema
    export-data         Export table data
    export-all-schema   Export all table schemas. Table name will be used for the filename
    export-all-data     Export all table data. Table name will be used for the filename
    import-schema       Import table schema
    import-data         Import table data

	Options:
	  --table             Name of dynamodb table to import/export
	  --file              File name to use to import/export of table data and schemas
    --profile           Optional: Name of specific AWS CLI profile contained in ~/.aws/credentials
    --region            Optional: Over-ride profile region, or environment variable AWS_REGION


	Examples
    node dynamodb_tasks.js list-tables --region=us-east-1
	  node dynamodb_tasks.js export-schema --region=us-east-1 --table=example_table --file=example_file
	  node dynamodb_tasks.js export-data --region=us-east-1  --table=example_table --file=example_file
	  node dynamodb_tasks.js import-schema --region=us-east-1  --table=example_table --file=example_file
	  node dynamodb_tasks.js import-data --region=us-east-1  --table=example_table --file=example_file

	`);

const methods = {
  'list-tables': listTablesCli
 ,'export-schema': exportSchemaCli
 ,'export-data': exportDataCli
 ,'export-all-data': exportAllDataCli
 ,'export-all-schema': exportAllSchemaCli
 ,'import-schema': importSchemaCli
 ,'import-data': importDataCli
};

const cli_tablename = (cli.flags.table !== undefined) ? cli.flags.table : false; 
const cli_filename = (cli.flags.file !== undefined) ? cli.flags.file : false;

if (cli.flags.profile !== undefined)       { AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: cli.flags.profile});}
if (cli.flags.region !== undefined)        { AWS.config.update({region: cli.flags.region});}
if (cli.flags.maxRetries !== undefined)    { AWS.config.maxRetries = cli.flags.maxRetries;}

const dynamodb = new AWS.DynamoDB();

const method = methods[cli.input[0]] || cli.showHelp();

bluebird.resolve(method.call(undefined, cli))
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });

function listTablesCli() {
  return listTables().then(tables => console.log(tables.join(' ')));
}

function listTables() {
  const params = {};
  let tables = [];
  const listTablesPaged = () => {
    return dynamodb.listTables(params).promise()
      .then(data => {
        tables = tables.concat(data.TableNames);
        if (data.LastEvaluatedTableName !== undefined) {
          params.ExclusiveStartTableName = data.LastEvaluatedTableName;
          return listTablesPaged();
        }

        return tables;
      });
  };

  return listTablesPaged();
}

function exportSchemaCli() {
  if (!!cli_tablename) {
    console.error('Error: --table option is required for this action');
    cli.showHelp();
    return false;
  }
  return exportSchema(cli_tablename, cli_filename || null)
}

function exportAllSchemaCli() {
  return bluebird.map(listTables(), tableName => {
    console.log(`Exporting ${tableName}`);
    return exportSchema(tableName, null);
  }, { concurrency: 1 });
}

function exportSchema(targetTableName, outputFileName=null) {
  if(!outputFileName) outputFileName = sanitizeFilename(targetTableName + '.dynamoschema');
  return dynamodb.describeTable({ TableName: targetTableName }).promise()
    .then(data => {
      return fs.writeFileAsync(outputFileName, JSON.stringify(data.Table, null, 2))
    });
}

function importSchemaCli() {
  if (!cli_filename || !cli_tablename) {
    console.error('Error: --file and --table options are required for this action');
    cli.showHelp();
    return false;
  }
  return importSchema(cli_tablename, cli_filename);
}

function importSchema(targetTableName, inputFileName){
  const doWaitForActive = () => promisePoller({
    taskFn: () => {
      return dynamodb.describeTable({ TableName: targetTableName }).promise()
        .then(data => {
          if (data.Table.TableStatus !== 'ACTIVE') throw new Error();
        });
    },
    interval: 1000,
    retries: 60
  });

  fs.readFileAsync(inputFileName)
    .then(data => JSON.parse(data))
    .then(json => {
      if (targetTableName) json.TableName = targetTableName;

      filterTableSchema(json);

      return dynamodb.createTable(json).promise()
        .then(() => {
          if (cli.flags.waitForActive !== undefined) {
            return doWaitForActive();
          }
        });
    });
}

function filterTableSchema(table) {
  delete table.TableStatus;
  delete table.CreationDateTime;
  delete table.ProvisionedThroughput.LastIncreaseDateTime;
  delete table.ProvisionedThroughput.LastDecreaseDateTime;
  delete table.ProvisionedThroughput.NumberOfDecreasesToday;
  delete table.TableSizeBytes;
  delete table.ItemCount;
  delete table.TableArn;
  delete table.LatestStreamLabel;
  delete table.LatestStreamArn;
  delete table.TableId;

  (table.LocalSecondaryIndexes || []).forEach(index => {
    delete index.IndexSizeBytes;
    delete index.ItemCount;
    delete index.IndexArn;
  });

  (table.GlobalSecondaryIndexes || []).forEach(index => {
    delete index.IndexStatus;
    delete index.IndexSizeBytes;
    delete index.ItemCount;
    delete index.IndexArn;
    delete index.ProvisionedThroughput.NumberOfDecreasesToday;
  });
}

function importDataCli() {
  if (!cli_filename || !cli_tablename) {
    console.error('Error: --file and --table options are required for this action');
    cli.showHelp();
    return false;
  }
  return importData(cli_tablename, cli_filename);
} 

function importData(targetTableName, inputFileName) {

  // dynamodb.describeTable(params, function(err, data) {
  //   if (err) console.log(err, err.stack); // an error occurred
  //   else     console.log(data);         

  // dynamodb.updateTable({ TableName: target_tablename, ProvisionedThroughput: { ReadCapacityUnits: 50, WriteCapacityUnits: 50 } }, function(err, data) {
  //   if (err) { console.log(err, err.stack) && return false}
  // } 

  const readStream = fs.createReadStream(inputFileName, { highWaterMark: 2048 });
  const parseStream = JSONStream.parse('*');

  let n = 0;
  let items = [];

  const logProgress = () => console.log('Imported', n, 'items');
  const logThrottled = _.throttle(logProgress, 5000, { trailing: false });
  const batchSize = 25;

  readStream
    .pipe(parseStream)
    .on('data', data => {
      items.push({'PutRequest': {'Item' : data}});   
      n++;
      if(items.length==batchSize){
        parseStream.pause();
        dynamodb.batchWriteItem({RequestItems: {[targetTableName]: items}}).promise()
        .then(() => items=[])
        .then(() => parseStream.resume())
        .then(() => logThrottled())
        .catch(err => parseStream.emit('error', err));
      }
    })
    .on('end', () => {
      if(items.length>0){
        ddbBatchWriteItem({RequestItems: {[targetTableName]: items}}).promise()
        .then(() => items=[])
        .catch(err => parseStream.emit('error', err));
      }
    })
    .on('error', error => {
      console.log(error);
      parseStream.destroy(error);
    });

    return new Promise((resolve, reject) => {
      parseStream.on('end', resolve);
      parseStream.on('error', reject);
    })
    .then(() => console.log('Import complete. Imported ', n, 'items'));
}

function exportDataCli() {
  if (!cli_filename || !cli_tablename) {
    console.error('Error: --file and --table options are required for this action');
    cli.showHelp();
    return false;
  }
  return exportData(cli_tablename, cli_filename);
}

function exportAllDataCli() {
  return bluebird.map(listTables(), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportData(tableName);
  }, { concurrency: 1 });
}

function exportData(targetTableName, outputFileName=null) {
  if(!outputFileName) outputFileName = sanitizeFilename(targetTableName + '.dynamodata');
  const writeStream = fs.createWriteStream(outputFileName);
  const stringify = JSONStream.stringify();
  stringify.pipe(writeStream);

  let n = 0;

  const params = { TableName: targetTableName };
  const scanPage = () => {
    return bluebird.resolve(dynamodb.scan(params).promise())
      .then(data => {
        data.Items.forEach(item => stringify.write(item));

        n += data.Items.length;
        console.log('Exported', n, 'items');

        if (data.LastEvaluatedKey !== undefined) {
          params.ExclusiveStartKey = data.LastEvaluatedKey;
          return scanPage();
        }
      });
  }

  return scanPage()
    .finally(() => {
      stringify.end();
      return streamToPromise(stringify);
    })
    .finally(() => writeStream.end());
}


