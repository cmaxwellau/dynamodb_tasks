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
	  $ node dynamodb_tasks.js export-schema <option> Export the schema
	  $ node dynamodb_tasks.js export-table <option> Export the table
	  $ node dynamodb_tasks.js export-table <option> Export the table

	  AWS credentials are specified in ~/.aws/credentials

	Options
	  --region AWS region
	  --table Table to export
	  --file File name to export to 

	Examples
	  node dynamodb_tasks.js export-schema --region=us-east-1 --table=example_table --file=example_file
	  node dynamodb_tasks.js export-table --region=us-east-1  --table=example_table --file=example_file
	  node dynamodb_tasks.js import-schema --region=us-east-1  --table=example_table --file=example_file
	  node dynamodb_tasks.js import-data --region=us-east-1  --table=example_table --file=example_file

	`);

const methods = {
  'export-schema': exportSchemaCli,
  'import-schema': importSchemaCli,
  'list-tables': listTablesCli,
  'export-all-schema': exportAllSchemaCli,
  'export-table': exportDataCli,
  'export-all-data': exportAllDataCli,
  'import-data': importDataCli
};
if (cli.flags.maxRetries !== undefined) AWS.config.maxRetries = cli.flags.maxRetries;

const method = methods[cli.input[0]] || cli.showHelp();

if (cli.flags.profile) {
  AWS.config.credentials = new AWS.SharedIniFileCredentials({profile: cli.flags.profile});
}


bluebird.resolve(method.call(undefined, cli))
  .catch(err => {
    console.error(err.stack);
    process.exit(1);
  });

function listTablesCli(cli) {
  const region = cli.flags.region;

  return listTables(region)
    .then(tables => console.log(tables.join(' ')));
}

function listTables(region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  const params = {};

  let tables = [];
  const listTablesPaged = () => {
    return dynamoDb.listTables(params).promise()
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

function exportSchemaCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportSchema(tableName, cli.flags.file, cli.flags.region)
}

function exportAllSchemaCli(cli) {
  const region = cli.flags.region;
  return bluebird.map(listTables(region), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportSchema(tableName, null, region);
  }, { concurrency: 1 });
}

function exportSchema(tableName, file, region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  return dynamoDb.describeTable({ TableName: tableName }).promise()
    .then(data => {
      const table = data.Table;
      const file2 = file || sanitizeFilename(tableName + '.dynamoschema');

      return fs.writeFileAsync(file2, JSON.stringify(table, null, 2))
    });
}

function importSchemaCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;
  const waitForActive = cli.flags.waitForActive;

  if (!file) {
    console.error('--file is required')
    cli.showHelp();
  }

  const dynamoDb = new AWS.DynamoDB({ region });

  const doWaitForActive = () => promisePoller({
    taskFn: () => {
      return dynamoDb.describeTable({ TableName: tableName }).promise()
        .then(data => {
          if (data.Table.TableStatus !== 'ACTIVE') throw new Error();
        });
    },
    interval: 1000,
    retries: 60
  });

  fs.readFileAsync(file)
    .then(data => JSON.parse(data))
    .then(json => {
      if (tableName) json.TableName = tableName;

      filterTable(json);

      return dynamoDb.createTable(json).promise()
        .then(() => {
          if (waitForActive !== undefined) {
            return doWaitForActive();
          }
        });
    });
}

function filterTable(table) {
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

function importDataCli(cli) {
  const tableName = cli.flags.table;
  const file = cli.flags.file;
  const region = cli.flags.region;

  if (!tableName) {
    console.error('--table is required')
    cli.showHelp();
  }
  if (!file) {
    console.error('--file is required')
    cli.showHelp();
  }

  const dynamoDb = new AWS.DynamoDB({ region });

  const readStream = fs.createReadStream(file);
  const parseStream = JSONStream.parse('*');

  let n = 0;

  const logProgress = () => console.error('Imported', n, 'items');
  const logThrottled = _.throttle(logProgress, 5000, { trailing: false });

  readStream.pipe(parseStream)
    .on('data', data => {
      debug('data');

      n++;
      logThrottled();

      parseStream.pause();
      dynamoDb.putItem({ TableName: tableName, Item: data }).promise()
        .then(() => parseStream.resume())
        .catch(err => parseStream.emit('error', err));
    });

  return new Promise((resolve, reject) => {
    parseStream.on('end', resolve);
    parseStream.on('error', reject);
  })
    .then(() => logProgress());
}

function exportDataCli(cli) {
  const tableName = cli.flags.table;

  if (!tableName) {
    console.error('--table is requred')
    cli.showHelp();
  }

  return exportData(tableName, cli.flags.file, cli.flags.region);
}

function exportAllDataCli(cli) {
  const region = cli.flags.region;
  return bluebird.map(listTables(region), tableName => {
    console.error(`Exporting ${tableName}`);
    return exportData(tableName, null, region);
  }, { concurrency: 1 });
}

function exportData(tableName, file, region) {
  const dynamoDb = new AWS.DynamoDB({ region });

  const file2 = file || sanitizeFilename(tableName + '.dynamodata');
  const writeStream = fs.createWriteStream(file2);
  const stringify = JSONStream.stringify();
  stringify.pipe(writeStream);

  let n = 0;

  const params = { TableName: tableName };
  const scanPage = () => {
    return bluebird.resolve(dynamoDb.scan(params).promise())
      .then(data => {
        data.Items.forEach(item => stringify.write(item));

        n += data.Items.length;
        console.error('Exported', n, 'items');

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


