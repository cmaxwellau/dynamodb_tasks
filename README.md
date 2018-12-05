# dynamodb_tasks
## Installation
* Git clone repository / unzip compressed file
* $ npm install package.json 

## Usage
```
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
    --profile           Name of AWS CLI profile contained in ~/.aws/credentials
      --region            Target AWS region
      --table             Name of DynamoDB table to import/export
      --file              File name to use to import/export of table data and schemas
```


## Examples
List all DynamoDB tables
```
node dynamodb_tasks.js list-tables --region=us-east-1
```

Export DynamoDB Table Schema
```
node dynamodb_tasks.js export-schema --region=us-east-1 --table=example_table --file=example_file
```

Export DynamoDB Table Data
```
node dynamodb_tasks.js export-data --region=us-east-1  --table=example_table --file=example_file
```

Import DynamoDB Table Schema
```
node dynamodb_tasks.js import-schema --region=us-east-1  --table=example_table --file=example_file
```

Import DynamoDB Table Data
```
node dynamodb_tasks.js import-data --region=us-east-1  --table=example_table --file=example_file
```
