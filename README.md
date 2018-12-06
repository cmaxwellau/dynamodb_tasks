# dynamodb_tasks
This is a tool for exporting and importing table data and schemas from DynamoDB.

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

## Installation
* Git clone this repository.
* ```npm install``` to download and install node dependencies.

## Configuration
###AWS Region 
You can set a default region in the CLI environment. This can still be overriden by an explicit command line option.

```
$ export AWS_REGION=ap-southeast-2
```

###AWS Credentials
Configure AWS credentials using a profile in ~/.aws/credentials and refer to it through an environment variable:

```
$ export AWS_PROFILE=my-dev-account-profile
```

You can also configure explicit credentials using environment variables:

```
$ export AWS_ACCESS_KEY_ID=AKIA12345678ABCDEAFGH
$ export AWS_SECRET_ACCESS_KEY=12345678ABCDEFGHabcdefgh12345678abcdefgh
```






## Usage Examples

### List all DynamoDB tablesexzpo

```
$ node dynamodb_tasks.js list-tables
```

### Export DynamoDB Table Schema

```
$ node dynamodb_tasks.js export-schema --table=example_table --file=example_file
```

### Export DynamoDB Table Data

```
$ node dynamodb_tasks.js export-data --table=example_table --file=example_file
```

### Import DynamoDB Table Schema

```
$ node dynamodb_tasks.js import-schema --table=example_table --file=example_file
```

### Import DynamoDB Table Data

```
$ node dynamodb_tasks.js import-data--table=example_table --file=example_file
```
