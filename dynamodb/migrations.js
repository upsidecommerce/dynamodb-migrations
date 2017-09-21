'use strict';

var BbPromise = require('bluebird'),
    fs = require('fs'),
    path = require('path');

var createTable = function(dynamodb, migration) {
  console.log("Starting table creation for table: " + migration.Table.TableName);
  return new BbPromise(function(resolve, reject) {
    dynamodb.raw.createTable(migration.Table, function(err) {
      if (err) {
        if(err.code === 'ResourceInUseException') {
          console.log('Table ' + migration.Table.TableName + ' already exists. Skipping creation...');
          return resolve(migration);
        }
        console.log(err);
        reject(migration);
      } else {
        console.log("Table creation requested for table: " + migration.Table.TableName + '. Waiting for Dynamo to confirm that the table is ready...');
        dynamodb.raw.waitFor('tableExists', {TableName: migration.Table.TableName}, function(err) {
          if(err) {
            console.log(err)
            reject(err);
          }

          console.log("Table creation completed for table: " + migration.Table.TableName);

          resolve(migration);
        });
      }
    });
  });
};

var formatTableName = function(migration, options) {
    return options.tablePrefix + migration.Table.TableName + options.tableSuffix;
};

var runSeeds = function(dynamodb, migration) {
    if (!migration.Seeds || !migration.Seeds.length) {
        return new BbPromise(function(resolve) {
            resolve(migration);
        });
    } else {
        var params,
            batchSeeds = migration.Seeds.map(function(seed) {
                return {
                    PutRequest: {
                        Item: seed
                    }
                };
            });
        params = {
            RequestItems: {}
        };
        params.RequestItems[migration.Table.TableName] = batchSeeds;
        return new BbPromise(function(resolve, reject) {
            var interval = 0,
                execute = function(interval) {
                    setTimeout(function() {
                        dynamodb.doc.batchWrite(params, function(err) {
                            if (err) {
                                if (err.code === "ResourceNotFoundException" && interval <= 5000) {
                                    execute(interval + 1000);
                                } else {
                                    reject(err);
                                }
                            } else {
                                console.log("Seed running complete for table: " + migration.Table.TableName);
                                resolve(migration);
                            }
                        });
                    }, interval);
                };
            execute(interval);
        });
    }
};

var create = function(migrationName, options) {
    return new BbPromise(function(resolve, reject) {
        var template = require('./templates/table.json');
        template.Table.TableName = migrationName;

        if (!fs.existsSync(options.dir)) {
            fs.mkdirSync(options.dir);
        }

        fs.writeFile(options.dir + '/' + migrationName + '.json', JSON.stringify(template, null, 4), function(err) {
            if (err) {
                return reject(err);
            } else {
                resolve('New file created in ' + options.dir + '/' + migrationName + '.json');
            }
        });
    });
};
module.exports.create = create;

var executeAll = function(dynamodb, options) {
    var tableExec = BbPromise.map(fs.readdirSync(options.dir), function(file) {
          if (path.extname(file) === ".json") {
            var migration = require(options.dir + '/' + file);
            migration.Table.TableName = formatTableName(migration, options);
            return createTable(dynamodb, migration);
          }
          return BbPromise.resolve();
    } ,{concurrency:options.tableConcurrency});

    return tableExec.then(function(migrations) {
        return BbPromise.map(migrations, function(migration) {
          return runSeeds(dynamodb, migration);
        }, {concurrency:options.seedConcurrency});
    });
};
module.exports.executeAll = executeAll;

var execute = function(dynamodb, options) {
    return new BbPromise(function(resolve, reject) {
        var migration = require(options.dir + '/' + options.migrationName + '.json');
        migration.Table.TableName = formatTableName(migration, options);
        createTable(dynamodb, migration).then(function(executedMigration) {
            runSeeds(dynamodb, executedMigration).then(resolve, reject);
        });
    });
};
module.exports.execute = execute;
