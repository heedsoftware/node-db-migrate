/* eslint-disable no-restricted-syntax */
/* eslint-disable guard-for-in */
/* eslint-disable no-console */
/* eslint-disable prefer-rest-params */
/* eslint-disable prefer-destructuring */
/* eslint-disable no-param-reassign */
/* eslint-disable default-case */
/* eslint-disable no-underscore-dangle */

const util = require('util');
const moment = require('moment');
const { ConnectionPool } = require('../../driver');

const Base = require('./base');
const log = require('../log');
const type = require('../data_type');

const SqlServerDriver = Base.extend({
  init(connection) {
    this._super();
    this.connection = connection;
  },

  close(callback) {
    this.connection.close();
    this.connection.on('end', () => {
      callback();
    });
  },

  mapDataType(spec) {
    let len = 8000;

    if (spec.length != null) {
      len = parseInt(spec.length, 10) || 8000;
    }

    switch (spec.type) {
      case type.CHAR:
        return len > 8000 ? 'char(max)' : `char(${len})`;
      case type.TEXT:
        if (len > 1073741824) {
          return 'text';
        }

        if (len > 8000) {
          return 'varchar(max)';
        }

        return `varchar(${len})`;
      case type.DATE_TIME:
        return 'datetime';
      case type.BLOB:
        if (len > 8000) {
          return 'varbinary(max)';
        }

        return 'varbinary';
      case type.BOOLEAN:
        return 'bit';
    }

    return this._super(spec.type);
  },

  createColumnDef(name, spec, options, tableName) {
    if (spec.type === 'string') {
      spec.type = 'text';
    }

    const escapedName = util.format('"%s"', name);
    const t = this.mapDataType(spec);
    let len;

    const constraint = this.createColumnConstraint(spec, options, tableName, name);
    return {
      foreignKey: constraint.foreignKey,
      constraints: [escapedName, t, len, constraint.constraints].join(' '),
    };
  },

  createColumnConstraint(spec, options, tableName, columnName) {
    const constraint = [];
    let cb;

    if (spec.unsigned) {
      constraint.push('UNSIGNED');
    }

    if (spec.primaryKey) {
      if (!options || options.emitPrimaryKey) {
        constraint.push('PRIMARY KEY');
      }
    }

    if (spec.primaryKey || spec.unique) {
      if (spec.autoIncrement) {
        constraint.push('IDENTITY(1,1)');
      }
    }

    if (spec.notNull === true) {
      constraint.push('NOT NULL');
    }

    if (spec.unique) {
      constraint.push('UNIQUE');
    }

    if (spec.null || spec.notNull === false) {
      constraint.push('NULL');
    }

    if (spec.defaultValue !== undefined) {
      if (spec.defaultConstraint !== undefined) {
        constraint.push(`CONSTRAINT ${spec.defaultConstraint}`);
      }

      constraint.push('DEFAULT');

      if (typeof spec.defaultValue === 'string') {
        constraint.push(`'${spec.defaultValue}'`);
      } else if (spec.defaultValue === null) {
        constraint.push('NULL');
      } else {
        constraint.push(`(${spec.defaultValue})`);
      }
    }

    if (spec.foreignKey) {
      cb = this.bindForeignKey(tableName, columnName, spec.foreignKey);
    }

    return {
      foreignKey: cb,
      constraints: constraint.join(' '),
    };
  },

  _makeParamArgs(args) {
    let params = Array.prototype.slice.call(args);
    const sql = params.shift();
    const callback = params.pop();

    if (params.length > 0 && Array.isArray(params[0])) {
      params = params[0];
    }

    return [sql, params, callback];
  },

  runSql() {
    const args = this._makeParamArgs(arguments);
    const callback = args[2];

    log.sql.apply(null, arguments);

    if (global.dryRun) {
      return callback();
    }

    const request = this.connection.request();

    return request
      .query(args[0])
      .then((result) => callback(null, result.recordset))
      .catch((err) => {
        console.log(err);
        callback(err);
      });
  },

  all() {
    const args = this._makeParamArgs(arguments);
    const callback = args[2];

    log.sql.apply(null, arguments);

    const request = this.connection.request();

    return request
      .query(args[0])
      .then((result) => callback(null, result.recordset))
      .catch((err) => {
        console.log(err);
        callback(err);
      });
  },

  allLoadedMigrations(callback) {
    const sql = `SELECT * FROM "${global.migrationTable}" ORDER BY run_on DESC, name DESC`;
    this.all(sql, callback);
  },

  addMigrationRecord(name, callback) {
    const formattedDate = moment(new Date()).format('YYYY-MM-DD HH:mm:ss');
    const sql = `INSERT INTO "${
      global.migrationTable
      }" ("name", "run_on") VALUES ('${name}', '${formattedDate}')`;
    this.runSql(sql, callback);
  },

  deleteMigration(migrationName, callback) {
    const sql = `DELETE FROM "${global.migrationTable}" WHERE name = '${migrationName}'`;
    this.runSql(sql, callback);
  },

  createTable(tableName, options, callback) {
    log.verbose('creating table:', tableName);
    let columnSpecs = options;
    let tableOptions = {};

    if (options.columns !== undefined) {
      columnSpecs = options.columns;
      tableOptions = options;
    }

    let ifNotExistsSql = '';
    if (tableOptions.ifNotExists) {
      ifNotExistsSql = `if not exists (select * from sysobjects where name='${tableName}' and xtype='U') `;
    }

    const primaryKeyColumns = [];
    const columnDefOptions = {
      emitPrimaryKey: false,
    };

    for (const columnName in columnSpecs) {
      const columnSpec = this.normalizeColumnSpec(columnSpecs[columnName]);
      columnSpecs[columnName] = columnSpec;
      if (columnSpec.primaryKey) {
        primaryKeyColumns.push(columnName);
      }
    }

    let pkSql = '';
    if (primaryKeyColumns.length > 1) {
      pkSql = util.format(', PRIMARY KEY ("%s")', primaryKeyColumns.join('", "'));
    } else {
      columnDefOptions.emitPrimaryKey = true;
    }

    const columnDefs = [];
    const foreignKeys = [];
    for (const columnName in columnSpecs) {
      const columnSpec = columnSpecs[columnName];
      const constraint = this.createColumnDef(columnName, columnSpec, columnDefOptions, tableName);

      columnDefs.push(constraint.constraints);
      if (constraint.foreignKey) {
        foreignKeys.push(constraint.foreignKey);
      }
    }

    const sql = util.format(
      '%s CREATE TABLE "%s" (%s%s)',
      ifNotExistsSql,
      tableName,
      columnDefs.join(', '),
      pkSql,
    );

    this.runSql(sql, (err) => {
      if (err) {
        callback(err);
        return;
      }

      this.recurseCallbackArray(foreignKeys, callback);
    });
  },

  renameTable(tableName, newTableName, callback) {
    const sql = util.format('RENAME TABLE "%s" TO "%s"', tableName, newTableName);
    this.runSql(sql, callback);
  },

  addColumn(tableName, columnName, columnSpec, callback) {
    const def = this.createColumnDef(
      columnName,
      this.normalizeColumnSpec(columnSpec),
      null,
      tableName,
    );
    const sql = util.format('ALTER TABLE "%s" ADD %s', tableName, def.constraints);
    this.runSql(sql, () => {
      if (def.foreignKey) {
        def.foreignKey(callback);
      } else {
        callback();
      }
    });
  },

  createDatabase(dbName, options, callback) {
    const spec = '';
    let ifNotExists = '';

    if (typeof options === 'function') {
      callback = options;
    } else {
      ifNotExists =
        options.ifNotExists === true
          ? `IF NOT EXISTS(select * from sys.databases where name='${dbName}') `
          : '';
    }

    this.runSql(util.format('%s CREATE DATABASE %s %s', ifNotExists, dbName, spec), callback);
  },

  switchDatabase(options, callback) {
    if (typeof options === 'object') {
      if (typeof options.database === 'string') {
        this.runSql(util.format('USE "%s"', options.database), callback);
      }
    } else if (typeof options === 'string') {
      this.runSql(util.format('USE "%s"', options), callback);
    } else {
      callback(null);
    }
  },

  dropDatabase(dbName, options, callback) {
    let ifExists = '';

    if (typeof options === 'function') {
      callback = options;
    } else {
      ifExists =
        options.ifExists === true
          ? `IF EXISTS (select * from sys.databases where name='${dbName}')`
          : '';
    }

    this.runSql(
      util.format(
        `
      %s
      BEGIN
      USE master;
      ALTER DATABASE ${dbName}
      SET AUTO_UPDATE_STATISTICS OFF;
      ALTER DATABASE ${dbName}
      SET SINGLE_USER WITH ROLLBACK IMMEDIATE;
      DROP DATABASE ${dbName}
      END
    `,
        ifExists,
      ),
      callback,
    );
  },

  removeConstraint(tableName, constraint, callback) {
    const exists = util.format(
      `IF EXISTS (SELECT * FROM sysobjects WHERE xtype = '%s' AND name = '%s')`,
      constraint.type,
      constraint.name,
    );
    const sql = util.format(
      `%s ALTER TABLE "%s" DROP CONSTRAINT "%s"`,
      exists,
      tableName,
      constraint.name,
    );
    this.runSql(sql, callback);
  },

  removeColumn(tableName, columnName, callback) {
    const exists = util.format(
      `IF EXISTS (SELECT column_name FROM INFORMATION_SCHEMA.columns WHERE table_name = '%s' AND column_name = '%s')`,
      tableName,
      columnName,
    );
    const sql = util.format(`%s ALTER TABLE "%s" DROP COLUMN "%s"`, exists, tableName, columnName);
    this.runSql(sql, callback);
  },

  addIndex(tableName, indexName, columns, unique, callback) {
    if (typeof unique === 'function') {
      callback = unique;
      unique = false;
    }

    if (!Array.isArray(columns)) {
      columns = [columns];
    }

    const sql = util.format(
      'CREATE %s INDEX "%s" ON "%s" ("%s")',
      unique ? 'UNIQUE ' : '',
      indexName,
      tableName,
      columns.join('", "'),
    );
    this.runSql(sql, callback);
  },

  insert(tableName, columnNameArray, valueArray, callback) {
    if (columnNameArray.length !== valueArray.length) {
      callback(new Error('The number of columns does not match the number of values.'));
      return;
    }

    let sql = util.format('INSERT INTO "%s" ', tableName);
    let columnNames = '(';
    let values = 'VALUES (';

    for (const index in columnNameArray) {
      columnNames += `"${columnNameArray[index]}"`;

      if (typeof valueArray[index] === 'string') {
        values += `'${this.escape(valueArray[index])}'`;
      } else {
        values += valueArray[index];
      }

      if (index !== columnNameArray.length - 1) {
        columnNames += ',';
        values += ',';
      }
    }

    sql += `${columnNames}) ${values});`;
    this.runSql(sql, callback);
  },

  removeIndex(tableName, indexName, callback) {
    if (arguments.length === 2 && typeof indexName === 'function') {
      callback = indexName;
      process.nextTick(() => {
        callback(new Error('Illegal arguments, must provide "tableName" and "indexName"'));
      });

      return;
    }

    const exists = util.format(
      `IF EXISTS (
      SELECT * FROM sys.indexes WHERE name='%s' AND object_id = OBJECT_ID('%s')
    )`,
      indexName,
      tableName,
    );
    const sql = util.format('%s DROP INDEX "%s" ON "%s"', exists, indexName, tableName);
    this.runSql(sql, callback);
  },

  dropTable(tableName, options, callback) {
    if (arguments.length < 3) {
      callback = options;
      options = {};
    }

    let ifExistsSql = '';
    if (options.ifExists) {
      ifExistsSql = `IF EXISTS (select * from sys.tables where name='${tableName}') `;
    }

    const sql = util.format('%s DROP TABLE %s', ifExistsSql, tableName);
    this.runSql(sql, callback);
  },

  renameColumn(tableName, oldColumnName, newColumnName, callback) {
    const alterSql = util.format(
      'sp_rename "%s.%s", "%s", COLUMN',
      tableName,
      oldColumnName,
      newColumnName,
    );
    this.runSql(alterSql, callback);
  },

  changeColumn(tableName, columnName, columnSpec, callback) {
    const constraint = this.createColumnDef(columnName, columnSpec);
    const sql = util.format('ALTER TABLE "%s" ALTER COLUMN %s', tableName, constraint.constraints);

    const exec = () => {
      this.runSql(sql, () => {
        if (constraint.foreignKey) {
          constraint.foreignKey(callback);
        } else {
          callback();
        }
      });
    };

    if (columnSpec.unique === false) {
      this.removeIndex(tableName, columnName, exec);
    } else {
      exec();
    }
  },

  addForeignKey(tableName, referencedTableName, keyName, fieldMapping, rules, callback) {
    if (arguments.length === 5 && typeof rules === 'function') {
      callback = rules;
      rules = {};
    }

    if (rules.onDelete && rules.onDelete === 'RESTRICT') {
      rules.onDelete = 'NO ACTION';
    }

    if (rules.onUpdate && rules.onUpdate === 'RESTRICT') {
      rules.onUpdate = 'NO ACTION';
    }

    const columns = Object.keys(fieldMapping);
    const referencedColumns = columns.map((key) => {
      return fieldMapping[key];
    });
    const sql = util.format(
      'ALTER TABLE "%s" ADD CONSTRAINT "%s" FOREIGN KEY (%s) REFERENCES "%s" (%s) ON DELETE %s ON UPDATE %s',
      tableName,
      keyName,
      this.tableQuoteArr(columns),
      referencedTableName,
      this.tableQuoteArr(referencedColumns),
      rules.onDelete || 'NO ACTION',
      rules.onUpdate || 'NO ACTION',
    );
    this.runSql(sql, callback);
  },

  removeForeignKey(tableName, keyName, options, callback) {
    let sql = util.format('ALTER TABLE "%s" DROP FOREIGN KEY "%s"', tableName, keyName);
    this.runSql(sql, () => {
      if (typeof options === 'function') {
        options();
      } else if (options.dropIndex === true) {
        sql = util.format('ALTER TABLE "%s" DROP INDEX "%s"', tableName, keyName);
        this.runSql(sql, () => {
          callback();
        });
      } else {
        callback();
      }
    });
  },

  tableQuoteArr(arr) {
    for (let i = 0; i < arr.length; i += 1) {
      arr[i] = `"${arr[i]}"`;
    }

    return arr;
  },
});

exports.connect = (config, callback) => {
  const connection = new ConnectionPool(config);

  connection.connect((err) => {
    if (err) {
      console.log('Error', err);
      callback(err);
      return;
    }

    // If no error, then good to proceed.
    console.log('Connected');

    callback(null, new SqlServerDriver(connection));
  });

  connection.on('error', (err) => console.log('Error', err));
};
