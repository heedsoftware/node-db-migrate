const config = require('@heed/core-utils.config');

module.exports = config.get('reporting_database:driver') === 'tedious' || process.env.CI === 'true'
  ? require('./tedious') 
  : require('./msnodesqlv8');