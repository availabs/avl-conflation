const path = require("path");

const { Pool, Client } = require("pg");

const npmrds_config = require(path.join(__dirname, "db.config.json"))

class Database {
  constructor(config) {
    this.config = { ...config };
    this.pool = new Pool(config);
  }
  query(...args) {
    return this.pool.query(...args)
      .then(({ rows }) => rows || []);
  }
  end() {
    return this.pool.end();
  }
}

module.exports = {
  npmrdsPool: new Database(npmrds_config),
  npmrdsClient: async () => {
    const client = new Client(npmrds_config);
    await client.connect();
    return client;
  }
}
