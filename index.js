
const express = require('express');
const cluster = require('cluster');
const os = require("os");

const numCPUs = os.cpus().length;
const NUM_WORKERS = +process.env.NUM_WORKERS || (numCPUs - 1);

const app = express();

const PORT = process.env.PORT || 4445;

const routes = require("./routes");

const { loadConflationData } = require("./routes/utils/graph")

const setupWorkerProcesses = () => {

  for (let i = 0; i < NUM_WORKERS; ++i) {
    cluster.fork();
  }

  cluster.on('online', worker => {
    console.log(`Worker ${ worker.process.pid } is online.`);
  });

  cluster.on('exit', (worker, code, signal) => {
    console.log(
      `Worker ${ worker.process.pid }
      died with code: ${ code },
      and signal: ${ signal }.`
    );
    cluster.fork();
  });
};

const setUpExpress = () => {

  app.use(express.json({ limit: "500mb" }));
  app.use(express.urlencoded({
    limit: "10mb", extended: true,
    type: "application/x-www-form-urlencoded"
  }));

  app.use((req, res, next) => {
    res.header('Access-Control-Allow-Origin', req.get('origin'));
    res.header('Cache-Control', 'no-store, no-cache, must-revalidate');
    res.header('Access-Control-Allow-Credentials', true);
    res.header(
      'Access-Control-Allow-Headers',
      'Origin, X-Requested-With, Content-Type, Accept, Authorization'
    );
    res.header(
      'Access-Control-Allow-Methods',
      'GET, PUT, POST, DELETE, PATCH, OPTIONS'
    );

    if (req.method === 'OPTIONS') {
      return res.end();
    }

    return next();
  });

  app.use(express.static(`${ __dirname }/static`));

  routes.forEach(({ route, get, post }) => {
    const method = get ? "get" : "post";
    app[method](route, get || post);
  });

  app.listen(PORT);
};

const setupServer = async () => {
  if (cluster.isMaster) {
    setupWorkerProcesses();
    console.log(`Server is listening on port: ${ PORT }`);
  }
  else {
    loadConflationData();
    setUpExpress();
  }
};

setupServer();
