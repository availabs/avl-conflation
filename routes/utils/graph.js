const { pipeline, Writable } = require("stream");

const split = require("split2");
const { to: copyTo } = require('pg-copy-streams');
const { rollup: d3rollup, mean: d3mean, group: d3group } = require("d3-array");
const { scaleLinear } = require("d3-scale");

const turfDistance = require("@turf/distance").default;

const createGraph = require("ngraph.graph");
const { aStar } = require("ngraph.path")

const { npmrdsPool, npmrdsClient } = require("./db_service")

const CONFLATION_VERSION = "v0_6_0"

let NODES = [];
let WAYS = [];
let GRAPH = createGraph();
let AVG_SPEED_LIMIT_BY_NETWORK_LEVEL = new Map();
let TMC_META = new Map();

const streamNodes = async client => {

  const graphWriter = new Writable({
    write(chunk, enc, callback) {
      const [id, geoJSON] = chunk.toString().split("|");
      const geom = JSON.parse(geoJSON);
      GRAPH.addNode(+id, { coords: geom.coordinates });
      callback(null);
    }
  });

  await new Promise((resolve, reject) => {
    pipeline(
      client.query(
        copyTo(`
          COPY (
            SELECT id, ST_AsGeoJSON(wkb_geometry) AS geom
            FROM conflation.conflation_map_2020_nodes_${ CONFLATION_VERSION }
            WHERE id IN (
              SELECT unnest(a.node_ids)
              FROM conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS a
                JOIN conflation.conflation_map_2020_${ CONFLATION_VERSION } AS b
                  USING(id)
              WHERE n < 7
            )
          )
          TO STDOUT WITH (FORMAT TEXT, DELIMITER '|')`)
      ),
      split(),
      graphWriter,
      err => {
        if (err) {
          reject(err);
        } else {
          resolve(null);
        }
      }
    )
  });
}

const streamWays = async client => {

  const graphWriter = new Writable({
    write(chunk, enc, callback) {
      const [id, nodesJSON, tmc, n] = chunk.toString().split("|");
      const nodes = JSON.parse(nodesJSON);
      for (let i = 1; i < nodes.length; ++i) {
        if (nodes[i - 1] !== nodes[i]) {
          const n1 = GRAPH.getNode(+nodes[i - 1]),
            n2 = GRAPH.getNode(+nodes[i]),
            dist = turfDistance(n1.data.coords, n2.data.coords);
          GRAPH.addLink(+nodes[i - 1], +nodes[i], { wayId: id, tmc, n, dist });
        }
      }
      callback(null);
    }
  });

  return new Promise((resolve, reject) => {
    pipeline(
      client.query(
        copyTo(`
          COPY (
            SELECT a.id, array_to_json(a.node_ids), b.tmc, b.n
            FROM conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS a
              JOIN conflation.conflation_map_2020_${ CONFLATION_VERSION } AS b
                USING(id)
            WHERE n < 7
          )
          TO STDOUT WITH (FORMAT CSV, DELIMITER '|')`
        )
      ),
      split(),
      graphWriter,
      err => {
        if (err) {
          reject(err);
        }
        else {
          resolve(null);
        }
      }
    )
  })
  .catch(error => {
    console.log("There was an error streaming ways:", error);
  });

}

const getAvgSpeedLimitByNetworkLevel = async () => {
  const sql = `
    SELECT n, tags->>'maxspeed' AS maxspeed
    FROM conflation.conflation_map_2020_${ CONFLATION_VERSION } AS c
      JOIN osm.osm_ways_v200101 AS o
        ON c.osm = o.id
    WHERE o.tags->>'maxspeed' IS NOT NULL;
  `
  const data = await npmrdsPool.query(sql);

  const reducer = group => {
    return d3mean(group, d => parseInt(d.maxspeed)) / 3600.0;
  }

  AVG_SPEED_LIMIT_BY_NETWORK_LEVEL = d3rollup(data, reducer, d => +d.n);
}

const getTmcMeta = async () => {
  const sql = `
    SELECT tmc, miles
    FROM tmc_metadata_2020
  `
  TMC_META = await npmrdsPool.query(sql)
    .then(rows => rows.reduce((a, c) => {
      a.set(c.tmc, c.miles);
      return a;
    }, new Map()));
}

const getNpmrds = async (point, miles, startDate, endDate, startTime, endTime) => {

  startTime = Math.max(0, startTime - 3);
  endTime - Math.min(288, endTime + 3);

  const sql = `
    SELECT tmc, epoch, AVG(travel_time_all_vehicles) AS tt
    FROM npmrds
    WHERE date >= $1 AND date <= $2
    AND epoch >= $3 AND epoch < $4
    AND tmc = ANY(
      SELECT DISTINCT tmc
      FROM conflation.conflation_map_2020_${ CONFLATION_VERSION }
      WHERE n < 7
      AND ST_DWithin(
        ST_Transform(wkb_geometry, 2877),
        ST_Transform('SRID=4326;POINT(${ +point.lng } ${ +point.lat })'::geometry, 2877),
        ${ +miles } * 1609.34
      )
    )
    GROUP BY 1, 2
  `
  const rows = await npmrdsPool.query(sql, [startDate, endDate, startTime, endTime]);

  const byTmc = d3rollup(rows, g => g.map(d => d.tt).pop(), d => d.tmc, d => +d.epoch)

  const NPMRDS = new Map();

  const doInterpolate = (tmc, epochs) => {
    const scale = scaleLinear(),
      domain = [],
      range = [],
      missing = [];

    for (let e = +startTime; e < +endTime; ++e) {
      if (epochs.has(e)) {
        domain.push(e);
        range.push(epochs.get(e));
        NPMRDS.get(tmc).set(e, epochs.get(e));
      }
      else {
        missing.push(e);
      }
    }

    scale.domain(domain).range(range);

    for (const e of missing) {
      NPMRDS.get(tmc).set(e, scale(e));
    }
  }

  byTmc.forEach((epochs, tmc) => {
    NPMRDS.set(tmc, new Map());

    for (let e = startTime; e < endTime; ++e) {
      if (epochs.has(e)) {
        NPMRDS.get(tmc).set(e, epochs.get(e));
      }
      else {
        doInterpolate(tmc, epochs);
        break;
      }
    }
  })

  return NPMRDS;

  // return rows.reduce((a, c) => {
  //   a.set(c.tmc, c.tt);
  //   return a;
  // }, new Map());
}

const getNodesFromCoords = async coords => {
  const promises = coords.map((lngLat, i) => {
    const sql = `
      SELECT id, ${ i } AS index
      FROM conflation.conflation_map_2020_nodes_${ CONFLATION_VERSION }
      WHERE id = ANY(
        SELECT UNNEST(node_ids)
        FROM conflation.conflation_map_2020_ways_${ CONFLATION_VERSION } AS w
          JOIN conflation.conflation_map_2020_${ CONFLATION_VERSION } AS m
            USING(id)
        WHERE m.n < 7
      )
      ORDER BY wkb_geometry <-> 'SRID=4326;POINT(${ +lngLat.lng } ${ +lngLat.lat })'::geometry
      LIMIT 1
    `
    return npmrdsPool.query(sql)
      .then(rows => rows.pop())
  })
  const rows = await Promise.all(promises);

  return rows.sort((a, b) => +a.index - +b.index).map(({ id }) => +id);
}

const getTravelTime = (tmc, epoch, NPMRDS, miles, n) => {
  if (NPMRDS && NPMRDS.has(tmc) && NPMRDS.get(tmc).has(+epoch) && TMC_META.has(tmc)) {
    const tmcMiles = TMC_META.get(tmc);
    return NPMRDS.get(tmc).get(+epoch) * (miles / tmcMiles);
  }
  return miles / AVG_SPEED_LIMIT_BY_NETWORK_LEVEL.get(+n);
}

const walkGraph = (startNode, durations, startTime, NPMRDS = null) => {
  const requests = [{
    nodeId: startNode,
    travelTime: 0,
    epoch: startTime
  }];

  const linksForDurations = durations.map(() => new Map());

  const visitedNodes = new Map();

  visitedNodes.set(startNode, 0);

  let index = 0;

  while (index < requests.length) {
    const { nodeId, travelTime, epoch } = requests[index++];

    const node = GRAPH.getNode(+nodeId);

    if (node && node.links) {
      node.links
        .filter(({ fromId }) => +fromId === +nodeId)
        .forEach(({ toId, data }) => {
          const toNode = GRAPH.getNode(+toId);

          const id = +toId;

          const { tmc, n } = data;

          const miles = turfDistance(node.data.coords, toNode.data.coords, { units: "miles" });

          const tt = travelTime + getTravelTime(tmc, epoch, NPMRDS, miles, n);

          const newEpoch = startTime + Math.floor(tt / 300.0);

          for (const i in durations) {
            const duration = durations[i];
            if ((tt < duration) &&
                (!visitedNodes.has(id) || (visitedNodes.get(id) > tt))
              ) {

              visitedNodes.set(id, tt);

              linksForDurations[i].set(id, toNode.data.coords);

              requests.push({
                nodeId: +toId,
                travelTime: tt,
                epoch: newEpoch
              });
              break;
            }
          }
        })
    }
  }

  return linksForDurations;
}

const getRoute = async coords => {

console.log("COORDS:", coords)

  const nodes = await getNodesFromCoords(coords);

console.log("NODES:", nodes)

  const pathFinder = aStar(GRAPH, {
    oriented: true,
    distance: (from, to, link) => {
      return link.data.dist;
    }
  });

  const path = [];
  for (let i = 1; i < nodes.length; ++i) {
    path.push(...pathFinder.find(nodes[i - 1], nodes[i]).reverse());
  }

console.log("PATH:", path);

  const route = [];

  for (let i = 1; i < path.length; ++i) {
    const from = path[i - 1].id,
      to = path[i].id,
      links = path[i].links;

    const wayId = links.reduce((a, c) => {
      return c.fromId === from && c.toId === to ? c.data.wayId : a;
    }, null);

    if (wayId && !route.includes(wayId)) {
      route.push(wayId);
    };
  }
console.log("ROUTE:", route)
  return route;
}

module.exports = {
  getNodesFromCoords,
  getNpmrds,
  walkGraph,
  getRoute,

  loadConflationData: async () => {
console.log(`LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);

    const client = await npmrdsClient();

    try {

      GRAPH = createGraph();

console.log(`STREAMING NODES FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED STREAMING NODES FOR PROCESS: ${ process.pid }`);
      await streamNodes(client);
console.timeEnd(`FINISHED STREAMING NODES FOR PROCESS: ${ process.pid }`);

console.log(`STREAMING WAYS FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED STREAMING WAYS FOR PROCESS: ${ process.pid }`);
      await streamWays(client);
console.timeEnd(`FINISHED STREAMING WAYS FOR PROCESS: ${ process.pid }`);

console.log(`LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);
      await getAvgSpeedLimitByNetworkLevel();
console.timeEnd(`FINISHED LOADING AVG SPEED LIMITS FOR PROCESS: ${ process.pid }`);

console.log(`LOADING TMC META FOR PROCESS: ${ process.pid }`);
console.time(`FINISHED LOADING TMC META FOR PROCESS: ${ process.pid }`);
      await getTmcMeta();
console.timeEnd(`FINISHED LOADING TMC META FOR PROCESS: ${ process.pid }`);

    }
    catch (e) {
      console.log("ERROR:", e);
    }
    finally {
      client.end();
    }

console.timeEnd(`FINISHED LOADING GRAPH DATA FOR PROCESS: ${ process.pid }`);
  },
}
