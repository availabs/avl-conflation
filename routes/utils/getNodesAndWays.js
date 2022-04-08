
const { npmrdsPool } = require("./db_service");

const { getGraph, getWayMap } = require("./graph");

const getNodesAndWays = async request => {
  const {
    point = { lng: -74.22390566699274, lat: 42.59112220705157 },
    distance = 5
  } = request;

  const meters = 1609.34 * +distance * 1.25;

console.log("RECEIVED REQUEST:", new Date().toLocaleString());
console.time("NODES AND WAYS")

  const sql1 = `
    SELECT id, ST_AsGeoJSON(wkb_geometry) AS geom
    FROM conflation.conflation_map_2020_nodes_v0_6_0
    WHERE id IN (
      SELECT unnest(a.node_ids)
      FROM conflation.conflation_map_2020_ways_v0_6_0 AS a
        JOIN conflation.conflation_map_2020_v0_6_0 AS b
          USING(id)
      WHERE n < 7
    )
    AND ST_Transform(wkb_geometry, 26918) <->
      ST_Transform(ST_SetSRID(ST_Point(${ +point.lng }, ${ +point.lat }), 4326), 26918) <= ${ meters }
    ORDER BY wkb_geometry <-> ST_SetSRID(ST_Point(${ +point.lng }, ${ +point.lat }), 4326) ASC
  `;
  const nodeIds = await npmrdsPool.query(sql1);
  const [closest] = nodeIds;

  // const sql2 = `
  //   SELECT id, ST_AsGeoJSON(wkb_geometry) AS geom
  //   FROM conflation.conflation_map_2020_nodes_v0_6_0
  //   WHERE id IN (
  //     SELECT unnest(a.node_ids)
  //     FROM conflation.conflation_map_2020_ways_v0_6_0 AS a
  //       JOIN conflation.conflation_map_2020_v0_6_0 AS b
  //         USING(id)
  //     WHERE n < 7
  //   )
  //   ORDER BY wkb_geometry <-> ST_SetSRID(ST_Point(${ +point.lng }, ${ +point.lat }), 4326) ASC
  //   LIMIT 1
  // `;
  // const [closest] = await npmrdsPool.query(sql2);

console.timeEnd("NODES AND WAYS")

  const GRAPH = getGraph(),
    WAY_MAP = getWayMap();

  const nodeMap = new Map(),
    wayMap = new Map();

  nodeIds.forEach(({ id }) => {
    const node = GRAPH.getNode(+id);
    if (node) {
      node.links.forEach(link => {
        const way = WAY_MAP.get(link.data.wayId);
        if (way) {
          wayMap.set(way.id, way);
        }
      })
    }
  });

  const ways = [...wayMap.values()].map(w => ({ ...w, id: +w.id }));

  ways.forEach(({ nodes }) => {
    nodes.forEach(id => {
      const node = GRAPH.getNode(+id);
      if (node) {
        nodeMap.set(+id, { id: +id, coords: node.data.coords });
      }
    })
  })

  return {
    nodes: [...nodeMap.values()].map(n => ({ id: n.id, geom: { type: "Point", coordinates: n.coords } })),
    ways,
    closest: { id: +closest.id, geom: JSON.parse(closest.geom) }
  };
};

module.exports = { getNodesAndWays };
