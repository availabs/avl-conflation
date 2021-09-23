
const concaveman = require("concaveman")

const buffer = require("@turf/buffer").default;
const union = require("@turf/union").default;

const { writeFileSync } = require("fs")

const {
  getNodesFromCoords,
  getNpmrds,
  walkGraph
} = require("./graph");

const generateIsochrone = async request => {

  const {
    startPoint,
    startTime = 144,
    startDate = "01-01-2020",
    endDate = "12-31-2020",
    weekdays = [1, 2, 3, 4, 5],
    durations = [5, 15, 30]
  } = request;

console.log("RECEIVED REQUEST")
console.time("FINISHED REQUEST")

  const maxDuration = durations[durations.length - 1],
    epochs = Math.ceil(maxDuration / 5),
    miles = maxDuration * (75 / 60);

  const [startNode] = await getNodesFromCoords([startPoint]);

  if (!startNode) return null;

console.log("GETTING NPMRDS DATA")
console.time("RECEIVED NPMRDS DATA")
  const NPMRDS = await getNpmrds(startPoint, miles,
                                  startDate, endDate,
                                  startTime, startTime + epochs);
console.timeEnd("RECEIVED NPMRDS DATA")

console.log("WALKING GRAPH")
console.time("FINISHED WALKING GRAPH")
  const data = await walkGraph(startNode, durations.map(d => d * 60), NPMRDS, startTime);
console.timeEnd("FINISHED WALKING GRAPH")

  const [features] = data.reduce((a, c, i) => {
    const polygon = concaveman([...c.values(), ...a[1]], 2, 0);
    a[0].push({
      type: "Feature",
      properties: { time: durations[i] },
      geometry: {
        type: "Polygon",
        coordinates: [polygon]
      }
    });
    a[1].push(...polygon);
    return a;
  }, [[], []]);

  const collection = {
    type: "FeatureCollection",
    features
  };
console.timeEnd("FINISHED REQUEST")

  return collection;
}

module.exports = { generateIsochrone };
