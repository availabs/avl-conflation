
const concaveman = require("concaveman")

const buffer = require("@turf/buffer").default;
const union = require("@turf/union").default;

const { writeFileSync } = require("fs")

const {
  getNodesFromCoords,
  getNpmrds,
  walkGraph
} = require("./graph");

const timeToEpoch = time => {
  const [hour, minutes] = time.split(":").map(t => + t);
  return hour * 12 + Math.floor(minutes / 5);
}

const generateExperientialIsochrone = async request => {

  const {
    startPoint,
    startTime = "12:00",
    startDate = "01-01-2020",
    endDate = "12-31-2020",
    weekdays = [1, 2, 3, 4, 5],
    durations = [5, 15, 30]
  } = request;

  const startEpoch = timeToEpoch(startTime);

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
                                  startEpoch, startEpoch + epochs);
console.timeEnd("RECEIVED NPMRDS DATA")

console.log("WALKING GRAPH")
console.time("FINISHED WALKING GRAPH")
  const data = await walkGraph(startNode, durations.map(d => d * 60), startEpoch, NPMRDS);
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

const generateIsochrone = async request => {

  const {
    startPoint,
    startTime = "12:00",
    startDate = "01-01-2020",
    endDate = "12-31-2020",
    weekdays = [1, 2, 3, 4, 5],
    durations = [5, 15, 30]
  } = request;

  const startEpoch = timeToEpoch(startTime);

console.log("RECEIVED REQUEST")
console.time("FINISHED REQUEST")

  const maxDuration = durations[durations.length - 1],
    epochs = Math.ceil(maxDuration / 5),
    miles = maxDuration * (75 / 60);

  const [startNode] = await getNodesFromCoords([startPoint]);

  if (!startNode) return null;

console.log("WALKING GRAPH")
console.time("FINISHED WALKING GRAPH")
  const data = await walkGraph(startNode, durations.map(d => d * 60), startEpoch);
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

module.exports = {
  generateExperientialIsochrone,
  generateIsochrone
};
