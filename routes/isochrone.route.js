const {
  generateIsochrone,
  generateExperientialIsochrone
} = require("./utils/generateIsochrone")

module.exports = [
  { route: "/conflation.isochrone.experiential",
    post: (req, res) => {
      generateExperientialIsochrone(req.body)
        .then(geo => res.json({ result: geo }))
        .catch(error => {
console.log("ERROR:", error);
          res.json({ error: error.message });
        });
    }
  },
  { route: "/conflation.isochrone",
    post: (req, res) => {
      generateIsochrone(req.body)
        .then(geo => res.json({ result: geo }))
        .catch(error => {
console.log("ERROR:", error);
          res.json({ error: error.message });
        });
    }
  }
]
