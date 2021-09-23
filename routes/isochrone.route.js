const { generateIsochrone } = require("./utils/generateIsochrone")

module.exports = [
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
