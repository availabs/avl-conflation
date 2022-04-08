const { getNodesAndWays } = require("./utils/getNodesAndWays")

module.exports = [
  { route: "/conflation.nodes-and-ways",
    post: (req, res) => {
      getNodesAndWays(req.body)
        .then(result => res.json({ ...result }))
        .catch(error => {
console.log("ERROR:", error);
          res.json({ error: error.message });
        });
    }
  },
]
