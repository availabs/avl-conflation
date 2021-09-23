
const { getRoute } = require("./utils/graph")

module.exports = [
  { route: "/conflation.route",
    post: (req, res) => {
      const { coords } = req.body;
      return getRoute(coords)
        .then(route => res.json({ result: route }))
        .catch(error => res.json({ error: error.message }));
    }
  }
]
