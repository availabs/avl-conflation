
const { readdirSync } = require("fs");

const path = require("path")

const REGEX = /^[a-zA-Z_.]+[.]route[.]js$/;

module.exports = readdirSync(__dirname)
	.filter(file => REGEX.test(file))
	.reduce((routes, file) => {
    return routes.concat(require(path.join(__dirname, file)));
  }, []);
