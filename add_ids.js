const fs = require('fs');
let svg = fs.readFileSync('ui/MiddleEarthMap.svg', 'utf8');
svg = svg.replace(/<!-- \d+\. ([\w-]+) -->\r?\n\s*<line/g, (match, p1) => match + ' id="path-' + p1 + '"');
fs.writeFileSync('ui/MiddleEarthMap.svg', svg);
