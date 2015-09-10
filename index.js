// Generated on 2015-08-24 using generator-web-data-connector 0.0.0

var express = require('express'),
    request = require('request'),
    app = express(),
    port = process.env.PORT || 9001;

// Serve files as if this were a static file server.
app.use(express.static('./'));

// Proxy the index.html file.
app.get('/', function (req, res) {
  res.sendFile('./index.html');
});

// Create a proxy endpoint.
app.get('/proxy', function (req, res) {
  // Note that the "buildApiFrom(path)" helper in main.js sends the API endpoint
  // as a query parameter to our proxy. We read that in here and build the real
  // endpoint we want to hit.
  var nrql = encodeURIComponent(req.query.nrql),
      accountId = req.query.account,
      options = {
        url: 'https://insights-api.newrelic.com/v1/accounts/' + accountId + '/query?nrql=' + nrql,
        headers: {
          Accept: 'application/json',
          // Client-side code sends us exactly what we need for authentication.
          'X-Query-Key': req.header('X-Query-Key'),
          'User-Agent': 'new-relic-insights/0.0.0'
        }
      };


  // Make an HTTP request using the above specified options.
  console.log('Attempting to proxy request to ' + options.url);
  request(options, function (error, response, body) {
    var header;

    if (!error && response.statusCode == 200) {
      // Proxy all response headers.
      for (header in response.headers) {
        if (response.headers.hasOwnProperty(header)) {
          res.set(header, response.headers[header]);
        }
      }

      // Send the response body.
      res.send(body);
    }
    else {
      error = error || response.statusMessage || response.statusCode;
      console.log('Error fulfilling request: "' + error.toString() + '"');
      res.sendStatus(response.statusCode);
    }
  });
});

var server = app.listen(port, function () {
  var port = server.address().port;
  console.log('Express server listening on port ' + port);
});

module.exports = app;
