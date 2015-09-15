var module = module || {},
    window = window || {},
    jQuery = jQuery || {},
    tableau = tableau || {},
    wdcw = window.wdcw || {};

module.exports = function($, tableau, wdcw) {

  /**
   * Run during initialization of the web data connector.
   *
   * @param {string} phase
   *   The initialization phase. This can be one of:
   *   - tableau.phaseEnum.interactivePhase: Indicates when the connector is
   *     being initialized with a user interface suitable for an end-user to
   *     enter connection configuration details.
   *   - tableau.phaseEnum.gatherDataPhase: Indicates when the connector is
   *     being initialized in the background for the sole purpose of collecting
   *     data.
   *   - tableau.phaseEnum.authPhase: Indicates when the connector is being
   *     accessed in a stripped down context for the sole purpose of refreshing
   *     an OAuth authentication token.
   * @param {function} setUpComplete
   *   A callback function that you must call when all setup tasks have been
   *   performed.
   */
  wdcw.setup = function setup(phase, setUpComplete) {
    // Set the incremental extract refresh column header, irrespective of phase.
    this.setIncrementalExtractColumn('timestamp');
    setUpComplete();
  };

  /**
   * Run when the web data connector is being unloaded. Useful if you need
   * custom logic to clean up resources or perform other shutdown tasks.
   *
   * @param {function} tearDownComplete
   *   A callback function that you must call when all shutdown tasks have been
   *   performed.
   */
  /*wdcw.teardown = function teardown(tearDownComplete) {
    // Once shutdown tasks are complete, call this. Particularly useful if your
    // clean-up tasks are asynchronous in nature.
    tearDownComplete();
  };*/

  /**
   * Primary method called when Tableau is asking for the column headers that
   * this web data connector provides. Takes a single callable argument that you
   * should call with the headers you've retrieved.
   *
   * @param {function(Array<{name, type, incrementalRefresh}>)} registerHeaders
   *   A callback function that takes an array of objects as its sole argument.
   *   For example, you might call the callback in the following way:
   *   registerHeaders([
   *     {name: 'Boolean Column', type: 'bool'},
   *     {name: 'Date Column', type: 'date'},
   *     {name: 'DateTime Column', type: 'datetime'},
   *     {name: 'Float Column', type: 'float'},
   *     {name: 'Integer Column', type: 'int'},
   *     {name: 'String Column', type: 'string'}
   *   ]);
   *
   *   Note: to enable support for incremental extract refreshing, add a third
   *   key (incrementalRefresh) to the header object. Candidate columns for
   *   incremental refreshes must be of type datetime or integer. During an
   *   incremental refresh attempt, the most recent value for the given column
   *   will be passed as "lastRecord" to the tableData method. For example:
   *   registerHeaders([
   *     {name: 'DateTime Column', type: 'datetime', incrementalRefresh: true}
   *   ]);
   */
  wdcw.columnHeaders = function columnHeaders(registerHeaders) {
    var connectionData = this.getConnectionData(),
        accountId = connectionData.AccountID,
        nrql = connectionData.NRQLQuery;

    // Make a request to the API using your API token like this:
    $.ajax({
      url: buildApiFrom(nrql, {id: accountId}),
      headers: {
        // Note that the token is available on the "password" property of the
        // global tableau object. The password is encrypted when stored.
        'X-Query-Key': tableau.password
      },
      success: function columnHeadersRetrieved(response) {
        var processedColumns = [],
            event,
            propName;

        // Abort if the response represents an unsupported query.
        if (!isValidInsightsResponse(response)) {
          tableau.abortWithError('NRQL queries with aggregation are unsupported at this time.');
          registerHeaders(processedColumns);
          return;
        }

        event = response.results[0].events[0];

        // If necessary, process the response from the API into the expected
        // format (highlighted below):
        for (propName in event) {
          if (event.hasOwnProperty(propName)) {
            processedColumns.push({
              name: propName,
              type: wdcw.parseColumnType(propName, event[propName])
            });
          }
        }

        // Once data is retrieved and processed, call registerHeaders().
        registerHeaders(processedColumns);
      }
    });
  };


  /**
   * Primary method called when Tableau is asking for your web data connector's
   * data. Takes a callable argument that you should call with all of the
   * data you've retrieved. You may optionally pass a token as a second argument
   * to support paged/chunked data retrieval.
   *
   * @param {function(Array<{object}>, {string})} registerData
   *   A callback function that takes an array of objects as its sole argument.
   *   Each object should be a simple key/value map of column name to column
   *   value. For example, you might call the callback in the following way:
   *   registerData([
   *     {'String Column': 'String Column Value', 'Integer Column': 123}
   *   ]});
   *
   *   It's possible that the API you're interacting with supports some mechanism
   *   for paging or filtering. To simplify the process of making several paged
   *   calls to your API, you may optionally pass a second argument in your call
   *   to the registerData callback. This argument should be a string token that
   *   represents the last record you retrieved.
   *
   *   If provided, your implementation of the tableData method will be called
   *   again, this time with the token you provide here. Once all data has been
   *   retrieved, pass null, false, 0, or an empty string.
   *
   * @param {string} lastRecord
   *   Optional. If you indicate in the call to registerData that more data is
   *   available (by passing a token representing the last record retrieved),
   *   then the lastRecord argument will be populated with the token that you
   *   provided. Use this to update/modify the API call you make to handle
   *   pagination or filtering.
   *
   *   If you indicated a column in wdcw.columnHeaders suitable for use during
   *   an incremental extract refresh, the last value of the given column will
   *   be passed as the value of lastRecord when an incremental refresh is
   *   triggered.
   */
  wdcw.tableData = function tableData(registerData, lastRecord) {
    var connectionData = this.getConnectionData(),
        accountId = connectionData.AccountID,
        nrql = connectionData.NRQLQuery;

    // Do the same to retrieve your actual data.
    $.ajax({
      url: buildApiFrom(nrql, {last: lastRecord, id: accountId}),
      headers: {
        'X-Query-Key': tableau.password
      },
      success: function dataRetrieved(response) {
        var processedData = [],
            // Determine if more data is available via paging.
            moreData = false, // @todo Implement paging.
            events;

        // Abort if the response represents an unsupported query.
        if (!isValidInsightsResponse(response)) {
          tableau.abortWithError('NRQL queries with aggregation are unsupported at this time.');
          registerData(processedData);
          return;
        }

        events = response.results[0].events;

        // You may need to perform processing to shape the data into an array of
        // objects where each object is a map of column names to values.
        events.forEach(function shapeData(event) {
          var date;

          // Convert the timestamp into a date parseable by Tableau.
          if (event.timestamp) {
            date = new Date(parseInt(event.timestamp, 10));
            event.timestamp = date.toISOString();
          }

          processedData.push(event);
        });

        // Once you've retrieved your data and shaped it into the form expected,
        // call the registerData function. If more data can be retrieved, then
        // supply a token to inform further paged requests.
        // @see buildApiFrom()
        if (moreData) {
          registerData(processedData, response.meta.page);
        }
        // Otherwise, just register the response data with the callback.
        else {
          registerData(processedData);
        }
      }
    });
  };

  /**
   * Given a column from Insights and a sample value, attempts to determine the
   * column's type (according to Tableau).
   *
   * @param {string} column
   *   The name of the Insights event property.
   * @param {string} value
   *   A sample value for the event property.
   * @returns {string}
   *   One of datetime, float, int, or string.
   */
  wdcw.parseColumnType = function parseColumnType(column, value) {
    var isFloat = /^[+\-]?[0-9]*\.[0-9]+$/,
        isInt = /^[+\-]?[0-9]+$/;

    // Check if this is the timestamp column.
    if (column === 'timestamp') {
      return 'datetime';
    }
    // Check if the value is a float.
    else if (isFloat.test(value)) {
      return 'float';
    }
    // Check if the value is an integer.
    else if (isInt.test(value)) {
      return 'int';
    }
    else {
      return 'string';
    }
  };

  // You can write private methods for use above like this:

  /**
   * Helper function to build an API endpoint that uses our proxy.
   *
   * @param {string} nrql
   *   NRQL query to use when hitting Insights.
   *
   * @param {object} opts
   *   Options to inform query parameters and paging.
   */
  function buildApiFrom(nrql, opts) {
    var path = '/proxy?nrql=' + encodeURIComponent(nrql);
    opts = opts || {};

    // If opts.last was passed, build the URL so the next page is returned.
    if (opts.last) {
      path += '&page=' + opts.last + 1;
    }

    return path + '&account=' + opts.id;
  }

  function isValidInsightsResponse(response) {
    return response.results && response.results[0] && response.results[0].events;
  }

  return wdcw;
};

wdcw = module.exports(jQuery, tableau, wdcw);
