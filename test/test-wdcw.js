'use strict';

var assert = require('assert'),
    sinon = require('sinon'),
    jQuery = require('../bower_components/jquery/dist/jquery.js')(require('jsdom').jsdom().parentWindow),
    wdcwFactory = require('../src/main.js'),
    tableau = require('./util/tableau.js'),
    connector = require('./util/connector.js'),
    wdcw;

describe('new-relic-insights-connector:setup', function describesConnectorSetup() {
  var setUpComplete;

  beforeEach(function connectorSetupBeforeEach() {
    setUpComplete = sinon.spy();
    wdcw = wdcwFactory(jQuery, tableau, {});
  });

  it('calls completion callback during interactive phase', function connectorSetupInteractive() {
    // If available, ensure the callback is called during interaction.
    if (wdcw.hasOwnProperty('setup')) {
      wdcw.setup.call(connector, tableau.phaseEnum.interactivePhase, setUpComplete);
      assert(setUpComplete.called);
    }
  });

  it('calls completion callback during auth phase', function connectorSetupAuth() {
    // If available, ensure the callback is called during authentication.
    if (wdcw.hasOwnProperty('setup')) {
      wdcw.setup.call(connector, tableau.phaseEnum.authPhase, setUpComplete);
      assert(setUpComplete.called);
    }
  });

  it('calls completion callback during data gathering phase', function connectorSetupData() {
    // If available, ensure the callback is called during data gathering.
    if (wdcw.hasOwnProperty('setup')) {
      wdcw.setup.call(connector, tableau.phaseEnum.gatherDataPhase, setUpComplete);
      assert(setUpComplete.called);
    }
  });

});

describe('new-relic-insights-connector:columnHeaders', function describesConnectorColumnHeaders() {
  var registerHeaders;

  beforeEach(function connectorColumnHeadersBeforeEach() {
    registerHeaders = sinon.spy();
    // Here's how you might stub or mock various jQuery methods.
    sinon.stub(jQuery, 'ajax');
    sinon.stub(jQuery, 'getJSON');
    wdcw = wdcwFactory(jQuery, {}, {});
  });

  afterEach(function connectorColumnHeadersAfterEach() {
    // Don't forget to restore their original implementations after each test.
    jQuery.ajax.restore();
    jQuery.getJSON.restore();
  });

  // This test is not very meaningful. You should write actual test logic here
  // and/or in new cases below.
  it('should be tested here', function connectorColumnHeadersTestHere() {
    wdcw.columnHeaders.call(connector, registerHeaders);

    assert(registerHeaders.called || jQuery.ajax.called || jQuery.getJSON.called);
    if (registerHeaders.called) {
      assert(typeof registerHeaders.getCall(0).args[0] === 'array');
    }
  });

});

describe('new-relic-insights-connector:tableData', function describesConnectorTableData() {
  var regsterData;

  beforeEach(function connectorTableDataBeforeEach() {
    regsterData = sinon.spy();
    sinon.stub(jQuery, 'ajax');
    sinon.stub(jQuery, 'getJSON');
    wdcw = wdcwFactory(jQuery, {}, {});
  });

  afterEach(function connectorTableDataAfterEach() {
    jQuery.ajax.restore();
    jQuery.getJSON.restore();
  });

  // This test is not very meaningful. You should write actual test logic here
  // and/or in new cases below.
  it('should be tested here', function connectorTableDataTestHere() {
    wdcw.tableData.call(connector, regsterData);

    assert(regsterData.called || jQuery.ajax.called || jQuery.getJSON.called);
    if (regsterData.called) {
      assert(typeof regsterData.getCall(0).args[0] === 'array');
    }
  });

});

describe('new-relic-insights-connector:teardown', function describesConnectorTearDown() {
  var tearDownComplete;

  beforeEach(function connectorTearDownBeforeEach() {
    tearDownComplete = sinon.spy();
    wdcw = wdcwFactory(jQuery, {}, {});
  });

  it('calls teardown completion callback', function connectorTearDown() {
    // If available, ensure the completion callback is always called.
    if (wdcw.hasOwnProperty('teardown')) {
      wdcw.teardown.call(connector, tearDownComplete);
      assert(tearDownComplete.called);
    }
  });

});
