// Define our Web Data Connector
(function() {

    var myConnector = tableau.makeConnector();
    myConnector.getSchema = function(schemaCallback) {
        $.getJSON("../json/schema.json")
            .done(function(scehma_json) {
                schemaCallback(scehma_json.tables, scehma_json.connections);
            })
            .fail(function(jqxhr, textStatus, error) {
                var err = textStatus + ", " + error;
                tableau.log("Failed retrieving schema: " + err);
                tableau.abortWithError(err);
            });
    }

    myConnector.getData = function(table, doneCallback) {

        tableau.log("## Connection Data: " + tableau.connectionData);
        var connectionData = JSON.parse(tableau.connectionData);

        if (needsJoinFiltering(table.tableInfo.id)) {

            if (!table.isJoinFiltered) {
                tableau.abortWithError("The table must be filtered first.");
                return;
            }
        }

        var startTsInMs = table.incrementValue ? parseInt(table.incrementValue) + 1 : connectionData.st_timestamp ? connectionData.st_timestamp : new Date(0).getTime();
        var refreshEndTs = startTsInMs + (connectionData.refresh_mins * 60 * 1000);
        var endTsInMs = connectionData.end_timestamp ? connectionData.end_timestamp : refreshEndTs;

        if (endTsInMs > connectionData.currTs) {
            tableau.abortWithError("Aborting. Specified end time greater than current timestamp.");
            doneCallback();
            return;
        }

        loadTableData(table, connectionData.url, startTsInMs, endTsInMs, doneCallback);

    }

    tableau.registerConnector(myConnector);

})();

function loadTableData(table, url, startTs, endTs, doneCallback) {

    var buildPromises = [];
    
    if (table.tableInfo.id == "builds") {
        var source = new EventSource(url + '/build-export/v1/builds/since/' + startTs);
        source.addEventListener("Build", function(e) {
            var data = JSON.parse(e.data);

            // Close Event Source when currently fetched build timestamp is greater than end timestamp
            if (data["timestamp"] > endTs) {
                source.close();
                tableau.log("Source closed after finishing current timestamp exceeded specified timestamp.");
                Promise.all(buildPromises).then(function(data) {
                    tableau.log("Calling doneCallback after resolving all promises..");
                    doneCallback();
                });
                return;
            }

            buildPromises.push(persistBuildEventsData(url, table, doneCallback, data["buildId"], data["timestamp"]));
            if (buildPromises.length % 50 == 0) {
                tableau.reportProgress("Number of builds loaded: " + buildPromises.length);
            }

        }, false);
    } else {
        var filterVals = table.filterValues;

        if (filterVals.length == 0) {
            tableau.abortWithError("Aborting. Filter values are empty.");
            doneCallback();
            return;
        }

        for (var i in filterVals) {
            buildPromises.push(persistBuildEventsData(url, table, doneCallback, filterVals[i]));
            if (i % 50 == 0) {
                tableau.reportProgress("Number of builds loaded: " + i);
            }
        }

        Promise.all(buildPromises).then(function(data) {
            tableau.log("Calling doneCallback after resolving all promises..");
            doneCallback();
        });
    }

}

function persistBuildEventsData(baseUrl, table, doneCallback, buildId, buildTs) {

    return new Promise(function(resolve, reject) {

        var buildEvts = getBuildEvents(table.tableInfo.id);
        var buildEventPromise = getBuildEventsPromise(baseUrl, table.tableInfo.id, buildId, buildTs, buildEvts);

        Promise.all([buildEventPromise]).then(function(data) {
            table.appendRows(data[0]);
            resolve();
        }, function(error) {
            tableau.log("Error occured waiting for promises. Aborting");
            tableau.abortWithError(error.toString());
            doneCallback();
        });
    });

}

function getBuildEventsPromise(baseUrl, tableId, buildId, bTimestamp, buildEvents) {
    return new Promise(function(resolve, reject) {
        var obj = new XMLHttpRequest();
        var evtDataMap = {};
        obj.open("GET", baseUrl + '/build-export/v1/build/' + buildId + '/events?eventTypes=' + buildEvents.join(","), true);
        obj.onreadystatechange = function() {
            if (obj.readyState == 4 && obj.status == "200") {

                var dataArr = obj.responseText.split("\n\n");
                tableau.log("Number of events: " + dataArr.length + " for buildID: " + buildId);
                
                for (var i in dataArr) {

                    var evt = dataArr[i].split("\n");
                    if (!evt)
                        continue;

                    if (evt[1] && evt[1].indexOf("event: BuildEvent") == -1)
                        continue;

                    if (evt[2]) {
                        evt[2] = evt[2].replace("data: ", "");
                        var data = JSON.parse(evt[2]);
                        var evtType = data.type.eventType;
                        var evtData = data['data'];
                        evtData['timestamp'] = data['timestamp'];
                        if (!evtDataMap.hasOwnProperty(evtType))
                            evtDataMap[evtType] = [evtData];
                        else
                            evtDataMap[evtType].push(evtData);
                    }
                }
                resolve(createTableRecords(tableId, buildId, bTimestamp, evtDataMap));
            }
        }
        obj.send(null);
    });
}

function createTableRecords(tableId, buildId, bTimestamp, buildEventData) {
    var tableData = [];
    var tableEntry = {};
    tableEntry.build_id = buildId;
    if (bTimestamp)
        tableEntry.bts = bTimestamp;
    var taskValMap = {},
        testValMap = {};
    for (evt in buildEventData) {
        var data = buildEventData[evt];
        switch (evt) {
            case "BuildAgent":
                tableEntry.user_name = data[0]["username"];
                break;
            case "ProjectStructure":
                tableEntry.root_project_name = data[0]["rootProjectName"]
                break;
            case "BuildStarted":
                tableEntry.start = new Date(data[0]["timestamp"]);
                break;
            case "BuildFinished":
                tableEntry.finish = new Date(data[0]["timestamp"]);
                tableau.log("## Build Finish: " + tableEntry.finish);
                break;
            case "UserTag":
                tableEntry.tags = data[0]["tag"];
                break;
            case "UserNamedValue":
                tableEntry.key = data[0]["key"];
                tableEntry.value = data[0]["value"];
                break;
            case "TaskStarted":
                for (var i in data) {
                    var id = String(data[i]['id']);
                    var task = taskValMap.hasOwnProperty(id) ? taskValMap[id] : {};
                    task.task_id = id;
                    task.path = data[i]['path'];
                    task.classname = data[i]['className'];
                    task.start = data[i]['timestamp'];
                    taskValMap[id] = task;
                }
                break;
            case "TaskFinished":
                for (var i in data) {
                    var id = String(data[i]['id']);
                    var task = taskValMap.hasOwnProperty(id) ? taskValMap[id] : {};
                    task.finish = data[i]['timestamp'];
                    task.outcome = data[i]['outcome'];
                    taskValMap[id] = task;
                }
                break;
            case "TestStarted":
                for (var i in data) {
                    var id = data[i]['id'];
                    var task = data[i]['task'];
                    var key = id + ":" + task;
                    var test = testValMap.hasOwnProperty(key) ? testValMap[key] : {};
                    test.test_id = id;
                    test.task_id = task;
                    test.name = data[i]['name'];
                    test.classname = data[i]['className'];
                    test.start = data[i]['timestamp'];
                    testValMap[key] = test;
                }
                break;
            case "TestFinished":
                for (var i in data) {
                    var id = data[i]['id'];
                    var task = data[i]['task'];
                    var key = id + ":" + task;
                    var test = testValMap.hasOwnProperty(key) ? testValMap[key] : {};
                    var failed = data[i]['failed'];
                    var skipped = data[i]['skipped'];
                    test.status = skipped ? "skipped" : failed ? "failed" : "success";
                    test.finish = data[i]['timestamp'];
                    testValMap[key] = test;
                }
                break;
        }
    }

    var taskMapKeys = Object.keys(taskValMap);
    var testMapKeys = Object.keys(testValMap);

    if (tableId == "tasks" && taskMapKeys.length == 0 || tableId == "tests" && testMapKeys.length == 0)
        return tableData;

    if (taskMapKeys.length > 0) {
        for (var i in taskValMap) {
            var tableEntry = {};
            tableEntry.build_id = buildId;
            tableEntry.bts = bTimestamp;
            var task = taskValMap[i];
            tableEntry.task_id = task.task_id;
            tableEntry.path = task.path;
            tableEntry.type = task.classname;
            tableEntry.duration_millis = task.finish - task.start;
            tableEntry.outcome = task.outcome;
            tableData.push(tableEntry);
        }
    } else if (testMapKeys.length > 0) {
        for (var i in testValMap) {
            var tableEntry = {};
            tableEntry.build_id = buildId;
            tableEntry.bts = bTimestamp;
            var test = testValMap[i];
            tableEntry.task_id = test.task_id;
            tableEntry.test_id = test.test_id;
            tableEntry.name = test.name;
            tableEntry.class_name = test.classname;
            tableEntry.duration_millis = test.finish - test.start;
            tableEntry.status = test.status;
            tableData.push(tableEntry);
        }
    } else {
        tableData.push(tableEntry);
    }

    return tableData;
}

function getBuildEvents(tableId) {
    switch (tableId) {
        case "builds":
            return ["BuildAgent", "ProjectStructure", "BuildStarted", "BuildFinished", "UserTag"];
        case "tasks":
            return ["TaskStarted", "TaskFinished"];
        case "tests":
            return ["TestStarted", "TestFinished"];
        case "custom_values":
            return ["UserNamedValue"];
    }
}

function needsJoinFiltering(table) {
    var joinFilteredTables = ["tasks", "custom_values", "tests"];
    return joinFilteredTables.indexOf(table) >= 0;
}

//-------------------------------Connector UI---------------------------//

function setupConnector() {
    var url = $("#gradle_url").val();
    var st_timestamp = $("#st_timestamp").val();
    var end_timestamp = $("#end_timestamp").val();
    var refresh_mins = $("#refresh_mins").val();

    if (url) {
        tableau.connectionName = "Gradle WDC Data";
        var connectionData = {
            "url": url,
            "st_timestamp": st_timestamp ? parsePSTTimeInMs(st_timestamp) : st_timestamp,
            "end_timestamp": end_timestamp ? parsePSTTimeInMs(end_timestamp) : end_timestamp,
            "refresh_mins": refresh_mins ? parseInt(refresh_mins) : 15,
            "currTs": new Date().getTime()
        };
        tableau.connectionData = JSON.stringify(connectionData);
        tableau.submit();
    }
}

function parsePSTTimeInMs(timeString) {
    return new Date(timeString).getTime() + new Date().getTimezoneOffset() * 60 * 1000;
}

$(document).ready(function() {
    $("#submitButton").click(function() { // This event fires when a button is clicked
        setupConnector();
    });
    $('#inputForm').submit(function(event) {
        event.preventDefault();
        setupConnector();
    });
});