(function() {
    // Create the connector object
    var myConnector = tableau.makeConnector();
    myConnector.init = function(initCallback) {
        initCallback();
    }

    // Define the schema
    myConnector.getColumnHeaders = function(schemaCallback) {
        var conObj = JSON.parse(tableau.connectionData),
            included_tables = [],
            included_connections = [];
        var timeserie_cols = [{
                id: "id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "value",
                alias: "value",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "timestamp",
                alias: "timestamp",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "locationcode",
                alias: "location code",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "observationcode",
                alias: "observatie type",
                dataType: tableau.dataTypeEnum.string
            },
            {
                id: "observationunit",
                alias: "unit",
                dataType: tableau.dataTypeEnum.string
            }
        ];
        var timeserie_table = {
            id: "timeserie",
            alias: "timeseries",
            columns: timeserie_cols
        };
        included_tables.push(timeserie_table);

        if (conObj.objectType.includes("pumpstations")) {
            var pumpstation_cols = [{
                id: "id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "locationcode",
                alias: "location code",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "capacity",
                alias: "capacity",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "start_level",
                alias: "start_level",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "stop_level",
                alias: "stop_level",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "longitude",
                alias: "longitude",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "latitude",
                alias: "latitude",
                dataType: tableau.dataTypeEnum.float
            }];


            var pumpstation_table = {
                id: "pumpstation",
                alias: "pumpstations",
                columns: pumpstation_cols
            };

            var pumpstationTimeseries = {
                "alias": "Pumpstations with timeseries",
                "tables": [{
                    "id": "pumpstation",
                    "alias": "pumpstations"
                }, {
                    "id": "timeserie",
                    "alias": "timeseries"
                }],
                "joins": [{
                    "left": {
                        "tableAlias": "pumpstations",
                        "columnId": "locationcode"
                    },
                    "right": {
                        "tableAlias": "timeseries",
                        "columnId": "locationcode"
                    },
                    "joinType": "left"
                }]
            };

            included_tables.push(pumpstation_table);
            included_connections.push(pumpstationTimeseries);
        };


        if (conObj.objectType.includes("measuringstations")) {
            var measuringstation_cols = [{
                id: "id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "locationcode",
                alias: "location code",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "longitude",
                alias: "longitude",
                dataType: tableau.dataTypeEnum.float
            }, {
                id: "latitude",
                alias: "latitude",
                dataType: tableau.dataTypeEnum.float
            }];

            var measuringstation_table = {
                id: "measuringstation",
                alias: "measuringstations",
                columns: measuringstation_cols
            };

            var measuringstationTimeseries = {
                "alias": "Measuringstations with timeseries",
                "tables": [{
                    "id": "measuringstation",
                    "alias": "measuringstations"
                }, {
                    "id": "timeserie",
                    "alias": "timeseries"
                }],
                "joins": [{
                    "left": {
                        "tableAlias": "measuringstations",
                        "columnId": "locationcode"
                    },
                    "right": {
                        "tableAlias": "timeseries",
                        "columnId": "locationcode"
                    },
                    "joinType": "left"
                }]
            };

            included_tables.push(measuringstation_table);
            included_connections.push(measuringstationTimeseries);
        };

        if (conObj.objectType.includes("groundwaterstations")) {
            var groundwaterstation_cols = [{
                id: "id",
                alias: "id",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "locationcode",
                alias: "location code",
                dataType: tableau.dataTypeEnum.string
            }, {
                id: "geometry",
                alias: "geometry",
                dataType: tableau.dataTypeEnum.geometry
            }];

            var groundwaterstation_table = {
                id: "groundwaterstation",
                alias: "groundwaterstations",
                columns: groundwaterstation_cols
            };

            var groundwaterstationTimeseries = {
                "alias": "Groundwaterstations with timeseries",
                "tables": [{
                    "id": "groundwaterstation",
                    "alias": "groundwaterstations"
                }, {
                    "id": "timeserie",
                    "alias": "timeseries"
                }],
                "joins": [{
                    "left": {
                        "tableAlias": "groundwaterstations",
                        "columnId": "locationcode"
                    },
                    "right": {
                        "tableAlias": "timeseries",
                        "columnId": "locationcode"
                    },
                    "joinType": "left"
                }]
            };

            included_tables.push(groundwaterstation_table);
            included_connections.push(groundwaterstationTimeseries);
        };

        var assets_combined_cols = [{
            id: "id",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "locationcode",
            alias: "location code",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "asset_type",
            alias: "asset_type",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "geometry",
            alias: "geometry",
            dataType: tableau.dataTypeEnum.geometry
        }];

        var assets_combined_table = {
            id: "combined_assets",
            alias: "combined_assets",
            columns: assets_combined_cols
        };

        var combinedAssetsTimeseries = {
            "alias": "Combined assets with timeseries",
            "tables": [{
                "id": "combined_assets",
                "alias": "combined_assets"
            }, {
                "id": "timeserie",
                "alias": "timeseries"
            }],
            "joins": [{
                "left": {
                    "tableAlias": "combined_assets",
                    "columnId": "locationcode"
                },
                "right": {
                    "tableAlias": "timeseries",
                    "columnId": "locationcode"
                },
                "joinType": "left"
            }]
        };

        included_tables.push(assets_combined_table);
        included_connections.push(combinedAssetsTimeseries)

        var region_cols = [{
            id: "name",
            alias: "name",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "code",
            alias: "code",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "type",
            alias: "type",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "area",
            alias: "area",
            dataType: tableau.dataTypeEnum.string
        }, {
            id: "geometry",
            alias: "geometry",
            dataType: tableau.dataTypeEnum.geometry
        }];

        var region_table = {
            id: "regions",
            alias: "regions",
            columns: region_cols
        };

        var combinedregionsassets = {
            "alias": "Combined assets with regions",
            "tables": [{
                "id": "combined_assets",
                "alias": "combined_assets"
            }, {
                "id": "regions",
                "alias": "regions"
            }, {
                "id": "timeserie",
                "alias": "timeseries"
            }],
            "joins": [{
                "left": {
                    "tableAlias": "combined_assets",
                    "columnId": "geometry"
                },
                "right": {
                    "tableAlias": "regions",
                    "columnId": "geometry"
                },
                "joinType": "left"
            }, {
                "left": {
                    "tableAlias": "combined_assets",
                    "columnId": "locationcode"
                },
                "right": {
                    "tableAlias": "timeseries",
                    "columnId": "locationcode"
                },
                "joinType": "left"
            }]
        };

        included_tables.push(region_table);
        included_connections.push(combinedregionsassets);

        schemaCallback(included_tables, included_connections);
    };

    // Download the data

    var featureCol2PolygonArr = function(geojson, callback) {
        var features = geojson.features;
        var ret = [];
        for (var i = 0; i < features.length; i++) {
            var obj = features[i].properties;
            obj.geometry = features[i].geometry;
            ret.push(obj);
        }
        callback(ret);
    }

    var featureCol2PointArr = function(geojson, asset_type, callback) {
        var features = geojson.results;
        var ret = [];
        for (var i = 0; i < features.length; i++) {
            var obj = {};
            obj.id = features[i].id;
            obj.locationcode = features[i].code;
            obj.asset_type = asset_type;
            obj.geometry = features[i].geometry;
            obj.name = features[i].name;
            ret.push(obj);
        }
        callback(ret);
    }

    myConnector.getTableData = function(table, doneCallback) {
        var conObj = JSON.parse(tableau.connectionData),
            dateString = "start=" + conObj.startDate + "Z&end=" + conObj.endDate + "Z";

        $.ajaxSetup({
            headers: {
                'Authorization': "Basic " + btoa(conObj.username + ":" + conObj.pass)
            }
        })

        if (table.tableInfo.id == "timeserie") {
            for (var h = 0; h < conObj.objectType.length; h++) {
                if (!(conObj.bbox === null)) {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/" + conObj.objectType[h] + "/?organisation__name=" + conObj.organisationName + "&in_bbox=" + conObj.bbox + "&page_size=50";
                } else {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/" + conObj.objectType[h] + "/?organisation__name=" + conObj.organisationName + "&page_size=50";
                }
                $.ajax({
                    url: apiCallLocation,
                    dataType: 'json',
                    async: false,
                    success: function(resp) {
                        for (var i = 0; i < resp.results.length; i++) {
                            var locationCode = resp.results[i].code;
                            if (!(conObj.agg === "value")) {
                                var apiCall = "https://demo.lizard.net/api/v3/timeseries/?location__code=" + locationCode + "&" + dateString + "&fields=" + conObj.agg + "&window=" + conObj.lizWindow;
                            } else {
                                var apiCall = "https://demo.lizard.net/api/v3/timeseries/?location__code=" + locationCode + "&" + dateString + "&fields=" + conObj.agg;
                            };
                            $.ajax({
                                url: apiCall,
                                dataType: 'json',
                                async: false,
                                success: function(resp_ts) {
                                    var timeserie_table = [];
                                    for (var j = 0; j < resp_ts.results.length; j++) {
                                        for (var k = 0; k < resp_ts.results[j].events.length; k++) {
                                            timeserie_table.push({
                                                "id": resp_ts.results[j].id,
                                                "value": resp_ts.results[j].events[k][conObj.agg],
                                                "timestamp": new Date(resp_ts.results[j].events[k].timestamp),
                                                "locationcode": resp_ts.results[j].location.code,
                                                "observationcode": resp_ts.results[j].observation_type.code,
                                                "observationunit": resp_ts.results[j].observation_type.unit
                                            });
                                        }
                                    }
                                    table.appendRows(timeserie_table);
                                }
                            });
                        }
                    }
                })
            }
            doneCallback();
        };

        if (conObj.objectType.includes("pumpstations")) {
            if (table.tableInfo.id == "pumpstation") {
                if (!(conObj.bbox === null)) {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/pumpstations/?organisation__name=" + conObj.organisationName + "&in_bbox=" + conObj.bbox + "&page_size=50";
                } else {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/pumpstations/?organisation__name=" + conObj.organisationName + "&page_size=50";
                }
                $.ajax({
                    url: apiCallLocation,
                    dataType: 'json',
                    async: false,
                    success: function(resp) {
                        for (var i = 0; i < resp.results.length; i++) {
                            var location_table = [];
                            location_table.push({
                                "id": resp.results[i].id,
                                "locationcode": resp.results[i].code,
                                "longitude": resp.results[i].geometry.coordinates[0],
                                "latitude": resp.results[i].geometry.coordinates[1],
                                "capacity": resp.results[i].capacity,
                                "start_level": resp.results[i].start_level,
                                "stop_level": resp.results[i].stop_level
                            });
                            table.appendRows(location_table);
                        }
                        doneCallback();
                    }
                })
            };
        }

        if (conObj.objectType.includes("measuringstations")) {
            if (table.tableInfo.id == "measuringstation") {
                if (!(conObj.bbox === null)) {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/measuringstations/?organisation__name=" + conObj.organisationName + "&in_bbox=" + conObj.bbox + "&page_size=50";
                } else {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/measuringstations/?organisation__name=" + conObj.organisationName + "&page_size=50";
                }
                $.ajax({
                    url: apiCallLocation,
                    dataType: 'json',
                    async: false,
                    success: function(resp) {
                        for (var i = 0; i < resp.results.length; i++) {
                            var location_table = [];
                            location_table.push({
                                "id": resp.results[i].id,
                                "locationcode": resp.results[i].code,
                                "longitude": resp.results[i].geometry.coordinates[0],
                                "latitude": resp.results[i].geometry.coordinates[1]
                            });
                            table.appendRows(location_table);
                        }
                        doneCallback();
                    }
                })
            };
        }

        if (table.tableInfo.id == "groundwaterstation") {
            if (!(conObj.bbox === null)) {
                var apiCallLocation = "https://demo.lizard.net/api/v3/groundwaterstations/?organisation__name=" + conObj.organisationName + "&in_bbox=" + conObj.bbox + "&page_size=50";
            } else {
                var apiCallLocation = "https://demo.lizard.net/api/v3/groundwaterstations/?organisation__name=" + conObj.organisationName + "&page_size=50";
            }
            var data = "{ \"id\": \"groundwaterstation\"}";
            var settings = {
                "url": apiCallLocation,
                "method": "GET",
                "headers": {
                    "Content-Type": "application/json"
                },
                "processData": false,
                "data": data
            }
            $.ajax(settings).done(function(resp) {
                tableau.reportProgress("Parsing data");
                featureCol2PointArr(resp, "groundwaterstation", function(data) {
                    tableau.reportProgress("Returning data to Tableau");
                    table.appendRows(data);
                    doneCallback();
                });
            })
        };

        if (table.tableInfo.id == "regions") {
            var apiCall ="https://demo.lizard.net/api/v3/regions/?type=NEIGHBOURHOOD&geom_intersects=Polygon%20((4.9876086521400147%2052.03842860216160915,%205.17069184275882776%2052.03599379905544708,%205.16784119792831831%2052.13577685843272036,%204.98853735209345128%2052.13312210238419908,%204.9876086521400147%2052.03842860216160915))&page_size=200";
            var data = "{ \"id\": \"regions\"}";
            var settings = {
                "url": apiCall,
                "method": "GET",
                "headers": {
                    "Content-Type": "application/json"
                },
                "processData": false,
                "data": data
            }
            $.ajax(settings).done(function(resp) {
                tableau.reportProgress("Parsing data");
                featureCol2PolygonArr(resp.results, function(data) {
                    tableau.reportProgress("Returning data to Tableau");
                    table.appendRows(data);
                    doneCallback();
                });
            });
        }

        if (table.tableInfo.id == "combined_assets") {
            for (var h = 0; h < conObj.objectType.length; h++) {
                if (!(conObj.bbox === null)) {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/" + conObj.objectType[h] + "/?organisation__name=" + conObj.organisationName + "&in_bbox=" + conObj.bbox + "&page_size=50";
                } else {
                    var apiCallLocation = "https://demo.lizard.net/api/v3/" + conObj.objectType[h] + "/?organisation__name=" + conObj.organisationName + "&page_size=50";
                }
                var data = "{ \"id\": \"combined_assets\"}";
                $.ajax({
                    url: apiCallLocation,
                    dataType: 'json',
                    async: false,
                    "data": data,
                    success: function(resp) {
                        tableau.reportProgress("Parsing data");
                        featureCol2PointArr(resp, conObj.objectType[h], function(data) {
                            tableau.reportProgress("Returning data to Tableau");
                            table.appendRows(data);
                        });
                    }
                });
            }
            doneCallback();
        }
    };
    tableau.registerConnector(myConnector);

    // Create event listeners for when the user submits the form
    $(document).ready(function() {
        $("#submitButton").click(function() {
            var conObj = {
                startDate: $('#start-date-one').val().trim(),
                endDate: $('#end-date-one').val().trim(),
                organisationName: $('#organisation-name').val().trim(),
                bbox: $('#bbox').val().trim(),
                objectType: $('#location_type').val() || [' '],
                lizWindow: $('#liz-window').val(),
                agg: $('#aggregation').val(),
                username: $('#username').val().trim(),
                pass: $('#password').val().trim(),
            };

            // Simple date validation: Call the getDate function on the date object created
            function isValidDate(dateStr) {
                var d = new Date(dateStr);
                return !isNaN(d.getDate());
            }

            if (isValidDate(conObj.startDate) && isValidDate(conObj.endDate)) {
                tableau.connectionData = JSON.stringify(conObj); // Use this variable to pass data to your getSchema and getData functions
                tableau.connectionName = "Lizard timeseries"; // This will be the data source name in Tableau
                tableau.submit(); // This sends the connector object to Tableau
            } else {
                $('#errorMsg').html("Enter valid dates. For example, 2016-05-08.");
            }
        });
    });
})();