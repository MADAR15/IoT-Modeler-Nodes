/**
 * Copyright 2013,2015 IBM Corp.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 **/

module.exports = function(RED) {
    "use strict";
    var cassandra = require('cassandra-driver');
	var CassandraClient = new cassandra.Client({contactPoints: ['127.0.0.1'], keyspace: 'test'});

    function CassandraNode(n) {
        RED.nodes.createNode(this,n);
        this.hostname = n.hostname;
        this.port = n.port;
        this.ks = n.ks;
        this.name = n.name;

    }
	
	function ColumnFamily(n) {
        RED.nodes.createNode(this,n);
        this.cfname = n.cfname;
        this.columns = n.columns;
        this.partitionkeys = n.partitionkeys;
        this.clusteringkeys = n.clusteringkeys;

    }

    RED.nodes.registerType("cassandra",CassandraNode,{
        credentials: {
            user: {type:"text"},
            password: {type: "password"}
        }
    });
	
	RED.nodes.registerType("columnfamily",ColumnFamily,{
		
	});

    function ensureValidSelectorObject(selector) {
        if (selector != null && (typeof selector != 'object' || Buffer.isBuffer(selector))) {
            return {};
        }
        return selector;
    }


    function CassandraOutNode(n) {
        RED.nodes.createNode(this,n);
        this.cassandra = n.cassandra;
		this.columnfamily=n.columnfamily;
        this.operation = n.operation;
        this.cassandraConfig = RED.nodes.getNode(this.cassandra);
		this.columnfamilyConfig = RED.nodes.getNode(this.columnfamily);

        if (this.cassandraConfig) {
            var node = this;
			CassandraClient.connect(function(err) {
                if (err) {
                    node.error("connection problem "+err);
                } else {
                    var cf;
                    if (node.columnfamilyConfig.cfname) {
                        cf = node.columnfamilyConfig.cfname;
                    }
					var columns;
					if (node.columnfamilyConfig.columns) {
                        columns = node.columnfamilyConfig.columns;
                    }
                    node.on("input",function(msg) {
						//console.log(msg);
						if (!node.columnfamilyConfig.cfname) {
                            if (msg.columnfamilyConfig) {
                                cf = msg.columnfamilyConfig;
                            } else {
                                node.error("No columnfamily defined",msg);
                                return;
                            }
                        }
						
						if (!node.columnfamilyConfig.columns) {
                            if (msg.columns) {
                                columns = msg.columns;
                            } else {
                                node.error("No columns defined",msg);
                                return;
                            }
                        }
						
                        if (node.operation === "insert") {
							//var params = ['mick-jaggeezfdzfr', 'Sir Mick Jzefzefagger', 'mick@rollinzfzefzefgstones.com', "'"+new Date(2015, 6, 26)+"'"];
							var params = msg.payload.row.split(",");
							CassandraClient.execute("INSERT INTO "+node.columnfamilyConfig.cfname+" ("+columns+") VALUES (?, ?, ?,?)", params, function (err) {
                                    if (err) {
                                        node.error(err,msg);
                                    }
                                });
                            /* } */
                        } else if (node.operation === "delete") {
                            var key = msg.payload.key;
							CassandraClient.execute("delete from "+node.columnfamilyConfig.cfname+" where key = "+key, function (err) {
                                    if (err) {
                                        node.error(err,msg);
                                    }
                                });
                        }
                    });
                }
            });
        } else {
            this.error("missing cassandra configuration");
        }

        this.on("close", function() {
            if (this.clientDb) {
                this.clientDb.close();
            }
        });
    }
    RED.nodes.registerType("cassandra out",CassandraOutNode);

    function CassandraInNode(n) {
        RED.nodes.createNode(this,n);
        this.cassandra = n.cassandra;
		this.columnfamily=n.columnfamily;
        this.operation = n.operation;
        this.cassandraConfig = RED.nodes.getNode(this.cassandra);
		this.columnfamilyConfig = RED.nodes.getNode(this.columnfamily);

        if (this.cassandraConfig) {
            var node = this;
			CassandraClient.connect(function(err) {
                if (err) {
                    node.error("connection problem "+err);
                } else {
                    var cf;
                    if (node.columnfamilyConfig.cfname) {
                        cf = node.columnfamilyConfig.cfname;
                    }
					var columns;
					if (node.columnfamilyConfig.columns) {
                        columns = node.columnfamilyConfig.columns;
                    }
                    node.on("input",function(msg) {
						//console.log(msg);
						if (!node.columnfamilyConfig.cfname) {
                            if (msg.columnfamilyConfig) {
                                cf = msg.columnfamilyConfig;
                            } else {
                                node.error("No columnfamily defined",msg);
                                return;
                            }
                        }
						
						if (!node.columnfamilyConfig.columns) {
                            if (msg.columns) {
                                columns = msg.columns;
                            } else {
                                node.error("No columns defined",msg);
                                return;
                            }
                        }
                        var selector;
                        if (node.operation === "find") {
                            msg.projection = msg.projection || {};
                            selector = ensureValidSelectorObject(msg.payload);
                            var limit = msg.limit;
                            if (typeof limit === "string" && !isNaN(limit)) {
                                limit = Number(limit);
                            }
                            var skip = msg.skip;
                            if (typeof skip === "string" && !isNaN(skip)) {
                                skip = Number(skip);
                            }

                            coll.find(selector,msg.projection).sort(msg.sort).limit(limit).skip(skip).toArray(function(err, items) {
                                if (err) {
                                    node.error(err);
                                } else {
                                    msg.payload = items;
                                    delete msg.projection;
                                    delete msg.sort;
                                    delete msg.limit;
                                    delete msg.skip;
                                    node.send(msg);
                                }
                            });
                        } else if (node.operation === "count") {
                            selector = ensureValidSelectorObject(msg.payload);
                            coll.count(selector, function(err, count) {
                                if (err) {
                                    node.error(err);
                                } else {
                                    msg.payload = count;
                                    node.send(msg);
                                }
                            });
                        } else if (node.operation === "aggregate") {
                            msg.payload = (Array.isArray(msg.payload)) ? msg.payload : [];
                            coll.aggregate(msg.payload, function(err, result) {
                                if (err) {
                                    node.error(err);
                                } else {
                                    msg.payload = result;
                                    node.send(msg);
                                }
                            });
                        }
                    });
                }
            });
        } else {
            this.error("missing cassandra configuration");
        }

        this.on("close", function() {
            if (this.clientDb) {
                this.clientDb.close();
            }
        });
    }
    RED.nodes.registerType("cassandra in",CassandraInNode);
}
