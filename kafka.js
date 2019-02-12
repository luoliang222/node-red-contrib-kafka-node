/**
 * Created by fwang1 on 3/25/15.
 */
module.exports = function(RED) {
    "use strict";
    var isUtf8 = require('./is-utf8');
    /*
     *   Kafka Producer
     *   Parameters:
     - topics
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaOutNode(config) {
        RED.nodes.createNode(this,config);
        var topic = config.topic;
        var clusterZookeeper = config.zkquorum;
        var debug = (config.debug == true);
        var node = this;
        var kafka = require('kafka-node');
        var HighLevelProducer = kafka.HighLevelProducer;
        var topics = config.topics;
        var clientOption = {
            kafkaHost: clusterZookeeper,
        }

        if (debug){
            node.log('kafkaOutNode new Client: ' + JSON.stringify(clientOption));
        }
        var client = new kafka.KafkaClient(clientOption);

        try {
            // 处理连接状态
            var timerHandle = setInterval(function(){
                var broker = client.brokerForLeader();
                var ready = broker ? broker.isReady() : false;

                if(debug)
                    node.log('ready = ' + ready + '  connecting = ' + client.connecting);
                if (ready)
                    node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
                else if (client.connecting)
                    node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
                else
                    node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
            }, 1500);

            this.on("close", function(msg) {
                // 清除定时器
                clearInterval(timerHandle);
            });

            this.on("input", function(msg) {
                var publishTo = function(msg){
                    // 如果不是Buffer，将消息转换为字符串
                    if (msg.payload === null || msg.payload === undefined) {
                        msg.payload = "";
                    } else if (!Buffer.isBuffer(msg.payload)) {
                        if (typeof msg.payload === "object") {
                            msg.payload = JSON.stringify(msg.payload);
                        } else if (typeof msg.payload !== "string") {
                            msg.payload = "" + msg.payload;
                        }
                    }

                    var payloads = [];

                    // check if multiple topics
                    if (topics.indexOf(",") > -1){
                        var topicArry = topics.split(',');

                        for (i = 0; i < topicArry.length; i++) {
                            payloads.push({topic: topicArry[i], messages: msg.payload});
                        }
                    }
                    else {
                        payloads = [{topic: topics, messages: msg.payload}];
                    }

                    producer.send(payloads, function(err, data){
                        if (err){
                            node.error(err);
                        }
                        else{
                            if (debug)
                                node.log("kafka out debug ===> Sended: " + msg.payload);
                        }
                    });
                }

                if ( msg.hasOwnProperty("payload")) {
                    if (msg.hasOwnProperty("topic") && (typeof msg.topic === "string") && (msg.topic !== "")) { // topic must exist
                        publishTo(msg);  // send the message
                    }
                    else { node.warn(RED._("mqtt.errors.invalid-topic")); }
                }
            });
        }
        catch(e) {
            node.error(e);
        }

        if (debug){
            node.log('kafkaOutNode new HighLevelProducer ...');
        }
        var producer = new HighLevelProducer(client);
        this.status({fill:"green",shape:"dot",text:"connected to "+clusterZookeeper});
    }

    RED.nodes.registerType("kafka out", kafkaOutNode);


    /*
     *   Kafka Consumer
     *   Parameters:
     - topics
     - groupId
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaInNode(config) {
        console.log(' LL ++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++++');
        RED.nodes.createNode(this,config);

        var node = this;

        var kafka = require('kafka-node');
        var topics = String(config.topics);
        var clusterZookeeper = config.zkquorum;
        var groupId = config.groupId;
        var debug = (config.debug == true);

        var zkOptions = {
            kafkaHost: clusterZookeeper,
        };

        if(config.sessionTimeout != '')
        {
            try {
                zkOptions.requestTimeout = parseInt(config.sessionTimeout);
            }
            catch(e){
                node.error(e);
            }
        }

        if (debug){
            // node.log('kafkaInNode config: ' + JSON.stringify(config));
            node.log('kafkaInNode new Client: ' + JSON.stringify(zkOptions));
        }
        var client = new kafka.KafkaClient(zkOptions);
        var topicJSONArry = [];

        // check if multiple topics
        if (topics.indexOf(",") > -1){
            var topicArry = topics.split(',');
            if (debug) {
                console.log(topicArry)
                console.log(topicArry.length);
            }

            for (i = 0; i < topicArry.length; i++) {
                if (debug) {
                    console.log(topicArry[i]);
                }
                topicJSONArry.push({topic: topicArry[i]});
            }
            topics = topicJSONArry;
        }
        else {
            topics = [{topic:topics}];
        }

        var options = {
            groupId: groupId,
            autoCommit: config.autoCommit,
            autoCommitMsgCount: 10
        };

        if(config.fetchMaxBytes != '')
        {
            try {
                options.fetchMaxBytes = parseInt(config.fetchMaxBytes);
            }
            catch(e){
                node.error(e);
            }
        }

        try {
            //
            // 每次只向下游发送一个消息
            // 如果反馈消息发送成功，则保存当前偏移，并继续消费
            // 如果反馈消息发送失败，则重新发送当前消息
            //
            var waitMsgList = [];   // 待处理的消息队列
            var currMsg;            // 正在处理的消息
            var consumer = new kafka.Consumer(client, topics, options);
            consumer.resume();  // 有可能指定的group已暂停消费，默认执行唤醒

            var isPaused;
            this.log("kafkaInNode new Consumer");
            if (debug){
                this.log("topics: " + JSON.stringify(topics));
                this.log("options: " + JSON.stringify(options));
            }

            // 处理连接状态
            var timerHandle = setInterval(function(){
                var broker = client.brokerForLeader();
                var ready = broker ? broker.isReady() : false;

                if(debug)
                    node.log('ready = ' + ready + '  connecting = ' + client.connecting);
                if (ready)
                    node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
                else if (client.connecting)
                    node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
                else
                    node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
            }, 1500);
            
            this.on("close", function(msg) {
                // 清除定时器
                clearInterval(timerHandle);
            });

//            node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
//            node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
//            node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});

//            this.status({fill:"gray", shape:"dot", text:"connected to "+ clusterZookeeper});
//            this.status({fill:"green", shape:"dot", text:"connected to "+ clusterZookeeper});

            var sendNext = function(){
                if (currMsg)
                    return;     // 当前消息未处理完，暂不能发送
                if (waitMsgList.length > 0){
                    currMsg = waitMsgList[0];
                    waitMsgList.splice(0, 1);
                    node.send(currMsg);
                }
            }

            var onNewMsg = function(msg){
                if (config.autoCommit)
                    node.send(msg);
                else {
                    waitMsgList.push(msg);
                    sendNext(); // 发送下一条消息

                    // 如果等待队列超过最大数量，则暂停消费
                    if (waitMsgList.length > 10 && !isPaused){
                        consumer.pause();
                        isPaused = true;
                        if (debug)
                           node.log('kafka in debug ===> paused');
                    }
                }
            }

            // retry 如果为true则重新发送，否则消费下一条消息
            var kafkaCommit = function(retry){
                if (!currMsg){
                    node.error('kafka in no currMsg');
                    return;     // 没有当前消息，提交个啥？
                }

                if (retry){
                    if (debug)
                        node.log('kafka in debug ===> on kafka retry msg! ' + currMsg);
                    node.send(currMsg);     // 重新发送
                }
                else
                {
                    if (debug)
                        node.log('kafka in debug ===> on call commit, waiting =' + waitMsgList.length);

                    currMsg = null;     // 清除当前消息
                    if (waitMsgList.length == 0){
                        // 如果全部消息都已处理完毕，则提交offset，并恢复消费
                        consumer.commit(function(err, data) {
                            if (err){
                                node.log('kafka in commit err: ' + err.toString());
                            }
                            // 提交成功，恢复消费
                            if (isPaused){
                                consumer.resume();
                                isPaused = false;
                                if (debug)
                                    node.log('kafka in debug ===> resumed');
                            }
                            if (debug)
                                node.log('kafka in debug ===> on kafka commit ok! ' + JSON.stringify(data));
                        });
                    }
                    else {// 发送下一个等待的消息
                        sendNext();
                    }
                }
            };

            // 设置提交函数
            var flow = this.context().flow;
            flow.set("kafka-commit", kafkaCommit, groupId);
            node.log("LL +++++++ set flow context (kafka-commit + \"" + groupId + "\") ==> func()");

            // on -- 消费
            consumer.on('message', function (message) {
                if (debug) {
                    var msgstring = JSON.stringify(message);
                    node.log('kafka in debug ===> on message' + msgstring);
                }

                var msg = { };
                if(message){
                    msg = { topic: message.topic, payload: message.value };
                }
                else
                    return;

                if (isUtf8(msg.payload)){
                    msg.payload = msg.payload.toString();
                }

                onNewMsg(msg);
            });

            // on 错误处理
            consumer.on('error', function (err) {
                node.error('kafkaInNode Error: ' + err);
            });
        }
        catch(e){
            node.error(e);
            return;
        }
    }

    RED.nodes.registerType("kafka in", kafkaInNode);
};
