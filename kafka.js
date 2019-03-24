/**
 * Created by fwang1 on 3/25/15.
 */
module.exports = function(RED) {
    "use strict";
    var isUtf8 = require('./is-utf8');
    var fs = require('fs');
    var fspath = require("path");

    /*
     *   Kafka Producer
     *   Parameters:
     - topics
     - zkquorum(example: zkquorum = “[host]:2181")
     */
    function kafkaOutNode(config) {
        RED.nodes.createNode(this,config);
        var topic = config.topic;
        var brokerIPAndPort = config.zkquorum;
        var debug = (config.debug == true);
        var node = this;
        var kafka = require('kafka-node');
        var HighLevelProducer = kafka.HighLevelProducer;
        var topics = config.topics;
        var clientOption = {
            kafkaHost: brokerIPAndPort,
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
        this.status({fill:"green",shape:"dot",text:"connected to "+brokerIPAndPort});
    }

    RED.nodes.registerType("kafka out", kafkaOutNode);

    /////////////////////////////////////////////////////////////////////////////////////
    // 在文件中存储Offset
    //
    function OffsetStorage(host, group){
        var current = {};
        var offsetPath = RED.settings.userDir + '/kafkaOffset';
        console.log("KAFKA OFFSETDIR --------------------- :" + offsetPath);

        // 检查目录是否存在，如果没有则创建
        if (!fs.existsSync(offsetPath)){
            fs.mkdir(offsetPath, function(err){
                if (err) {
                    return console.error(err);
                }
            });
        }        

        function buildFileName(host, topic, group){
            var newHost = host.replace(/[.: ,，；;]/g, "_");
            var name = offsetPath + '/' + newHost + '_' + topic + '_' + group + '.txt';
            return name;
        }

        // 构建文件名
        // 从文件中读取offset，如果不存在则返回-1
        function readFrom(topic, cb){
            var fname = buildFileName(host, topic, group);
            current[topic] = -1;
            if (!fs.existsSync(fname)){
                console.log('Offset文件不存在 [' + fname + ']，首次读取该topic！');
                return;
            }

            fs.readFile(fname, function(err, data){
                if(err){
                    console.log('读取文件[' + fname + ']发生错误:' + err);
                }
                else {
                    console.log('read [' + fname + ']==>' + data.toString());
                    current[topic] = Number.parseInt(data.toString());
                }

                if (cb)
                    cb(current[topic]);
            })
        }

        function writeTo(topic, offset, cb){
            current[topic] = offset;

            var fname = buildFileName(host, topic, group);
            var tmpName = fname + '.tmp';
            fs.writeFile(tmpName,  offset.toString(),  function(err) {
                if (err) {
                    return console.error(err);
                }
                // 覆盖最终的文件
                fs.rename(tmpName, fname, function(){
                    if (cb)
                        cb();
                })
            })
        }

        this.initLoad = function(topic, cb){
            if (current[topic] == undefined){
                readFrom(topic, cb);
            }
            else
            {
                cb(current[topic]);
            }
        }

        this.get = function(topic){
            return current[topic];
        }

        this.set = function(topic, offset, cb){
            writeTo(topic, offset, cb);
        }
    }

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
        var brokerIPAndPort = config.zkquorum;
        var groupId = config.groupId;
        var debug = (config.debug == true);

        // 构造偏移存储对象
        var storage = new OffsetStorage(brokerIPAndPort, topics, groupId);
        var zkOptions = {
            // broker 的地址
            kafkaHost: brokerIPAndPort,
        };

        // 请求超时时间
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

        // 初始化读取offset
        for(var i=0; i<topics.length; i++){
            storage.initLoad(topics[i].topic);
        }

        var options = {
            groupId: groupId,                   // 设定的消费组ID
            autoCommit: config.autoCommit,      // 是否自动提交
            autoCommitMsgCount: 10,             // 消费多少条记录后自动提交
            fromOffset: true,                   // 使用在payload中设置的起始位置
        };

        // 每次最大的消费字节数
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
            var consumer;
            var isPaused;
            this.log("kafkaInNode new Consumer");
            if (debug){
                this.log("topics: " + JSON.stringify(topics));
                this.log("options: " + JSON.stringify(options));
            }

//            node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
//            node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
//            node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});

//            this.status({fill:"gray", shape:"dot", text:"connected to "+ brokerIPAndPort});
//            this.status({fill:"green", shape:"dot", text:"connected to "+ brokerIPAndPort});

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
                        if (consumer)
                            consumer.pause();
                        isPaused = true;
                        if (debug)
                           node.log('kafka in debug ===> paused');
                    }
                }
            }

            // 上次成功提交的时间
            var prvCommitTime = Date.now();
            // retry 如果为true则重新发送，否则消费下一条消息
            var kafkaCommit = function(retry){
                if (!consumer || config.autoCommit){// 未建立连接，或者是自动提交的，直接返回
                    return;
                }

                if (!currMsg){
                    node.error('kafka in no currMsg');
                    return;     // 没有当前消息，提交个啥？
                }

                if (retry){
                    if (debug)
                        node.log('kafka in debug ===> on kafka retry msg! ' + currMsg);
                    node.send(currMsg);     // 重新发送
                }
                else{
                    var topic = currMsg.topic;
                    var offset = currMsg.offset;
                    if (debug)
                        node.log('kafka in debug ===> on call commit, waiting =' + waitMsgList.length + ' topic = ' + topic + ' offset = ' + offset);
                    currMsg = null;     // 清除当前消息

                    // 全部消费完毕，或者超过1s钟，都立即保存当前的消费情况
                    var timeout = Date.now() - prvCommitTime
                    if (waitMsgList.length == 0 || timeout > 1000){
                        // 如果全部消息都已处理完毕，则提交offset，并恢复消费
                        storage.set(topic, offset, function(err){
                            if (err){
                                node.log('kafka in commit err: ' + err.toString());
                            }
                            // 提交成功，记录时间，恢复消费
                            prvCommitTime = Date.now();
                            if (isPaused && waitMsgList.length == 0){
                                consumer.resume();
                                isPaused = false;
                                if (debug)
                                    node.log('kafka in debug ===> resumed');
                            }

                            if (waitMsgList.length > 0){// 发送下一个等待的消息
                                sendNext();
                            }
        
                            if (debug)
                                node.log('kafka in debug ===> on kafka commit ok! At ' + topic + ':' + offset);
                        });
                    }
                    else {// 发送下一个等待的消息
                        sendNext();
                    }
                }
            };

            // 设置提交函数
            var flow = this.context().flow;
            flow.set("kafka-commit-" + groupId, kafkaCommit);
            node.log("LL +++++++ set flow context (\"kafka-commit-" + groupId + "\") ==> func()");

            // on -- 消费
            function initConsumer(){
                node.log('initConsumer ...');
                // 设置拉取得初始offset
                for(var i=0; i<topics.length; i++){
                    var topic = topics[i].topic;
                    // 如果存储的偏移是有效的，则读取下一条
                    // 否则从0开始读取，确保减少不丢失
                    var offset = storage.get(topic);
                    topics[i].offset = offset + 1;
                    
                    if(debug){
                        node.log('！！！set ' + topic + ' from ' + topics[i].offset);
                    }
                }
                consumer = new kafka.Consumer(client, topics, options);
                consumer.resume();  // 有可能指定的group已暂停消费，默认执行唤醒

                consumer.on('message', function (message) {
                    if (debug) {
                        var msgstring = JSON.stringify(message);
                        node.log('kafka in debug ===> on message' + msgstring);
                    }

                    var msg = { };
                    if(message){
                        msg = { topic: message.topic, offset: message.offset, payload: message.value };
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

            // 处理连接状态
            var timerHandle = setInterval(function(){
                var broker = client.brokerForLeader();
                var ready = broker ? broker.isReady() : false;

                if(debug)
                    node.log('connected = ' + ready + (client.connecting ? '  is_connecting': ''));
                if (ready){
                    if (!consumer)
                        initConsumer();
                    node.status({fill:"green",shape:"dot",text:"node-red:common.status.connected"});
                }
                else if (client.connecting)
                    node.status({fill:"yellow",shape:"ring",text:"node-red:common.status.connecting"});
                else{
                    node.status({fill:"red",shape:"ring",text:"node-red:common.status.disconnected"});
                    if (consumer){
                        consumer.close();
                        consumer = null;    // 如果已断开的，则清除consumer
                    }
                }
            }, 1500);
            
            this.on("close", function(msg) {
                // 清除定时器
                clearInterval(timerHandle);
            });

        }
        catch(e){
            node.error(e);
            return;
        }
    }

    RED.nodes.registerType("kafka in", kafkaInNode);
};
