// External Libraries
const agent = require('agentkeepalive');
const asyncRetry = require('async').retry;
const request = require('request');
const zlib = require('zlib');
const aws = require('aws-sdk');
const util = require('util')

// Constants
const MAX_REQUEST_TIMEOUT_MS = parseInt(process.env.LOGDNA_MAX_REQUEST_TIMEOUT) || 30000;
const FREE_SOCKET_TIMEOUT_MS = parseInt(process.env.LOGDNA_FREE_SOCKET_TIMEOUT) || 300000;
const LOGDNA_URL = process.env.LOGDNA_URL || 'https://logs.logdna.com/logs/ingest';
const MAX_REQUEST_RETRIES = parseInt(process.env.LOGDNA_MAX_REQUEST_RETRIES) || 5;
const REQUEST_RETRY_INTERVAL_MS = parseInt(process.env.LOGDNA_REQUEST_RETRY_INTERVAL) || 100;
const INTERNAL_SERVER_ERROR = 500;
const DEFAULT_HTTP_ERRORS = [
    'ECONNRESET'
    , 'EHOSTUNREACH'
    , 'ETIMEDOUT'
    , 'ESOCKETTIMEDOUT'
    , 'ECONNREFUSED'
    , 'ENOTFOUND'
];

const s3 = new aws.S3();

// Get Configuration from Environment Variables
const getConfig = () => {
    const pkg = require('./package.json');
    let config = {
        log_raw_event: false
        , UserAgent: `${pkg.name}/${pkg.version}`
    };

    config.cloudtrail = process.env.LOGDNA_CLOUDTRAIL ? true : false;
    if (config.cloudtrail) {
        config.UserAgent = `logdna-cloudtrail/${pkg.version}`;
    }
    if (process.env.LOGDNA_KEY) config.key = process.env.LOGDNA_KEY;
    if (process.env.LOGDNA_HOSTNAME) config.hostname = process.env.LOGDNA_HOSTNAME;
    if (process.env.LOGDNA_TAGS && process.env.LOGDNA_TAGS.length > 0) {
        config.tags = process.env.LOGDNA_TAGS.split(',').map(tag => tag.trim()).join(',');
    }
    if (process.env.LOGDNA_APP) config.app = process.env.LOGDNA_APP;
    if (process.env.LOGDNA_LEVEL) config.level = process.env.LOGDNA_LEVEL;

    if (process.env.LOG_RAW_EVENT) {
        config.log_raw_event = process.env.LOG_RAW_EVENT.toLowerCase();
        config.log_raw_event = config.log_raw_event === 'yes' || config.log_raw_event === 'true';
    }

    return config;
};

// Parse the GZipped Log Data
const parseEvent = async (event) => {
    console.log('parseEvent');
    console.log(event.Records[0]);

    const eventLogBucket = event.Records[0].s3.bucket.name;
    const eventLogKey = event.Records[0].s3.object.key;
    console.log(eventLogBucket);
    console.log(eventLogKey);

    const logObj = await s3.getObject({
        Bucket: eventLogBucket,
        Key: eventLogKey
    }).promise();

    console.log('logObj.Body');
    console.log(logObj.Body);

    return JSON.parse(zlib.unzipSync(Buffer.from(logObj.Body, 'base64')));
};

// Prepare the Messages and Options
const prepareLogs = (eventData, log_raw_event) => {
    console.log('prepareLogs');
    console.log(util.inspect(eventData,{depth:null}));
    return eventData.Records.map((event) => {
        const eventLog = {
            ...event
        };

        //console.log('eventLog');
        //console.log(util.inspect(eventLog,{depth:null}));

        /*if (log_raw_event) {
            eventLog.line = event.message;
            eventLog.meta = Object.assign({}, eventLog.meta, eventMetadata);
        }*/

        return eventLog;
    });
};

// Ship the Logs
const sendLine = async (payload, config, callback) => {
    // Check for Ingestion Key
    if (!config.key) return callback('Missing LogDNA Ingestion Key');

    // Set Hostname
    const hostname = 'logdna-cloudtrail-test';// config.hostname || 'logdna-cloudtrail';

    // Prepare HTTP Request Options
    const options = {
        url: LOGDNA_URL
        , qs: config.tags ? {
            tags: config.tags
            , hostname: hostname
            , apikey: config.key
        } : {
            hostname: hostname
            , apikey: config.key
        }
        , method: 'POST'
        , body: JSON.stringify({
           'lines': [
               {
                   'timestamp': payload.timestamp || payload.eventTime
                   , 'line': payload
                   , 'level': config.level || 'INFO'
                   , 'app': config.app || 'cloudtrail'
               }
           ]
        })
        , headers: {
            'Content-Type': 'application/json; charset=UTF-8'
            , 'user-agent': config.UserAgent
        }
        , timeout: MAX_REQUEST_TIMEOUT_MS
        , withCredentials: false
        , agent: new agent.HttpsAgent({
            freeSocketTimeout: FREE_SOCKET_TIMEOUT_MS
        })
    };

    console.log('http request options');
    console.log(util.inspect(options,{depth:null}));

    // Flush the Log
    asyncRetry({
        times: MAX_REQUEST_RETRIES
        , interval: (retryCount) => {
            return REQUEST_RETRY_INTERVAL_MS * Math.pow(2, retryCount);
        }, errorFilter: (errCode) => {
            return DEFAULT_HTTP_ERRORS.includes(errCode) || errCode === 'INTERNAL_SERVER_ERROR';
        }
    }, (reqCallback) => {
        return request(options, (error, response, body) => {
            if (error) {
                return reqCallback(error.code);
            }
            if (response.statusCode >= INTERNAL_SERVER_ERROR) {
                return reqCallback('INTERNAL_SERVER_ERROR');
            }
            return reqCallback(null, body);
        });
    }, (error, result) => {
        if (error) {
            console.log('error 42');
            console.log(util.inspect(error,{depth:null}));
            return callback(error);
        }
        return callback(null, result);
    });
};

// Main Handler
const handler = async (event, context, callback) => {
    const config = getConfig();
    const rawData = await parseEvent(event);
    const preparedData = prepareLogs(rawData, config.log_raw_event);
    let rslts = preparedData.map( (d) => {
        return sendLine(d, config, callback);
    });

    return await rslts;
};

module.exports = {
    getConfig
    , handler
    , parseEvent
    , prepareLogs
    , sendLine
};
