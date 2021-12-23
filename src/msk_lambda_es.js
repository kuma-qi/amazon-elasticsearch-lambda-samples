/*
 * Sample node.js code for AWS Lambda to upload the JSON documents
 * pushed from MSK to Amazon Elasticsearch.
 *
 */

/* == Imports == */
var https = require('https');
var zlib = require('zlib');
var crypto = require('crypto');
var endpoint = 'xxxxxxxxxxxxxxxxx.xxxx.es.amazonaws.com'; //your-domain-search-endpoint
var logFailedResponses = true;

exports.handler = function(recordList, context) {
    var elasticsearchBulkData = transformMSK(recordList);

    if (!elasticsearchBulkData) {
        console.log('Received a control message');
        context.succeed('Control message handled successfully');
        return;
    }
    const promise = new Promise(function(resolve, reject) {
      
            post(elasticsearchBulkData, function(error, success, statusCode, failedItems) {
                if (error) {
                    logFailure(error, failedItems);
                    context.fail(error);
                    reject(error)
                } else {
                    console.log('Success: ' + JSON.stringify(success));
                    context.succeed('Success');
                    resolve();
                }
            });
    })
  return promise
           
};

function transformMSK(logEvents) {

    var bulkRequestBody = '';
    
    logEvents.forEach(function(logEvent) {
        var timestamp = new Date();

        var indexName = [
            'classicvegas-' + timestamp.getUTCFullYear(),              
            ('0' + (timestamp.getUTCMonth() + 1)).slice(-2),  
            ('0' + timestamp.getUTCDate()).slice(-2)          
        ].join('.');
        
        var source = logEvent;
        source['@timestamp'] = timestamp;
        source['date'] = new Date(logEvent.date);

        var action = { "index": {} };
        action.index._index = indexName;
        action.index._type = '_doc';
        
        bulkRequestBody += [
            JSON.stringify(action),
            JSON.stringify(source),
        ].join('\n') + '\n';
    });
    return bulkRequestBody;
}
    
/*
 * Post the given document to Elasticsearch
 */
    
function post(body, callback) {
    var requestParams = buildRequest(endpoint, body);
    var request = https.request(requestParams, function(response) {
        var responseBody = '';
        response.on('data', function(chunk) {
            responseBody += chunk;
        });

        response.on('end', function() {
            var info = JSON.parse(responseBody);
            var failedItems;
            var success;
            var error;

            if (response.statusCode >= 200 && response.statusCode < 299) {
                failedItems = info.items.filter(function(x) {
                    return x.index.status >= 300;
                });

                success = {
                    "attemptedItems": info.items.length,
                    "successfulItems": info.items.length - failedItems.length,
                    "failedItems": failedItems.length
                };
            }

            if (response.statusCode !== 200 || info.errors === true) {
                
                
                
                if(info.items) {
                    failedItems = info.items.filter(function(item) {
                        return item.index && item.index.error;
                    });
                     delete info.items;
                     info.failedItems = failedItems;
                }
                error = {
                    statusCode: response.statusCode,
                    responseBody: info
                };
            }

            callback(error, success, response.statusCode, failedItems);
        });
    }).on('error', function(e) {
        callback(e);
    });
    request.end(requestParams.body);
}

function buildRequest(endpoint, body) {
    var endpointParts = endpoint.match(/^([^\.]+)\.?([^\.]*)\.?([^\.]*)\.amazonaws\.com$/);
    var region = endpointParts[2];
    var service = endpointParts[3];
    var datetime = (new Date()).toISOString().replace(/[:\-]|\.\d{3}/g, '');
    var date = datetime.substr(0, 8);
    var kDate = hmac('AWS4' + process.env.AWS_SECRET_ACCESS_KEY, date);
    var kRegion = hmac(kDate, region);
    var kService = hmac(kRegion, service);
    var kSigning = hmac(kService, 'aws4_request');

    var request = {
        host: endpoint,
        method: 'POST',
        path: '/_bulk',
        body: body,
        headers: {
            'Content-Type': 'application/json',
            'Host': endpoint,
            'Content-Length': Buffer.byteLength(body),
            'X-Amz-Security-Token': process.env.AWS_SESSION_TOKEN,
            'X-Amz-Date': datetime
        }
    };

    var canonicalHeaders = Object.keys(request.headers)
        .sort(function(a, b) { return a.toLowerCase() < b.toLowerCase() ? -1 : 1; })
        .map(function(k) { return k.toLowerCase() + ':' + request.headers[k]; })
        .join('\n');

    var signedHeaders = Object.keys(request.headers)
        .map(function(k) { return k.toLowerCase(); })
        .sort()
        .join(';');

    var canonicalString = [
        request.method,
        request.path, '',
        canonicalHeaders, '',
        signedHeaders,
        hash(request.body, 'hex'),
    ].join('\n');

    var credentialString = [ date, region, service, 'aws4_request' ].join('/');

    var stringToSign = [
        'AWS4-HMAC-SHA256',
        datetime,
        credentialString,
        hash(canonicalString, 'hex')
    ] .join('\n');

    request.headers.Authorization = [
        'AWS4-HMAC-SHA256 Credential=' + process.env.AWS_ACCESS_KEY_ID + '/' + credentialString,
        'SignedHeaders=' + signedHeaders,
        'Signature=' + hmac(kSigning, stringToSign, 'hex')
    ].join(', ');

    return request;
}

function hmac(key, str, encoding) {
    return crypto.createHmac('sha256', key).update(str, 'utf8').digest(encoding);
}

function hash(str, encoding) {
    return crypto.createHash('sha256').update(str, 'utf8').digest(encoding);
}

function logFailure(error, failedItems) {
    if (logFailedResponses) {
        if (failedItems && failedItems.length > 0) {
            console.log("Failed Items: ", JSON.stringify(failedItems));
        }
    }
}
