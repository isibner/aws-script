var AWS = require('aws-sdk');
var hl = require('highland');
var cheerio = require('cheerio');
var crypto = require('crypto');
var sprom = require('sprom');
var async = require('async');
var fs = require('fs');

var FILENAME = 'pageranks-with-descriptions';

var sha1 = function(input){
    return crypto.createHash('sha1').update(input).digest('hex');
};

var s3 = new AWS.S3();
var pageRankParams = {Bucket: 'cis555-bucket', Key: 'pagerank-out/part-r-00000'};

fs.truncateSync(FILENAME, 0);

var idx = 0;
var errors = 0;
var delimiter = '\u2603\u2603\u2603';
var worker = function (line, callback) {
    idx++;
    if (idx % 100 === 0) {
      console.log('processed ' + idx + ' docs; ' + errors + ' errors so far')
    }
    var urlPageRank = line.split('\t');
    var filename = sha1(urlPageRank[0]);
    var htmlParams = {Bucket: 'cis555-bucket', Key: 'crawl-real/' + filename};
    sprom(s3.getObject(htmlParams).createReadStream()).then(function (bodyBuffer) {
      var $ = cheerio.load(bodyBuffer.toString());
      var title = $('head title').text().replace(/(\r\n|\n|\r)/gm,'').trim();
      var description = $('meta[name=description]').attr('content');
      if (!description) {
        description = 'No description available.'
      } else {
        description = description.replace(/(\r\n|\n|\r)/gm,'').trim();
      }
      fs.appendFileSync(FILENAME, urlPageRank[0] + delimiter + urlPageRank[1] + delimiter + title + delimiter + description + '\n');
      callback();
    }, function (e) {
      console.log(e.message + ' while processing ' + urlPageRank[0]);
      errors++;
      fs.appendFileSync(FILENAME, urlPageRank[0] + delimiter + urlPageRank[1] + delimiter + 'No title.' + delimiter + 'No description.' + '\n');
      callback();
    });
};
var q = async.queue(worker, 10);
q.drain = function () {
  console.log(idx + ' items processed with ' + errors + ' errors');
};

hl(s3.getObject(pageRankParams).createReadStream()).split().toArray(function (array) {
  array = array.slice(0,100);
  console.log('Starting with ' + array.length + ' items...');
  q.push(array); 
});
