var AWS = require('aws-sdk');
var hl = require('highland');
var cheerio = require('cheerio');
var crypto = require('crypto');
var sprom = require('sprom');

var sha1 = function(input){
    return crypto.createHash('sha1').update(input).digest('hex');
};

var s3 = new AWS.S3();
var pageRankParams = {Bucket: 'cis555-bucket', Key: 'pagerank-out/part-r-00000'};

var file = require('fs').createWriteStream('pageranks-with-descriptions');

hl(s3.getObject(pageRankParams).createReadStream()).split().each(function (line) {
  var urlPageRank = line.split('\t');
  var filename = sha1(urlPageRank[0]);
  var htmlParams = {Bucket: 'cis555-bucket', Key: 'crawl/' + filename};
  sprom(s3.getObject(htmlParams).createReadStream()).then(function (bodyBuffer) {
    var $ = cheerio.load(bodyBuffer.toString());
    var title = $('head title').text().replace(/(\r\n|\n|\r)/gm,'').trim();
    var description = $('meta[name=description]').attr('content');
    if (!description) {
      description = 'No description available.'
    } else {
      description = description.replace(/(\r\n|\n|\r)/gm,'').trim();
    }
    var delimiter = '\u2603\u2603\u2603';
    file.write(urlPageRank[0] + delimiter + urlPageRank[1] + delimiter + title + delimiter + description + '\n');
  });
});
