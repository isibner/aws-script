var _ = require('highland');
var AWS = require('aws-sdk');
var s3 = new AWS.S3();

var fs = require('fs');
var path = require('path');

var listingFile = 'listing';
var outputDir = 'results/';
var errors = 0, processed = 0, logEvery = 10000;

String.prototype.endsWith = function(suffix) {
    return this.indexOf(suffix, this.length - suffix.length) !== -1;
};

String.prototype.isNotEmptyString = function (str) {
  return this.trim() !== '';
};

// Create a stream for the input file...
_(fs.createReadStream(listingFile))
// Read it line by line
.split()
// ...and process the lines 10 at a time...
.batch(10)
// ...flatten the arrays so we consider one line at a time...
.flatten()
// ...get an s3 url from a line...
.filter(function (line) {
  return line.isNotEmptyString();
})
.map(function (line) {
  var split = line.split(/\s+/g);
  return split[split.length -1];
})
.map(function (s3url) {
  var s3scheme = 's3:\/\/';
  if (s3url.indexOf(s3scheme) !== 0) {
    throw new Error('s3 url malformatted');
  } else {
    var path = s3url.substring(s3scheme.length);
    var bucket = path.substring(0, path.indexOf('/'));
    var key = path.substring(path.indexOf('/') + 1);
    var filename = path.substring(path.lastIndexOf('/') + 1);
    var res =  {Bucket: bucket, Key: key, filename: filename};
    return res;
  }
})
.filter(function (s3obj) {
  return s3obj.filename.isNotEmptyString() && !s3obj.filename.endsWith('/');
})
// ... and finally get the title of the page with Cheerio and log it!
.map(_.wrapCallback(function (data, next) {
  var getObjectParams = {Bucket: data.Bucket, Key: data.Key};
  var inStream = s3.getObject(getObjectParams).createReadStream();
  var outStream = fs.createWriteStream(path.join(outputDir, data.filename));
  outStream.on('finish', function () {
    processed++;
    next();
  });
  outStream.on('error', function (e) {
    console.log(e);
    errors++;
    processed++;
    next();
  })
  inStream.pipe(outStream);
}))
.parallel(100)
.each(function (data) {
  if (processed % logEvery === 0) {
    console.log('Processed ' + processed + ' files with ' + errors + ' errors.');
  }
});
