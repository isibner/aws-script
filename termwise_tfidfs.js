var AWS = require('aws-sdk');
var hl = require('highland');
var crypto = require('crypto');
var async = require('async');

var s3 = new AWS.S3();
var folder_name = 'inverted-index-merged';
var listObjectsParams = {Bucket: 'cis555-bucket', Prefix: folder_name + '/part-r'};

var sha1 = function(input){
    var str = crypto.createHash('sha1').update(input).digest('hex');
    while (str.charAt(0) === '0') {
      str = str.substring(1);
    }
    return str;
};

var idx = 0, errors = 0;

var Writable = require('stream').Writable;
var write_stream = Writable();
var FOLDER_NAME = 'termwise_tfidfs_out/';
write_stream._write = function (chunk, enc, next) {
  var data = chunk.toString();
  var tabIndex = data.indexOf('\t');
  if (tabIndex === -1) {
    console.log('No tab char for ' + data);
    console.log('ignoring...');
    next();
  } else {
    var filename = sha1(data.substring(0, tabIndex));
    var putObjectParams = {Bucket: 'cis555-bucket', Key: FOLDER_NAME + filename, Body: data};
    s3.putObject(putObjectParams, function (err, data) {
      if (err) {
        console.log(err.message);
        errors++;
      }
      if (idx % 1000 === 0){
        console.log('Processed ' + idx + ' terms with ' + errors + ' errors.');
      }
      idx++;
      next();
    })
  }
};


// Each line in each file
// Split by tab
// First item is term; hash this and it becomes filename
// Store the remainder as the contents of that file
s3.listObjects(listObjectsParams, function (e, data) {
  console.log(data.Contents);
  hl(data.Contents)
  .map(function (datum) {
    var getObjectParams = {Bucket: 'cis555-bucket', Key: datum.Key};
    return hl(s3.getObject(getObjectParams).createReadStream());
  })
  .series()
  .split()
  .pipe(write_stream);
})