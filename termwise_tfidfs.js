var AWS = require('aws-sdk');
var hl = require('highland');
var crypto = require('crypto');
var async = require('async');
var fs = require('fs');

var s3 = new AWS.S3();

var sha1 = function (input) {
    var str = crypto.createHash('sha1').update(input).digest('hex');
    while (str.charAt(0) === '0') {
      str = str.substring(1);
    }
    return str;
};

var idx = 0, errors = 0;

// Each line in each file
// Split by tab
// First item is term; hash this and it becomes filename
// Store the remainder as the contents of that file
// write everything to /ec2_data
var folder_name = 'inverted-index-merged';
var listObjectsParams = {Bucket: 'cis555-bucket', Prefix: folder_name + '/part-r'};
// WARNING: Change if you run this again!
var output_dir = 'ec2-data/tfidfs_out_2/';
require('mkdirp').sync(output_dir);

var Writable = require('stream').Writable;
var write_stream = Writable();
write_stream._write = function (chunk, enc, next) {
  if (chunk.length > 10 * 1024 * 1024) {
    console.log('chunk too large - had length ' + chunk.length);
    next();
  }
  var data = chunk.toString();
  var tabIndex = data.indexOf('\t');
  if (tabIndex === -1) {
    console.log('No tab char for ' + data);
    console.log('ignoring...');
    next();
  } else {
    var filename = output_dir + sha1(data.substring(0, tabIndex));
    fs.writeFileSync(filename, data, {flag: 'w+'});
    if (idx % 10000 === 0) {
      console.log('Processed ' + idx + ' terms with ' + errors + ' errors.');
    }
    idx++;
    next();
  }
};

write_stream.on('finish', function () {
  console.log('Finished processing ' + idx + ' terms with ' + errors + ' errors');
});

write_stream.on('error', function (e) {
  errors++;
  console.log('ERROR: ' + e.message);
});

s3.listObjects(listObjectsParams, function (_err, s3objects) {
  console.log(s3objects.Contents);
  var streams = require('underscore').map(s3objects.Contents, function (datum) {
    var getObjectParams = {Bucket: 'cis555-bucket', Key: datum.Key};
    var strm = s3.getObject(getObjectParams).createReadStream();
    return strm;
  });
  var hlStreams = require('underscore').map(streams, hl);
  hl(hlStreams).parallel(3).split().pipe(write_stream);
});
