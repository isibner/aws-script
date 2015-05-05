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
var folder_name = 'inverted-index-merged';
var listObjectsParams = {Bucket: 'cis555-bucket', Prefix: folder_name + '/part-r'};
var output_dir = 'ec2-data/tfidfs_out_2/';
require('mkdirp').sync(output_dir);

s3.listObjects(listObjectsParams, function (_err, s3objects) {
  var streams = require('underscore').map(s3objects.Contents, function (datum) {
    var getObjectParams = {Bucket: 'cis555-bucket', Key: datum.Key};
    console.log('Preparing to process ' + datum.Key);
    var strm = s3.getObject(getObjectParams).createReadStream();
    return strm;
  });
  var hlStreams = require('underscore').map(streams, hl);
  hl(hlStreams).merge().split().each(function (line) {
    var data = line.toString();
    var tabIndex = data.indexOf('\t');
    if (tabIndex === -1) {
      console.log('No tab char for ' + data.substring(0, 200) + '...');
      console.log('ignoring...');
      errors++;
    } else {
      var filename = output_dir + sha1(data.substring(0, tabIndex));
      fs.writeFileSync(filename, data, {flag: 'w+'});
      if (idx % 10000 === 0) {
        console.log('Processed ' + idx + ' terms with ' + errors + ' errors.');
      }
      idx++;
    }
  });
});
