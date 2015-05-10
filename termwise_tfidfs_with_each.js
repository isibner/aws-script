var AWS = require('aws-sdk');
var hl = require('highland');
var crypto = require('crypto');
var async = require('async');
var fs = require('fs');

var s3 = new AWS.S3();

/** Function count the occurrences of substring in a string;
 * @param {String} string   Required. The string;
 * @param {String} subString    Required. The string to search for;
 * @param {Boolean} allowOverlapping    Optional. Default: false;
 */
function occurrences(string, subString, allowOverlapping) {

    string += ''; subString += '';
    if (subString.length <= 0) {
      return string.length + 1;
    }
    var n = 0, pos = 0;
    var step = (allowOverlapping) ? (1) : (subString.length);

    while (true) {
        pos = string.indexOf(subString, pos);
        if (pos >= 0) {
          n++;
          pos += step;
        } else {
          break;
        }
    }
    return (n);
}

var idx = 0, errors = 0;
var folder_name = 'inverted-index-final';
var listObjectsParams = {Bucket: 'cis555-bucket', Prefix: folder_name + '/part-r'};
var output_dir = 'ec2-data/tfidfs_out_final/';
var counts_dir = 'ec2-data/tfidfs_counts/';
require('mkdirp').sync(output_dir);
require('mkdirp').sync(counts_dir);

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
      var filename = output_dir + data.substring(0, tabIndex);
      fs.writeFileSync(filename, data, {flag: 'w+'});
      var countsFilename = counts_dir + data.substring(0, tabIndex);
      fs.writeFileSync(countsFilename, '' + occurrences(data, 'snappy'), {flag: 'w+'});
      if (idx % 10000 === 0) {
        console.log('Processed ' + idx + ' terms with ' + errors + ' errors.');
      }
      idx++;
    }
  });
});
