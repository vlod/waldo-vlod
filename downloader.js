const async = require('async');
const cheerio = require('cheerio');
const ExifImage = require('exif').ExifImage;
const fs = require('fs');
const gm = require('gm').subClass({ imageMagick: true });
const _flatten = require('lodash.flatten');
const _map = require('lodash.map');
const mkdirp = require('mkdirp');
const redis = require('redis');
const request = require('request');

const client = redis.createClient();
const S3_IMAGES = 'https://s3.amazonaws.com/waldo-recruiting';
const LOCAL_IMAGE_STORE = './images';
const NUM_WORKERS = 10; // concurrency for queue

// add an error handler to catch any redis problems
client.on('error', (err) => {
  console.log('Redis err:', err);
});

/**
 * Identifies if an image is jpg which is a valid exif carrier
 * @param {map} contents (filename, size, hash)
 */
const gmIdentifyImage = (contents, cb) => {
  gm(`${LOCAL_IMAGE_STORE}/${contents.imageFilename}`)
  .identify((err, data) => {
    if (err) {
      console.log(`ERROR: gmIdentifyImage: ${contents.imageFilename} err: ${err}`);
      return cb({});
    }

    return cb(data);
  });
};

/**
 * Setup a queue that supports concurrency
 */
console.log(`Set queue with NUM_WORKERS = ${NUM_WORKERS}`);
const queue = async.queue((contents, callback) => {
  // we don't need to dl the image, as we already have a valid one on the fs
  if (contents.download === false) {
    insertEXIFDataForImage(contents, callback);
  }
  else {
    // pull down the image and then insert the exif into db
    console.log(`pulling down image:[${contents.imageFilename}] size:[${contents.imageSize}]`);

    // keep track of dodgy http.get responses
    let statusCode;
    let contentType;
    let validContentType = true;

    request.get(`${S3_IMAGES}/${contents.imageFilename}`)
      // make sure we get http-code of 200 and a valid content-type that has exif
      .on('response', (response) => {
        statusCode = response.statusCode;
        contentType = response.headers['content-type'];

        if (statusCode !== 200) {
          console.log(`ERROR got status code:[${statusCode}] for image: ${S3_IMAGES}/${contents.imageFilename} type:[${response.headers['content-type']}]`);
        }
        // these are the only image formats that have exif. TODO: handle TIFFs
        if (contentType !== 'image/jpeg') {
          validContentType = false;
          console.log(`ERROR got contentType code:[${contentType}] for image: ${S3_IMAGES}/${contents.imageFilename}`);
        }
      })
      .on('error', (err) => {
        console.log('request error', err);
      })
      .on('end', () => {
        if (statusCode === 200 && validContentType) {
          insertEXIFDataForImage(contents, callback);
        }
        else callback();
      })
      // and pipe data to a file, so we can try to recover if we die
      .pipe(fs.createWriteStream(`${LOCAL_IMAGE_STORE}/${contents.imageFilename}`));
  }
}, NUM_WORKERS);

// assign a callback when the queue is empty
queue.drain = () => {
  console.log('all queue items have been processed');
  // close the connection to redis
  client.quit();
};

/**
 * Flattens map into array
 *
 * Input: { 'a': 4, 'b': 8 }
 * Output: ['a', 4, 'b', 8 ]
 */
const flattenKeyValues = (keyValues) => {
  const results = _map(keyValues, (value, key) => [key, value.toString()]);
  return _flatten(results);
};

/**
 * Little handler to know when we've finished processing a task in the queue
 * @param {*} contents
 */
const queuePushErrorHandler = contents => (err) => {
  if (err) throw err;
  console.log(`.finished processing:${contents.imageFilename}`);
};

/**
 * Extracts the exif from file and insert key-values into redis
 *
 * Input: {map} contents (filename, size, hash of the image)
 * Input: {func} callback (to tell the queue we're done)
 */
const insertEXIFDataForImage = (contents, callback) => {
  // let make absolutely sure that we are dealing with a real jpg, not just one that has a .jpg extension
  gmIdentifyImage(contents, (details) => {
    if (details.format && details.format.match('JPEG')) {
      // pull the exif from the image
      new ExifImage({ image: `${LOCAL_IMAGE_STORE}/${contents.imageFilename}` }, (error, exifData) => {
        if (error) throw error;

        // set the redis key: 'i:${imageHash}' => {ISO:2500, ApertureValue:2.625 ...}
        client.hmset(`i:${contents.imageHash}`, flattenKeyValues(exifData.exif), (err) => {
          if (err) throw err;

          // tell the queue we're done
          callback();
        });
      });
    }
    else {
      // not jpg. if we have an exif and it has a format tag show it
      const format = details.format ? details.format : '';
      console.log(`ERROR insertEXIFDataForImage file:[${LOCAL_IMAGE_STORE}/${contents.imageFilename}] is not valid JPG: [${format}]`);

      // tell the queue we're done
      callback();
    }
  });
};

/**
 * Main routine
 */

// somewhere to store images, so we can recover
mkdirp(LOCAL_IMAGE_STORE, (err) => {
  if (err) throw err;

  // go pull the list of imags to process
  request(S3_IMAGES, (err1, resp, data) => {
  // fs.readFile('./waldo-recruiting.xml', 'utf8', (err1, data) => {
    if (err1) throw err;
    const $ = cheerio.load(data, { ignoreWhitespace: true, xmlMode: true });

    // for each entry, pick up details we'll need
    $('Contents').each((i, element) => {
      const contents = {
        imageFilename: $(element).children('Key').text(),
        imageSize: $(element).children('Size').text(),
        imageHash: $(element).children('ETag').text().replace(/"/g, ''), // designated file hash
      };

      // make sure this is a image extension that supports exif
      if (contents.imageFilename.match(/\.jpg$/)) {
        // ask redis if we've processed this image already
        client.exists(`i:${contents.imageHash}`, (err2, results) => {
          // yes, let's skip it
          if (results === 1) {
            console.log(`.skipping key:${contents.imageFilename} as redis already has the hash:[i:${contents.imageHash}]`);
          }

          // nope, redis doesn't know about it
          else {
            // TODO: not great idea to store all these images in the same directory.
            // probably should md5 the filename and use the first 2 chars for directory

            // do we have this file on the fs and is it the correct length?
            fs.stat(`${LOCAL_IMAGE_STORE}/${contents.imageFilename}`, (err3, stats) => {
              if (err3 && err3.code === 'ENOENT') { // ENOENT: file not found
                console.log(`Not seen: [${contents.imageFilename}] before, adding it to the queue`);

                // no image file in fs, add it to the queue
                queue.push(contents, queuePushErrorHandler(contents));
              }
              else {
                // is the file size correct?
                console.log(`We have ${contents.imageFilename} already, size shouldBe:${contents.imageSize} actual:${stats.size}`);
                if (parseInt(contents.imageSize, 10) === stats.size) {
                  // yes file size is correct. just get the exif and push to commander
                  console.log(`${contents.imageFilename} has the correct file size, exif and storing info in redis`);

                  contents.download = false; // no need to re-download
                  queue.push(contents, queuePushErrorHandler(contents));
                }
                else {
                  // no. size of file is different to what's expected!
                  // probably incomplete download. in any case, let's schedule it to dl again. add it to the queue
                  console.log(`${contents.imageFilename} was not fully downloaded, will try again..`);

                  queue.push(contents, queuePushErrorHandler(contents));
                }
              }
            });
          }
        });
      }
    });

    // give the queue a sec to load,
    // if any nothing has been added to the queue, close the connection to redis otherwise redis waits forever
    setTimeout(() => {
      if (!queue.started) {
        console.log('nothing on the queue. closing redis');
        client.quit();
      }
    }, 1000);
  });
});
