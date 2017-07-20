const redis = require('redis');

const lookupKeyTag = (key, tag) => {
  // console.log(`.looking up key:[${key}] with tag:[${tag}]`);

  const client = redis.createClient();
  client.hget(`i:${key}`, tag, (err, data) => {
    if (err) throw err;

    if (data === null) console.log(`key:[${key}] with tag:[${tag}] not found`);
    else console.log(`results: ${JSON.stringify(data, null, 2)}`);

    // close connection
    client.quit();
  });
};

const lookupKey = (key) => {
  // console.log(`.looking up key:[${key}]`);

  const client = redis.createClient();
  client.hgetall(`i:${key}`, (err, data) => {
    if (err) throw err;

    if (data === null) console.log(`key:[${key}] not found`);
    else console.log(`results: ${JSON.stringify(data, null, 2)}`);

    // close connection
    client.quit();
  });
};


// TODO: use a cmdline lib to make this more resilient.
if (process.argv.length < 3) {
  console.log('Usage: node lookup.js hashkey [exifTag]');
  console.log('e.g. ');
  console.log(' $ node lookup.js 04057962cae0c5952196a2eceb6a5715');
  console.log(' $ node lookup.js 04057962cae0c5952196a2eceb6a5715 ISO');
}
else if (process.argv.length < 4) {
  lookupKey(process.argv[2]);
}
else if (process.argv.length < 5) {
  lookupKeyTag(process.argv[2], process.argv[3]);
}
